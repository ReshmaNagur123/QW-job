# Author: Nagapradeep Naguri
# Program: NDNH
# Purpose: Process QW submissions which have been through EVS,
#          Name Search, update employer tables
# Iteration: 3
# Job Number: 3
# Job Name:ocse-fpls-ndnh-qw-update-emp-table-job
# Replaced Mainframe Programs: FPQW0155 & FPQW0150

import sys
import logging
import time
import re

from datetime import datetime

import boto3
import s3fs

from boto3.s3.transfer import TransferConfig

from pyspark.sql.functions import (
    substring,
    col,
    upper,
    trim,
    length,
    when,
    lit,
    row_number,
    broadcast,
    coalesce,
    lpad,
    rpad,
    max as spark_max,
    rank,
    udf,
    to_date,
    current_timestamp,
    concat,
    monotonically_increasing_id,
    least,
    greatest,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
)
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from fplslib import Fpls

from qw_helpers import generate_output_filename

LOGGER = logging.getLogger(__name__)


def delete_s3_prefix(s3_path, label):
    """Delete S3 directory contents. Used for checkpoints and
    staging cleanup."""
    fs = s3fs.S3FileSystem(anon=False)
    try:
        fs.rm(s3_path, recursive=True)
        LOGGER.info(f"Deleted {label}: {s3_path}")
    except FileNotFoundError:
        LOGGER.info(
            f"[SKIP] {label} path does not exist, nothing to delete:"
            f" {s3_path}"
        )


# Pre-defined temp table columns for optimization
TEMP_TABLE_COLS = [
    "work_ein",
    "qw_employer_name",
    "addr1",
    "addr2",
    "addr3",
    "city",
    "state",
    "zip5",
    "zip4",
    "empr_forgn_cc",
    "empr_forgn_c_name",
    "empr_forgn_zip",
    "opt_addr1",
    "opt_addr2",
    "opt_addr3",
    "opt_city",
    "opt_state",
    "opt_zip5",
    "opt_zip4",
    "opt_empr_forgn_cc",
    "opt_empr_forgn_c_name",
    "opt_empr_forgn_zip",
    "batch_number",
    "state_key_code",
    "empr_name_pntr",
    "empr_addr_pntr",
    "empr_opt_addr_pntr",
    "empr_addr_inp_ind_cd",
]


def run_job(fpls, connection):
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "odate"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    job_start_ts = time.time()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # Fixed 200 MB broadcast threshold.
    # The lookup tables broadcast in this job (unique_feins_df,
    # max_nm_snum_per_fein, max_addr_snum_per_fein) are at most a
    # few thousand rows of 9-byte EINs — well under 1 MB in practice.
    # 200 MB provides ample headroom while being small enough that
    # AQE statistics underestimation cannot accidentally broadcast a
    # large DataFrame. Intentional broadcasts use explicit broadcast()
    # hints and are unaffected by this threshold.
    broadcast_threshold = 200 * 1024 * 1024
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)
    LOGGER.info(
        f"[CONFIG] broadcast_threshold={broadcast_threshold:,} bytes "
        f"({broadcast_threshold // (1024**2)} MB)"
    )

    # sc.defaultParallelism = total vCPU slots across executor nodes only.
    # The driver node does not register with the Spark scheduler so it is
    # already excluded. spark.executor.cores is set by Glue based on worker
    # type (G.1X=4, G.2X=8, G.4X=16, G.8X=32, G.16X=64), so dividing
    # defaultParallelism by it gives the exact number of executor nodes
    # regardless of which worker type is configured.
    vcpus_per_worker = int(spark.conf.get("spark.executor.cores", "4"))
    num_executors = max(sc.defaultParallelism // vcpus_per_worker, 1)
    num_jdbc_partitions = num_executors
    LOGGER.info(
        f"[CONFIG] defaultParallelism={sc.defaultParallelism}, "
        f"vcpus_per_worker={vcpus_per_worker}, "
        f"num_executors={num_executors}, "
        f"num_jdbc_partitions={num_jdbc_partitions}"
    )

    # Set JDBC batch size based on executor count.
    # More executors = more parallel writers; scale batch down proportionally
    # so total in-flight rows stays bounded. Floor at 10000 for small clusters.
    jdbc_batch_size = max(10000, 300000 // num_executors)
    LOGGER.info(f"[CONFIG] jdbc_batch_size={jdbc_batch_size}"
                f" (num_executors={num_executors})")

    def overwrite_tmp_table(table_name, df):
        """Truncate and overwrite tmp table with df."""
        fpls.db.init_spark_format_jdbc_options(df.write.format("jdbc")).option(
            "dbtable", table_name
        ).option(
            "batchsize", jdbc_batch_size
        ).option(
            "rewriteBatchedStatements", "true"
        ).option(
            "truncate", "true"
        ).mode(
            "overwrite"
        ).save()

    def append_tmp_table(table_name, df):
        """Append data to tmp table with df."""
        fpls.db.init_spark_format_jdbc_options(df.write.format("jdbc")).option(
            "dbtable", table_name
        ).option(
            "batchsize", jdbc_batch_size
        ).option(
            "rewriteBatchedStatements", "true"
        ).mode(
            "append"
        ).save()

    odate = args["odate"]
    job_name = args["JOB_NAME"]
    bucket_name = fpls.config["bucket_name"]
    odate_no_dashes = odate.replace("-", "")
    odate_yymmdd = odate_no_dashes[2:8]

    LOGGER.info(f"Job {job_name} started for odate={odate}")

    exception_feins_bc = spark.sparkContext.broadcast(
        set(fpls.config.get("exception_feins", []))
    )
    fein_name_threshold = fpls.config.get("fein_name_min_match_score", 0.781)

    input_prefix = fpls.config.get(
        "qw_update_emp_input_path",
        f"s3://{bucket_name}/{job_name}/input/"
    )
    output_path = f"s3://{bucket_name}/{job_name}/output/"
    staging_path = (
        f"s3://{bucket_name}/{job_name}/output_staging/{odate_yymmdd}/"
    )
    LOGGER.info(f"Staging path: {staging_path}")
    LOGGER.info(f"Final output path: {output_path}")
    reject_path = f"s3://{bucket_name}/{job_name}/rejects/{odate_yymmdd}/"
    checkpoint_path = (
        f"s3://{bucket_name}/{job_name}/checkpoints/{odate_yymmdd}/"
    )
    LOGGER.info(f"Checkpoint path: {checkpoint_path}")
    delete_s3_prefix(checkpoint_path, "checkpoint")
    LOGGER.info("[CHECKPOINT] Cleared checkpoint directory at job start")
    delete_s3_prefix(staging_path, "staging")
    LOGGER.info("[STAGING] Cleared staging directory at job start")

    LOGGER.info("[STAGE] Stage 1 starting: Parsing input")
    LOGGER.info(f"[INPUT] Resolved input path: '{input_prefix}'")
    section_start = time.time()
    raw_df = spark.read.text(input_prefix).cache()
    total_cnt = raw_df.count()
    input_files = raw_df.inputFiles()
    LOGGER.info(f"[INPUT] Files read: '{input_files}'")
    LOGGER.info(f"[INPUT] Total input records: {total_cnt}")
    LOGGER.info(
        "[TIMER] Stage 1 (parsing - initial read) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Set shuffle partitions from two floors and a cap:
    #   record_floor: total_cnt // 200000 targets ~200K rows per partition,
    #                 a reasonable size for wide rows like this job's schema.
    #   executor_floor: 2 * num_executors ensures at least 2 tasks per executor
    #                   so no executor sits idle during a shuffle stage.
    #   cap: 4 * num_executors prevents excessive small-partition overhead;
    #        AQE coalescePartitions will merge further at runtime.
    record_floor = max(total_cnt // 200000, 1)
    executor_floor = 2 * num_executors
    shuffle_max = 4 * num_executors
    shuffle_partitions = min(shuffle_max, max(record_floor, executor_floor))
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    LOGGER.info(
        f"[CONFIG] shuffle_partitions={shuffle_partitions} "
        f"(record_floor={record_floor}, "
        f"executor_floor={executor_floor}, "
        f"cap={shuffle_max})"
    )

    if total_cnt == 0:
        LOGGER.info("No input records")
        fpls.notification(
            f"Job '{job_name}' completed with no input."
        ).subject(f"Job '{job_name}' - Success (No Input)").send_job_success()
        job.commit()
        return

    valid_df = raw_df.filter(length(col("value")) > 1)
    invalid_df = (
        raw_df.filter(length(col("value")) <= 1)
        .withColumn("reject_reason", lit("EOF marker or empty record"))
        .withColumn("record_length", length(col("value")))
    )

    invalid_cnt = invalid_df.count()
    # valid_cnt is the complement of invalid_cnt — no second scan needed.
    valid_cnt = total_cnt - invalid_cnt
    LOGGER.info(f"Valid: {valid_cnt}, Invalid: {invalid_cnt}")

    if invalid_cnt > 0:
        invalid_df.select(
            col("value"), col("reject_reason"), col("record_length")
        ).write.mode("overwrite").option("header", "true").csv(reject_path)

    if valid_cnt == 0:
        fpls.notification(
            f"Job '{job_name}' completed with no valid records."
        ).subject(f"Job '{job_name}' - Success (No Valid)").send_job_success()
        job.commit()
        return

    parsed_df = (
        valid_df.select(
            col("value"),
            # File metadata (1-143)
            substring(col("value"), 1, 70).alias("file_name"),
            substring(col("value"), 71, 10).alias("file_rec_num_id"),
            substring(col("value"), 81, 10).alias("file_addr1_id"),
            substring(col("value"), 91, 10).alias("file_addr2_id"),
            substring(col("value"), 121, 3).alias("connect_direct_node"),
            substring(col("value"), 124, 10).alias("file_node_rdate"),
            substring(col("value"), 134, 10).alias("file_node_ttime"),
            # Header info (144-195)
            substring(col("value"), 144, 2).alias("state_key_code"),
            substring(col("value"), 144, 9).alias("state_key_code_full"),
            substring(col("value"), 153, 2).alias("transmission_type"),
            substring(col("value"), 155, 1).alias("dod_code"),
            substring(col("value"), 156, 8).alias("sort_key_date"),
            substring(col("value"), 164, 8).alias("sort_key_time"),
            substring(col("value"), 172, 6).alias("batch_number"),
            substring(col("value"), 178, 2).alias("ver_cntl"),
            substring(col("value"), 180, 8).alias("submitter_datestamp"),
            substring(col("value"), 188, 8).alias("receipt_datestamp"),
            substring(col("value"), 196, 1).alias("hdr_bypass_cd_on"),
            substring(col("value"), 197, 1).alias("is_batch_num_999999_test"),
            substring(col("value"), 198, 1).alias("record_status"),
            substring(col("value"), 199, 10).alias("cntl_fld_odc_or_filler"),
            # Error info (209-229)
            substring(col("value"), 209, 1).alias("out_err_cnt"),
            substring(col("value"), 210, 4).alias("out_err_cd_1"),
            substring(col("value"), 214, 4).alias("out_err_cd_2"),
            substring(col("value"), 218, 4).alias("out_err_cd_3"),
            substring(col("value"), 222, 4).alias("out_err_cd_4"),
            substring(col("value"), 226, 4).alias("out_err_cd_5"),
            # SSN validation (230-232)
            substring(col("value"), 230, 1).alias("ssn_val_code"),
            substring(col("value"), 231, 1).alias("ssn_vrfd_cd"),
            substring(col("value"), 232, 1).alias("ssn_vldtn_cd"),
            # Namesearch (233-238)
            substring(col("value"), 233, 3).alias("namesearch_score_11"),
            substring(col("value"), 236, 3).alias("namesearch_score_12"),
            # Final fields (239-354)
            substring(col("value"), 239, 9).alias("final_ssn"),
            substring(col("value"), 248, 16).alias("final_first_nm"),
            substring(col("value"), 264, 16).alias("final_middle_nm"),
            substring(col("value"), 280, 30).alias("final_last_nm"),
            substring(col("value"), 310, 45).alias("final_empr_nm"),
            # Additional table info (355-374)
            substring(col("value"), 355, 10).alias("ssn_sequence_num"),
            substring(col("value"), 365, 10).alias("submn_uid"),
            # Employer info (375-417)
            substring(col("value"), 375, 9).alias("empr_ein"),
            substring(col("value"), 384, 1).alias("empr_table_ind"),
            substring(col("value"), 385, 1).alias("empr_name_ind"),
            substring(col("value"), 386, 10).alias("empr_name_pntr_input"),
            substring(col("value"), 396, 1).alias("empr_addr_ind_input"),
            substring(col("value"), 397, 10).alias("empr_addr_pntr_input"),
            substring(col("value"), 407, 1).alias("empr_opt_addr_ind_input"),
            substring(col("value"), 408, 10).alias("empr_opt_addr_pntr_input"),
            # Foreign flags (418-423)
            substring(col("value"), 418, 1).alias("qw_frgn_empr_flg_cc"),
            substring(col("value"), 419, 1).alias("qw_frgn_empr_flg_cname"),
            substring(col("value"), 420, 1).alias("qw_frgn_empr_flg_czip"),
            substring(col("value"), 421, 1).alias("qw_frgn_opt_flg_cc"),
            substring(col("value"), 422, 1).alias("qw_frgn_opt_flg_cname"),
            substring(col("value"), 423, 1).alias("qw_frgn_opt_flg_czip"),
            # Validation fields (424-427)
            substring(col("value"), 424, 1).alias("vald_ssn_chg_cd"),
            substring(col("value"), 425, 1).alias("vald_ein_cd"),
            substring(col("value"), 426, 1).alias("vald_wage_amt_cd"),
            substring(col("value"), 427, 1).alias("vald_rpt_prd_cd"),
            # Work fields (439-570)
            substring(col("value"), 439, 9).alias("wrk_ssn"),
            substring(col("value"), 448, 16).alias("wrk_first_nm"),
            substring(col("value"), 464, 16).alias("wrk_middle_nm"),
            substring(col("value"), 480, 30).alias("wrk_last_nm"),
            substring(col("value"), 510, 45).alias("wrk_empr_nm"),
            substring(col("value"), 555, 1).alias("wrk_rpt_prd_qtr"),
            substring(col("value"), 556, 4).alias("wrk_rpt_prd_year"),
            substring(col("value"), 560, 11).alias("wrk_wage_amt_x"),
            # QW data record (571-1171)
            substring(col("value"), 571, 2).alias("qw_identifier"),
            substring(col("value"), 573, 9).alias("qw_employee_ssn"),
            substring(col("value"), 582, 16).alias("qw_empe_first_name"),
            substring(col("value"), 598, 16).alias("qw_empe_middle_name"),
            substring(col("value"), 614, 30).alias("qw_empe_last_name"),
            substring(col("value"), 644, 11).alias("qw_empe_wage_amt"),
            substring(col("value"), 655, 1).alias("qw_rpt_period_q"),
            substring(col("value"), 656, 2).alias("qw_rpt_period_cc"),
            substring(col("value"), 658, 2).alias("qw_rpt_period_yy"),
            substring(col("value"), 660, 9).alias("work_ein"),
            substring(col("value"), 669, 12).alias("qw_employer_st_ein"),
            substring(col("value"), 681, 45).alias("qw_employer_name"),
            substring(col("value"), 726, 40).alias("qw_empr_address1"),
            substring(col("value"), 766, 40).alias("qw_empr_address2"),
            substring(col("value"), 806, 40).alias("qw_empr_address3"),
            substring(col("value"), 846, 25).alias("qw_empr_city"),
            substring(col("value"), 871, 2).alias("qw_empr_state"),
            substring(col("value"), 873, 5).alias("qw_empr_zip_5"),
            substring(col("value"), 878, 4).alias("qw_empr_zip_4"),
            substring(col("value"), 882, 2).alias("qw_empr_forgn_cc"),
            substring(col("value"), 884, 25).alias("qw_empr_forgn_c_name"),
            substring(col("value"), 909, 15).alias("qw_empr_forgn_zip"),
            substring(col("value"), 924, 40).alias("qw_empr_opt_address1"),
            substring(col("value"), 964, 40).alias("qw_empr_opt_address2"),
            substring(col("value"), 1004, 40).alias("qw_empr_opt_address3"),
            substring(col("value"), 1044, 25).alias("qw_empr_opt_city"),
            substring(col("value"), 1069, 2).alias("qw_empr_opt_state"),
            substring(col("value"), 1071, 5).alias("qw_empr_opt_zip_5"),
            substring(col("value"), 1076, 4).alias("qw_empr_opt_zip_4"),
            substring(col("value"), 1080, 2).alias("qw_er_opt_frn_cc"),
            substring(col("value"), 1082, 25).alias("qw_er_opt_frn_c_name"),
            substring(col("value"), 1107, 15).alias("qw_er_opt_frn_zip"),
            # Final address records (1172-1604)
            substring(col("value"), 1172, 40).alias("fnl_empr_address1"),
            substring(col("value"), 1212, 40).alias("fnl_empr_address2"),
            substring(col("value"), 1252, 40).alias("fnl_empr_address3"),
            substring(col("value"), 1292, 25).alias("fnl_empr_city"),
            substring(col("value"), 1317, 2).alias("fnl_empr_state"),
            substring(col("value"), 1319, 5).alias("fnl_empr_zip_5"),
            substring(col("value"), 1324, 4).alias("fnl_empr_zip_4"),
            substring(col("value"), 1328, 6).alias("fnl_empr_err_tab"),
            substring(col("value"), 1334, 2).alias("fnl_frgn_cc"),
            substring(col("value"), 1336, 25).alias("fnl_frgn_c_name"),
            substring(col("value"), 1361, 15).alias("fnl_frgn_zip"),
            substring(col("value"), 1376, 40).alias("fnl_opt_address1"),
            substring(col("value"), 1416, 40).alias("fnl_opt_address2"),
            substring(col("value"), 1456, 40).alias("fnl_opt_address3"),
            substring(col("value"), 1496, 25).alias("fnl_opt_city"),
            substring(col("value"), 1521, 2).alias("fnl_opt_state"),
            substring(col("value"), 1523, 5).alias("fnl_opt_zip_5"),
            substring(col("value"), 1528, 4).alias("fnl_opt_zip_4"),
            substring(col("value"), 1532, 6).alias("fnl_opt_err_tab"),
            substring(col("value"), 1538, 2).alias("fnl_opt_frgn_cc"),
            substring(col("value"), 1540, 25).alias("fnl_opt_frgn_c_name"),
            substring(col("value"), 1565, 15).alias("fnl_opt_frgn_zip"),
        )
        # Replace SSN with zeros when ssn_val_code indicates invalid
        .withColumn(
            "wrk_ssn",
            when(col("ssn_val_code") == "I", lit("000000000")).otherwise(
                col("wrk_ssn")
            ),
        )
        .withColumn(
            "final_ssn",
            when(col("ssn_val_code") == "I", lit("000000000")).otherwise(
                col("final_ssn")
            ),
        )
        # Override vald_ein_cd to 'I' when EIN is not exactly 9 digits
        .withColumn(
            "vald_ein_cd",
            when(
                (length(trim(col("empr_ein"))) != 9)
                | (~col("empr_ein").rlike("^[0-9]{9}$")),
                lit("I"),
            ).otherwise(col("vald_ein_cd")),
        )
        # Clear employer name fields when EIN is invalid
        .withColumn(
            "final_empr_nm",
            when(col("vald_ein_cd") == "I", lit("")).otherwise(
                col("final_empr_nm")
            ),
        )
        .withColumn(
            "wrk_empr_nm",
            when(col("vald_ein_cd") == "I", lit("")).otherwise(
                col("wrk_empr_nm")
            ),
        )
        .withColumn("input_order", monotonically_increasing_id())
        .withColumn("empr_name_pntr", lit(0))
        .withColumn("empr_addr_pntr", lit(0))
        .withColumn("empr_opt_addr_pntr", lit(0))
        .withColumn(
            "empr_forgn_cc",
            when(
                col("qw_frgn_empr_flg_cc") == "Y", col("qw_empr_forgn_cc")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "empr_forgn_c_name",
            when(
                col("qw_frgn_empr_flg_cname") == "Y",
                col("qw_empr_forgn_c_name"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "empr_forgn_zip",
            when(
                col("qw_frgn_empr_flg_czip") == "Y", col("qw_empr_forgn_zip")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "opt_empr_forgn_cc",
            when(
                col("qw_frgn_opt_flg_cc") == "Y", col("qw_er_opt_frn_cc")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "opt_empr_forgn_c_name",
            when(
                col("qw_frgn_opt_flg_cname") == "Y",
                col("qw_er_opt_frn_c_name"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "opt_empr_forgn_zip",
            when(
                col("qw_frgn_opt_flg_czip") == "Y", col("qw_er_opt_frn_zip")
            ).otherwise(lit(None)),
        )
        # Apply same logic to qw_ prefixed fields for output
        .withColumn(
            "qw_empr_forgn_cc",
            when(
                col("qw_frgn_empr_flg_cc") == "Y", col("qw_empr_forgn_cc")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "qw_empr_forgn_c_name",
            when(
                col("qw_frgn_empr_flg_cname") == "Y",
                col("qw_empr_forgn_c_name"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "qw_empr_forgn_zip",
            when(
                col("qw_frgn_empr_flg_czip") == "Y", col("qw_empr_forgn_zip")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "qw_er_opt_frn_cc",
            when(
                col("qw_frgn_opt_flg_cc") == "Y", col("qw_er_opt_frn_cc")
            ).otherwise(lit(None)),
        )
        .withColumn(
            "qw_er_opt_frn_c_name",
            when(
                col("qw_frgn_opt_flg_cname") == "Y",
                col("qw_er_opt_frn_c_name"),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "qw_er_opt_frn_zip",
            when(
                col("qw_frgn_opt_flg_czip") == "Y", col("qw_er_opt_frn_zip")
            ).otherwise(lit(None)),
        )
    )

    # Add legacy column name aliases for backward compatibility with
    # downstream business logic. Eliminates duplicate substring
    # parsing while preserving downstream code semantics.
    parsed_df = (
        parsed_df
        .withColumn("addr1", col("qw_empr_address1"))
        .withColumn("addr2", col("qw_empr_address2"))
        .withColumn("addr3", col("qw_empr_address3"))
        .withColumn("city", col("qw_empr_city"))
        .withColumn("state", col("qw_empr_state"))
        .withColumn("zip5", col("qw_empr_zip_5"))
        .withColumn("zip4", col("qw_empr_zip_4"))
        .withColumn("opt_addr1", col("qw_empr_opt_address1"))
        .withColumn("opt_addr2", col("qw_empr_opt_address2"))
        .withColumn("opt_addr3", col("qw_empr_opt_address3"))
        .withColumn("opt_city", col("qw_empr_opt_city"))
        .withColumn("opt_state", col("qw_empr_opt_state"))
        .withColumn("opt_zip5", col("qw_empr_opt_zip_5"))
        .withColumn("opt_zip4", col("qw_empr_opt_zip_4"))
        .withColumn("frgn_empr_flag_cc", col("qw_frgn_empr_flg_cc"))
        .withColumn("frgn_empr_flag_cname", col("qw_frgn_empr_flg_cname"))
        .withColumn("frgn_empr_flag_czip", col("qw_frgn_empr_flg_czip"))
        .withColumn("frgn_opt_flag_cc", col("qw_frgn_opt_flg_cc"))
        .withColumn("frgn_opt_flag_cname", col("qw_frgn_opt_flg_cname"))
        .withColumn("frgn_opt_flag_czip", col("qw_frgn_opt_flg_czip"))
        .withColumn("is_batch_999999", col("is_batch_num_999999_test"))
        .withColumn("empr_addr_inp_ind_cd", col("empr_addr_ind_input"))
    )

    LOGGER.info("[STAGE 1.1] parsed_df constructed (pre-materialization)")

    # STAGED CHECKPOINT 1: Save parsed data
    parsed_df.write.mode("overwrite").parquet(f"{checkpoint_path}parsed/")
    LOGGER.info("[CHECKPOINT] Saved parsed_df to checkpoint")
    raw_df.unpersist()
    # Drop value column after checkpoint re-read to save memory.
    # Re-joined back via input_order before final output concat.
    # Memory optimization - drop large value column when not needed.
    parsed_df = spark.read.parquet(
        f"{checkpoint_path}parsed/"
    ).drop("value")
    parsed_df = parsed_df.cache()
    parsed_reloaded_cnt = parsed_df.count()
    LOGGER.info(
        "[CHECKPOINT] Re-loaded parsed_df from checkpoint "
        f"({parsed_reloaded_cnt:,} rows)"
    )
    LOGGER.info(
        "[TIMER] Stage 1 (parsing) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Extract unique FEINs
    LOGGER.info(
        "[STAGE] Stage 2 starting: Constructing unique FEINs / "
        "pre-Stage 3 prep"
    )
    section_start = time.time()
    unique_feins_df = (
        parsed_df.select(col("work_ein").alias("empr_id")).distinct().cache()
    )
    fein_count = unique_feins_df.count()
    LOGGER.info(f"Extracted {fein_count} unique FEINs from input")

    # Write unique FEINs to DB tmp table so EMP table JDBC reads can
    # filter server-side via WHERE EXISTS, avoiding full table scans.
    fein_filter_tmp = "pndnh.ndnh_qw_update_emp_table_job_fein_filter_tmp"
    overwrite_tmp_table(fein_filter_tmp, unique_feins_df)
    LOGGER.info(
        f"[FILTER] Written {fein_count:,} FEINs to {fein_filter_tmp}"
    )

    # Extract unique empr_ein values for EMPRADDR filtering
    unique_empr_ein_df = (
        parsed_df.select(col("empr_ein").alias("empr_idfr")).distinct().cache()
    )
    empr_ein_count = unique_empr_ein_df.count()
    LOGGER.info(
        f"Extracted {empr_ein_count} unique empr_ein values "
        "for EMPRADDR"
    )

    LOGGER.info("Loading database tables with server-side FEIN filter")

    unk_addr_df = (
        parsed_df.filter(
            (
                (col("empr_addr_inp_ind_cd") == "X")
                & (trim(col("qw_employer_name")) == "")
            )
            | (col("batch_number") == "999999")
            | (col("is_batch_999999") == "Y")
        )
        .withColumn("empr_name_pntr", lit(0))
        .withColumn("empr_addr_pntr", lit(0))
        .withColumn("empr_opt_addr_pntr", lit(0))
    )

    knwn_addr_df = parsed_df.join(unk_addr_df, ["input_order"], "left_anti")
    unk_addr_df = unk_addr_df.cache()
    knwn_addr_df = knwn_addr_df.cache()
    unk_cnt = unk_addr_df.count()
    # knwn_cnt is the complement of unk_cnt — no second scan needed.
    knwn_cnt = parsed_reloaded_cnt - unk_cnt
    LOGGER.info(f"Unknown: {unk_cnt}, Known: {knwn_cnt}")

    # Pseudo-FEIN identification
    # FIX: Check empr_ein (position 375-383) instead of work_ein
    # (position 660-668) to match COBOL logic
    pseudo_fein_df = knwn_addr_df.filter(
        (substring(trim(col("empr_ein")), 1, 1) == "A")
        | (substring(lpad(trim(col("empr_ein")), 9, "0"), 3, 7) == "0000000")
        | col("empr_ein").isin(list(exception_feins_bc.value))
    )

    # Keep all pseudo records for parquet output
    # Dedup only used for EMPRADDR sequence generation (DB write path)
    pseudo_fein_df_for_db = pseudo_fein_df.dropDuplicates(
        [
            "empr_ein",
            "qw_employer_name",
            "addr1",
            "addr2",
            "addr3",
            "city",
            "state",
            "zip5",
            "zip4",
        ]
    )

    non_pseudo_df = knwn_addr_df.join(
        pseudo_fein_df, ["input_order"], "left_anti"
    )

    non_pseudo_df = non_pseudo_df.cache()
    pseudo_cnt = pseudo_fein_df.count()
    # non_pseudo_cnt is the complement of pseudo_cnt within knwn_addr_df
    # — no second scan needed.
    non_pseudo_cnt = knwn_cnt - pseudo_cnt
    LOGGER.info(f"Pseudo: {pseudo_cnt}, Non-pseudo: {non_pseudo_cnt}")
    LOGGER.info(
        "[TIMER] Stage 2 (FEIN extraction + classification) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # STAGED CHECKPOINT 2: Save classification results
    unk_addr_df.write.mode("overwrite").parquet(f"{checkpoint_path}unknown/")
    if pseudo_cnt > 0:
        pseudo_fein_df.write.mode("overwrite").parquet(
            f"{checkpoint_path}pseudo/"
        )
        pseudo_fein_df_for_db.write.mode("overwrite").parquet(
            f"{checkpoint_path}pseudo_for_db/"
        )
    if non_pseudo_cnt > 0:
        non_pseudo_df.write.mode("overwrite").parquet(
            f"{checkpoint_path}non_pseudo/"
        )
    LOGGER.info("[CHECKPOINT] Saved classification results")
    knwn_addr_df.unpersist()

    unk_addr_df = spark.read.parquet(f"{checkpoint_path}unknown/").cache()
    if pseudo_cnt > 0:
        pseudo_fein_df = spark.read.parquet(f"{checkpoint_path}pseudo/")
        pseudo_fein_df = pseudo_fein_df.cache()
        pseudo_fein_df_for_db = spark.read.parquet(
            f"{checkpoint_path}pseudo_for_db/"
        ).cache()
    if non_pseudo_cnt > 0:
        non_pseudo_df = spark.read.parquet(
            f"{checkpoint_path}non_pseudo/"
        ).cache()
    LOGGER.info("[CHECKPOINT] Re-loaded classification from checkpoints")

    stage3_start = time.time()

    LOGGER.info(
        "[STAGE] Stage 3 starting: DB table loads + "
        "broadcast filtering"
    )
    # Read EMPRNM with hash-based predicates for parallelism
    section_start = time.time()
    LOGGER.info("Loading EMPRNM table")
    predicates_emprnm = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    emprnm_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_id, empr_nm, nm_snum, app_odate FROM pndnh.EMPRNM"
            " WHERE EXISTS (SELECT 1 FROM "
            f"{fein_filter_tmp} f WHERE f.empr_id = EMPRNM.empr_id)) t"
        ),
        predicates=predicates_emprnm,
    )
    LOGGER.info(
        "[TIMER] EMPRNM read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EADDR with hash-based predicates for parallelism
    section_start = time.time()
    LOGGER.info("Loading EADDR table")
    predicates_eaddr = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    eaddr_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_id, addr_snum, empr_addrln40_1 AS addr1, "
            "empr_addrln40_2 AS addr2, empr_addrln40_3 AS addr3, "
            "empr_city25 AS city, empr_st AS state, "
            "empr_zip5 AS zip5, empr_zip4 AS zip4, empr_frgn_cntry_cd, "
            "empr_frgn_cntry_nm, empr_frgnpzn, addrck_pmycd, "
            "addrck_scycd_1, addrck_scycd_2, app_odate "
            "FROM pndnh.EADDR WHERE EXISTS (SELECT 1 FROM "
            f"{fein_filter_tmp} f WHERE f.empr_id = EADDR.empr_id)) t"
        ),
        predicates=predicates_eaddr,
    )
    LOGGER.info(
        "[TIMER] EADDR read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EMPRADDR (lightweight) with hash-based predicates for parallelism
    section_start = time.time()
    LOGGER.info("Loading EMPRADDR table (lightweight projection)")
    predicates_empraddr = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    empraddr_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_idfr AS empr_id, empr_snum AS addr_snum "
            "FROM pndnh.EMPRADDR) t"
        ),
        predicates=predicates_empraddr,
    )
    LOGGER.info(
        "[TIMER] EMPRADDR (lightweight) read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EMPNMADR with hash-based predicates for parallelism
    section_start = time.time()
    LOGGER.info("Loading EMPNMADR table")
    predicates_empnmadr = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    empnmadr_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_id, nm_snum, addr_snum, app_odate "
            "FROM pndnh.EMPNMADR WHERE EXISTS (SELECT 1 FROM "
            f"{fein_filter_tmp} f WHERE f.empr_id = EMPNMADR.empr_id)) t"
        ),
        predicates=predicates_empnmadr,
    )
    LOGGER.info(
        "[TIMER] EMPNMADR read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Filter in Spark using broadcast join (EMPRADDR only --
    # EMPRNM, EADDR, EMPNMADR are pre-filtered server-side via WHERE EXISTS)
    section_start = time.time()
    LOGGER.info("Filtering EMPRADDR in Spark using broadcast join")
    fein_filter = broadcast(unique_feins_df)

    emprnm_df = emprnm_full.cache()
    emprnm_count = emprnm_df.count()
    LOGGER.info(
        f"[FILTER] EMPRNM loaded: {emprnm_count:,} records"
    )
    eaddr_df = eaddr_full.cache()
    eaddr_count = eaddr_df.count()
    LOGGER.info(
        f"[FILTER] EADDR loaded: {eaddr_count:,} records"
    )
    empraddr_df = empraddr_full.join(fein_filter, "empr_id", "inner").cache()
    empraddr_count = empraddr_df.count()
    LOGGER.info(
        f"[FILTER] EMPRADDR filtered to {empraddr_count:,} records"
    )
    empnmadr_perm = empnmadr_full.cache()
    empnmadr_count = empnmadr_perm.count()
    LOGGER.info(
        f"[FILTER] EMPNMADR loaded: {empnmadr_count:,} records"
    )

    # STAGED CHECKPOINT 3b: Save database tables
    emprnm_df.write.mode("overwrite").parquet(f"{checkpoint_path}db_emprnm/")
    eaddr_df.write.mode("overwrite").parquet(f"{checkpoint_path}db_eaddr/")
    empraddr_df.write.mode("overwrite").parquet(
        f"{checkpoint_path}db_empraddr/"
    )
    empnmadr_perm.write.mode("overwrite").parquet(
        f"{checkpoint_path}db_empnmadr/"
    )
    LOGGER.info("[CHECKPOINT] Saved database tables to checkpoints")
    LOGGER.info(
        "[TIMER] Stage 3 (DB table loads + broadcast filtering) "
        f"completed in {time.time() - stage3_start:.2f}s"
    )

    if non_pseudo_cnt > 0:
        LOGGER.info("[STAGE] Stage 4 starting: Name matching")
        section_start = time.time()

        emprnm_for_feins = (
            emprnm_df.join(
                broadcast(non_pseudo_df.select("work_ein").distinct()),
                emprnm_df.empr_id == col("work_ein"),
                "inner",
            )
            .select(emprnm_df["*"])
            .withColumn("empr_nm_norm", upper(trim(col("empr_nm"))))
            .alias("emprnm_lookup")
        )
        emprnm_for_feins = emprnm_for_feins.cache()
        emprnm_for_feins_count = emprnm_for_feins.count()
        LOGGER.info(
            "[STAGE 4] emprnm_for_feins cached: "
            f"{emprnm_for_feins_count:,} records"
        )

        # Prefilter: drop cross-product pairs where length disparity
        # guarantees a Jaro-Winkler score below threshold.
        # Trimmed lengths used — fixed-width fields have trailing spaces
        # that inflate raw length and defeat the ratio check.
        # Ratio tightened to 0.72: empirically JW >= 0.781 requires
        # min/max >= ~0.65; 0.72 gives a small safety margin while
        # pruning more candidates than the original 0.7.
        # Min length of 4: JW >= 0.781 is near-impossible for strings
        # shorter than 4 characters.
        emprnm_for_feins_limited = (
            emprnm_for_feins
            .withColumn("_nm_len", length(trim(col("empr_nm"))))
            .filter(col("_nm_len") >= 4)
            .dropDuplicates(["empr_id", "empr_nm_norm"])
            .alias("emprnm_lookup")
        )
        emprnm_for_feins_limited = emprnm_for_feins_limited.cache()
        emprnm_for_feins_limited_count = emprnm_for_feins_limited.count()
        LOGGER.info(
            "[STAGE 4] emprnm_for_feins_limited cached: "
            f"{emprnm_for_feins_limited_count:,} records"
        )

        input_names = (
            non_pseudo_df.select("work_ein", "qw_employer_name")
            .distinct()
            .withColumn(
                "qw_employer_name_norm",
                upper(trim(col("qw_employer_name")))
            )
            .withColumn("_qw_len", length(trim(col("qw_employer_name"))))
            .filter(col("_qw_len") > 0)
            .alias("input")
        )

        exact_match = input_names.join(
            emprnm_for_feins,
            (
                col("input.qw_employer_name_norm")
                == col("emprnm_lookup.empr_nm_norm")
            )
            & (col("input.work_ein") == col("emprnm_lookup.empr_id")),
            "left",
        ).select(
            col("input.work_ein").alias("work_ein"),
            col("input.qw_employer_name").alias("qw_employer_name"),
            col("emprnm_lookup.nm_snum").alias("matched_nm_snum"),
            when(col("emprnm_lookup.nm_snum").isNotNull(), lit("F"))
            .otherwise(lit("N"))
            .alias("name_match"),
            when(col("emprnm_lookup.nm_snum").isNotNull(), lit("F"))
            .otherwise(lit("N"))
            .alias("name_ind"),
        )
        exact_match = exact_match.cache()
        exact_match_count = exact_match.count()
        LOGGER.info(
            "[STAGE 4] exact_match cached: "
            f"{exact_match_count:,} records"
        )

        jw_start = time.time()
        LOGGER.info("[STAGE 4.1] Jaro-Winkler fuzzy match starting")
        fuzzy_input = (
            exact_match.filter(col("name_match") == "N")
            .select("work_ein", "qw_employer_name")
            .withColumn("qw_employer_name_norm",
                        upper(trim(col("qw_employer_name"))))
            .dropDuplicates(["work_ein", "qw_employer_name_norm"])
            .withColumn("_qw_len", length(col("qw_employer_name_norm")))
            .filter(col("_qw_len") >= 4)
            .alias("fuzzy_input")
        )
        fuzzy_candidates = (
            fuzzy_input
            .join(
                emprnm_for_feins_limited,
                (col("fuzzy_input.work_ein") == col("emprnm_lookup.empr_id"))
                & (
                    least(
                        col("fuzzy_input._qw_len"),
                        col("emprnm_lookup._nm_len"),
                    )
                    >= lit(0.72)
                    * greatest(
                        col("fuzzy_input._qw_len"),
                        col("emprnm_lookup._nm_len"),
                    )
                ),
                "inner",
            )
            .withColumn(
                "jw_score",
                fpls.compare.jaro_winkler_score_udf(
                    "fuzzy_input.qw_employer_name",
                    "emprnm_lookup.empr_nm",
                ),
            )
            .filter(col("jw_score") >= fein_name_threshold)
            .drop("_nm_len", "_qw_len")
        )
        fuzzy_candidates = fuzzy_candidates.cache()
        fuzzy_count = fuzzy_candidates.count()
        LOGGER.info(
            "[STAGE 4.1] Jaro-Winkler fuzzy match complete: "
            f"{fuzzy_count:,} candidates in "
            f"{time.time() - jw_start:.2f}s"
        )

        window_best = Window.partitionBy(
            "work_ein", "qw_employer_name"
        ).orderBy(col("jw_score").desc())
        best_fuzzy = fuzzy_candidates.withColumn(
            "rnk", rank().over(window_best)
        ).filter(col("rnk") == 1)

        fuzzy_match = best_fuzzy.select(
            "work_ein",
            "qw_employer_name",
            col("nm_snum").alias("matched_nm_snum"),
            lit("F").alias("name_match"),
            lit("F").alias("name_ind"),
            col("jw_score"),
        )

        name_match_result = exact_match.filter(
            col("name_match") == "F"
        ).unionByName(fuzzy_match, allowMissingColumns=True)
        window_dedup_name = Window.partitionBy(
            "work_ein", "qw_employer_name"
        ).orderBy(
            when(col("name_match") == "F", lit(0)).otherwise(lit(1)),
            col("matched_nm_snum").asc_nulls_last(),
        )
        name_match_result = (
            name_match_result
            .withColumn("_rn", row_number().over(window_dedup_name))
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        max_nm_snum_per_fein = broadcast(
            emprnm_df.groupBy("empr_id").agg(
                spark_max("nm_snum").alias("max_nm_snum")
            )
        ).cache()

        new_names = name_match_result.filter(col("name_match") == "N")
        # window_new_seq assigns sequence numbers within each FEIN partition.
        # No separate rank step needed — row_number() over the same window
        # in new_names_with_seq handles ordering directly.
        window_new_seq = Window.partitionBy(
            "work_ein").orderBy("qw_employer_name")
        new_names_with_seq = (
            new_names
            .join(
                max_nm_snum_per_fein,
                new_names.work_ein == max_nm_snum_per_fein.empr_id,
                "left",
            )
            .withColumn("_seq_rank", row_number().over(window_new_seq))
            .withColumn(
                "new_nm_snum",
                coalesce(col("max_nm_snum"), lit(0)) + col("_seq_rank")
            )
            .drop("_seq_rank")
            .select(
                "work_ein",
                "qw_employer_name",
                col("new_nm_snum").alias("matched_nm_snum"),
                lit("N").alias("name_match"),
                lit("N").alias("name_ind"),
            )
        )
        new_names_with_seq = new_names_with_seq.cache()
        new_names_with_seq_count = new_names_with_seq.count()
        LOGGER.info(
            "[STAGE 4] new_names_with_seq: "
            f"{new_names_with_seq_count:,} new name records"
        )

        final_name_match = name_match_result.filter(
            col("name_match") == "F"
        ).unionByName(new_names_with_seq, allowMissingColumns=True)
        final_name_match = final_name_match.cache()
        final_name_match_count = final_name_match.count()
        LOGGER.info(
            "[STAGE 4] final_name_match cached: "
            f"{final_name_match_count:,} records"
        )

        non_pseudo_with_name = (
            non_pseudo_df.alias("left")
            .join(
                final_name_match.alias("right"),
                (col("left.work_ein") == col("right.work_ein"))
                & (
                    col("left.qw_employer_name")
                    == col("right.qw_employer_name")
                ),
                "left",
            )
            .select(
                col("left.*"),
                col("right.matched_nm_snum"),
                col("right.name_ind"),
            )
            .withColumn(
                "empr_name_pntr", coalesce(col("matched_nm_snum"), lit(0))
            )
            .withColumn("empr_name_ind", coalesce(col("name_ind"), lit(" ")))
            .drop("matched_nm_snum", "name_ind")
        )

        emprnm_for_feins.unpersist()
        emprnm_for_feins_limited.unpersist()
        exact_match.unpersist()
        fuzzy_candidates.unpersist()
        new_names_with_seq.unpersist()
        final_name_match.unpersist()
        non_pseudo_df.unpersist()
    else:
        non_pseudo_with_name = non_pseudo_df.withColumn(
            "empr_name_pntr", lit(0)
        ).withColumn("empr_name_ind", lit(" "))

    # STAGED CHECKPOINT 3: Save name-matched data
    if non_pseudo_cnt > 0:
        non_pseudo_with_name.write.mode("overwrite").parquet(
            f"{checkpoint_path}name_matched/"
        )
        LOGGER.info("[CHECKPOINT] Saved name_matched results")
        non_pseudo_with_name = spark.read.parquet(
            f"{checkpoint_path}name_matched/"
        ).cache()
        name_matched_reloaded_cnt = non_pseudo_with_name.count()
        LOGGER.info(
            "[CHECKPOINT] Re-loaded name_matched from checkpoint "
            f"({name_matched_reloaded_cnt:,} rows)"
        )
        LOGGER.info(
            "[TIMER] Stage 4 (name matching) completed in "
            f"{time.time() - section_start:.2f}s"
        )

    LOGGER.info("[STAGE] Stage 5 starting: Address matching")
    section_start = time.time()

    # Pre-normalize eaddr_df once for both primary and optional address joins.
    # A single window dedup pass (addr_snum DESC per normalized key) serves
    # both join sites, eliminating the duplicate window operation that
    # eaddr_df_norm previously performed over the same 3.2M-row table.
    eaddr_deduped = eaddr_df \
        .withColumn("_addr1", upper(trim(col("addr1")))) \
        .withColumn("_city",  upper(trim(col("city")))) \
        .withColumn("_state", upper(trim(col("state")))) \
        .withColumn("_zip5",  upper(trim(col("zip5")))) \
        .withColumn("_rn", row_number().over(
            Window.partitionBy(
                "empr_id", "_addr1", "_city", "_state", "_zip5"
            ).orderBy(col("addr_snum").desc())
        )) \
        .filter(col("_rn") == 1) \
        .drop("_rn") \
        .withColumnRenamed("_addr1", "addr1_norm") \
        .withColumnRenamed("_city", "city_norm") \
        .withColumnRenamed("_state", "state_norm") \
        .withColumnRenamed("_zip5", "zip5_norm") \
        .cache()

    # Pre-normalize non_pseudo_with_name for join
    non_pseudo_with_name = non_pseudo_with_name \
        .withColumn("addr1_norm", upper(trim(col("addr1")))) \
        .withColumn("city_norm", upper(trim(col("city")))) \
        .withColumn("state_norm", upper(trim(col("state")))) \
        .withColumn("zip5_norm", upper(trim(col("zip5"))))

    addr_match = non_pseudo_with_name.join(
        eaddr_deduped,
        (non_pseudo_with_name.work_ein == eaddr_deduped.empr_id)
        & (trim(non_pseudo_with_name.addr1) != "")
        & (non_pseudo_with_name.addr1_norm == eaddr_deduped.addr1_norm)
        & (non_pseudo_with_name.city_norm == eaddr_deduped.city_norm)
        & (non_pseudo_with_name.state_norm == eaddr_deduped.state_norm)
        & (non_pseudo_with_name.zip5_norm == eaddr_deduped.zip5_norm),
        "left",
    ).select(
        non_pseudo_with_name["*"],
        eaddr_deduped.addr_snum.alias("matched_addr_snum"),
    )

    addr_match = addr_match.withColumn(
        "empr_name_ind_preserved", col("empr_name_ind")
    )
    addr_match = addr_match.cache()
    addr_match_count = addr_match.count()
    LOGGER.info(
        "[STAGE 5] addr_match cached: "
        f"{addr_match_count:,} records"
    )

    max_addr_snum_per_fein = broadcast(
        eaddr_df.groupBy("empr_id").agg(
            spark_max("addr_snum").alias("max_addr_snum")
        )
    ).cache()

    new_addrs_distinct = (
        addr_match.filter(col("matched_addr_snum").isNull())
        .select(
            "work_ein",
            "addr1",
            "city",
            "state",
            "zip5",
            "input_order",
        )
        .dropDuplicates(["work_ein", "addr1", "city", "state", "zip5"])
        .withColumn("addr_type", lit("primary"))
    )

    window_all_addr = Window.partitionBy("work_ein").orderBy("input_order")
    new_addrs_with_seq = (
        new_addrs_distinct.join(
            max_addr_snum_per_fein,
            new_addrs_distinct.work_ein == max_addr_snum_per_fein.empr_id,
            "left",
        )
        .withColumn("addr_rank", row_number().over(window_all_addr))
        .withColumn(
            "new_addr_snum",
            coalesce(col("max_addr_snum"), lit(0)) + col("addr_rank"),
        )
        .drop(
            "max_addr_snum", "addr_rank", "empr_id", "addr_type", "input_order"
        )
    )
    new_addrs_with_seq = new_addrs_with_seq.cache()
    new_addrs_with_seq_count = new_addrs_with_seq.count()
    LOGGER.info(
        "[STAGE 5] new_addrs_with_seq cached: "
        f"{new_addrs_with_seq_count:,} records"
    )

    existing_feins = broadcast(emprnm_df.select("empr_id").distinct()).alias(
        "existing_feins"
    )
    pseudo_feins_broadcast = broadcast(
        pseudo_fein_df.select("work_ein")
        .distinct()
        .withColumnRenamed("work_ein", "pseudo_work_ein")
    ).alias("pseudo_feins")

    window_first_occurrence = Window.partitionBy(
        "work_ein", "addr1", "city", "state", "zip5"
    ).orderBy("input_order")
    addr_with_seq = (
        addr_match.join(
            new_addrs_with_seq,
            ["work_ein", "addr1", "city", "state", "zip5"],
            "left",
        )
        .join(
            existing_feins,
            col("work_ein") == col("existing_feins.empr_id"),
            "left",
        )
        .join(
            pseudo_feins_broadcast,
            col("work_ein") == col("pseudo_feins.pseudo_work_ein"),
            "left",
        )
        .withColumn(
            "addr_occurrence_rank", row_number().over(window_first_occurrence)
        )
        .withColumn(
            "empr_addr_pntr",
            when(
                col("matched_addr_snum").isNotNull(), col("matched_addr_snum")
            ).otherwise(col("new_addr_snum")),
        )
        .withColumn(
            "empr_addr_ind",
            when(col("vald_ein_cd") == "I", lit("X"))
            .when(col("matched_addr_snum").isNotNull(), lit("F"))
            .when(
                (col("matched_addr_snum").isNull())
                & (col("addr_occurrence_rank") == 1),
                lit("N"),
            )
            .otherwise(lit("F")),
        )
        .withColumn(
            "empr_addr_inp_ind_cd",
            when(col("matched_addr_snum").isNotNull(), lit("F")).otherwise(
                lit("N")
            ),
        )
        .withColumn(
            "empr_table_ind",
            when(col("pseudo_feins.pseudo_work_ein").isNotNull(), lit("O"))
            .when(col("existing_feins.empr_id").isNotNull(), lit("F"))
            .otherwise(lit("N")),
        )
        .drop(
            "matched_addr_snum",
            "new_addr_snum",
            "empr_id",
            "pseudo_work_ein",
            "addr_occurrence_rank",
        )
    )

    addr_match.unpersist()

    # Pre-normalize optional address fields for join
    addr_with_seq = addr_with_seq \
        .withColumn("opt_addr1_norm", upper(trim(col("opt_addr1")))) \
        .withColumn("opt_city_norm", upper(trim(col("opt_city")))) \
        .withColumn("opt_state_norm", upper(trim(col("opt_state")))) \
        .withColumn("opt_zip5_norm", upper(trim(col("opt_zip5"))))

    # Reuse eaddr_deduped (already normalized and deduped above) for the
    # optional address join, eliminating the second window pass over eaddr_df.
    opt_addr_match = (
        addr_with_seq.alias("addr_seq")
        .join(
            eaddr_deduped.alias("eaddr_lookup"),
            (col("addr_seq.work_ein") == col("eaddr_lookup.empr_id"))
            & (trim(col("addr_seq.opt_addr1")) != "")
            & (
                col("addr_seq.opt_addr1_norm")
                == col("eaddr_lookup.addr1_norm")
            )
            & (
                col("addr_seq.opt_city_norm")
                == col("eaddr_lookup.city_norm")
            )
            & (
                col("addr_seq.opt_state_norm")
                == col("eaddr_lookup.state_norm")
            )
            & (
                col("addr_seq.opt_zip5_norm")
                == col("eaddr_lookup.zip5_norm")
            ),
            "left",
        )
        .select(
            col("addr_seq.*"),
            col("eaddr_lookup.addr_snum").alias("matched_opt_snum"),
        )
    )

    window_opt = Window.partitionBy(
        "work_ein",
        "opt_addr1",
        "opt_city",
        "opt_state",
        "opt_zip5",
    ).orderBy("input_order")
    opt_with_rank = opt_addr_match.withColumn(
        "opt_rank",
        when(
            trim(col("opt_addr1")) == "",
            lit(1)
        ).otherwise(row_number().over(window_opt))
    )
    opt_with_rank = opt_with_rank.cache()
    opt_with_rank_count = opt_with_rank.count()
    LOGGER.info(
        "[STAGE 5] opt_with_rank cached: "
        f"{opt_with_rank_count:,} records"
    )
    # eaddr_deduped has now been consumed at both join sites.
    eaddr_deduped.unpersist()

    new_opt_addrs = (
        opt_with_rank.filter(
            (col("matched_opt_snum").isNull())
            & (col("opt_rank") == 1)
            & (trim(col("opt_addr1")) != "")
        )
        .select(
            col("work_ein"),
            col("opt_addr1").alias("addr1"),
            col("opt_city").alias("city"),
            col("opt_state").alias("state"),
            col("opt_zip5").alias("zip5"),
        )
        # FIX: exclude optional addresses that are already being inserted
        # as primary addresses in this same run. Without this filter,
        # the same physical address gets two different addr_snum values
        # (one from primary path, one from optional path).
        .join(
            new_addrs_with_seq.select(
                "work_ein", "addr1", "city", "state", "zip5"
            ),
            ["work_ein", "addr1", "city", "state", "zip5"],
            "left_anti",
        )
        .distinct()
        .withColumn("addr_type", lit("optional"))
        .withColumn("input_order", lit(999999999))
    )

    max_after_primary = new_addrs_with_seq.groupBy("work_ein").agg(
        spark_max("new_addr_snum").alias("max_primary_snum")
    )

    window_opt_seq = Window.partitionBy("work_ein").orderBy("addr1")
    new_opt_with_seq = (
        new_opt_addrs.join(max_after_primary, "work_ein", "left")
        .join(
            max_addr_snum_per_fein,
            new_opt_addrs.work_ein == max_addr_snum_per_fein.empr_id,
            "left",
        )
        .withColumn("opt_rank", row_number().over(window_opt_seq))
        .withColumn(
            "new_opt_snum",
            coalesce(col("max_primary_snum"), col("max_addr_snum"), lit(0))
            + col("opt_rank"),
        )
        .drop(
            "max_primary_snum",
            "max_addr_snum",
            "opt_rank",
            "empr_id",
            "addr_type",
            "input_order",
        )
        .withColumnRenamed("addr1", "opt_addr1")
        .withColumnRenamed("city", "opt_city")
        .withColumnRenamed("state", "opt_state")
        .withColumnRenamed("zip5", "opt_zip5")
    )

    final_df = (
        opt_with_rank.join(
            new_opt_with_seq,
            [
                "work_ein",
                "opt_addr1",
                "opt_city",
                "opt_state",
                "opt_zip5",
            ],
            "left",
        )
        # FIX: also look up new primary snums in case the optional address
        # matches a primary address being inserted in this same run.
        .join(
            new_addrs_with_seq.select(
                col("work_ein"),
                col("addr1").alias("opt_addr1"),
                col("city").alias("opt_city"),
                col("state").alias("opt_state"),
                col("zip5").alias("opt_zip5"),
                col("new_addr_snum").alias("primary_match_snum"),
            ),
            ["work_ein", "opt_addr1", "opt_city", "opt_state", "opt_zip5"],
            "left",
        )
        .withColumn(
            "empr_opt_addr_pntr",
            when(col("matched_opt_snum").isNotNull(), col("matched_opt_snum"))
            .when(col("primary_match_snum").isNotNull(), col("primary_match_snum"))
            .when(col("new_opt_snum").isNotNull(), col("new_opt_snum"))
            .otherwise(lit(0)),
        )
        .withColumn(
            "empr_opt_addr_ind",
            when(trim(col("opt_addr1")) == "", lit(" "))
            .when(col("matched_opt_snum").isNotNull(), lit("F"))
            .when(col("opt_rank") == 1, lit("N"))
            .otherwise(lit("F")),
        )
        .withColumn("empr_name_ind", col("empr_name_ind_preserved"))
        .withColumn(
            "empr_table_ind", coalesce(col("empr_table_ind"), lit(" "))
        )
        .withColumn("qw_empr_forgn_cc", col("empr_forgn_cc"))
        .withColumn("qw_empr_forgn_c_name", col("empr_forgn_c_name"))
        .withColumn("qw_empr_forgn_zip", col("empr_forgn_zip"))
        .withColumn("qw_er_opt_frn_cc", col("opt_empr_forgn_cc"))
        .withColumn("qw_er_opt_frn_c_name", col("opt_empr_forgn_c_name"))
        .withColumn("qw_er_opt_frn_zip", col("opt_empr_forgn_zip"))
        .drop(
            "opt_rank",
            "matched_opt_snum",
            "new_opt_snum",
            "primary_match_snum",
            "empr_name_ind_preserved",
        )
    )

    # STAGED CHECKPOINT 4: Save address-matched data
    final_df_count = final_df.count()
    LOGGER.info(
        "[STAGE 5] final_df count: "
        f"{final_df_count:,} records"
    )
    final_df.write.mode("overwrite").parquet(f"{checkpoint_path}addr_matched/")
    LOGGER.info("[CHECKPOINT] Saved addr_matched results")
    non_pseudo_with_name.unpersist()
    final_df = spark.read.parquet(f"{checkpoint_path}addr_matched/").cache()
    addr_matched_reloaded_cnt = final_df.count()
    LOGGER.info(
        "[CHECKPOINT] Re-loaded addr_matched from checkpoint "
        f"({addr_matched_reloaded_cnt:,} rows)"
    )
    LOGGER.info(
        "[TIMER] Stage 5 (address matching) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Unpersist first-lifecycle cached DFs before re-reading from
    # checkpoint. Prevents cache accumulation across stages.
    emprnm_df.unpersist()
    eaddr_df.unpersist()
    empraddr_df.unpersist()
    empnmadr_perm.unpersist()
    # Re-read database tables from checkpoints
    emprnm_df = spark.read.parquet(f"{checkpoint_path}db_emprnm/").cache()
    eaddr_df = spark.read.parquet(f"{checkpoint_path}db_eaddr/").cache()
    empraddr_df = spark.read.parquet(
        f"{checkpoint_path}db_empraddr/"
    ).cache()
    empnmadr_perm = spark.read.parquet(
        f"{checkpoint_path}db_empnmadr/"
    ).cache()
    unk_addr_df = spark.read.parquet(f"{checkpoint_path}unknown/").cache()
    if pseudo_cnt > 0:
        pseudo_fein_df = spark.read.parquet(f"{checkpoint_path}pseudo/")
        pseudo_fein_df = pseudo_fein_df.cache()
        pseudo_fein_df_for_db = spark.read.parquet(
            f"{checkpoint_path}pseudo_for_db/"
        ).cache()

    # Name retrieval for blank employer names
    LOGGER.info(
        "[STAGE] Stage 6 starting: Union + output preparation"
    )
    section_start = time.time()
    blank_name_df_check = final_df.filter(
        (trim(col("qw_employer_name")) == "") & (col("empr_addr_pntr") > 0)
    )

    # Use limit(1) first to short-circuit on the common case of no blanks,
    # then count only if blanks exist. Cap at 101 so the scan stops as soon
    # as we know whether we are in the <= 100 or > 100 branch.
    blank_name_count = 0
    if blank_name_df_check.limit(1).count() > 0:
        blank_name_count = blank_name_df_check.limit(101).count()

    if blank_name_count == 0:
        LOGGER.info("No records with blank employer names found")
    elif blank_name_count <= 100:

        blank_feins = [
            row.work_ein
            for row in blank_name_df_check.select("work_ein")
            .distinct()
            .collect()
        ]
        cursor = connection.cursor()
        try:
            placeholders = ",".join(["%s"] * len(blank_feins))
            cursor.execute(
                "SELECT empr_id, empr_nm FROM pndnh.EMPRNM "
                f"WHERE empr_id IN ({placeholders})",
                blank_feins,
            )
            name_map = {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()
        LOGGER.info(f"Retrieved {len(name_map)} employer names via direct SQL")
        if name_map:
            name_map_bc = spark.sparkContext.broadcast(name_map)

            def lookup_name(ein, current_name):
                if current_name and current_name.strip():
                    return current_name
                return name_map_bc.value.get(ein, current_name)

            lookup_udf = udf(lookup_name, StringType())
            final_df = final_df.withColumn(
                "qw_employer_name",
                lookup_udf(col("work_ein"), col("qw_employer_name")),
            )
    else:
        blank_name_start = time.time()
        blank_name_df = blank_name_df_check
        blank_feins = broadcast(blank_name_df.select("work_ein").distinct())
        empnmadr_filtered = empnmadr_perm.join(
            blank_feins, empnmadr_perm.empr_id == blank_feins.work_ein, "inner"
        ).cache()
        empnmadr_filtered.count()
        emprnm_filtered = emprnm_df.join(
            blank_feins, emprnm_df.empr_id == blank_feins.work_ein, "inner"
        ).cache()
        emprnm_filtered.count()

        name_lookup_df = (
            blank_name_df.alias("blank")
            .join(
                empnmadr_filtered.alias("empnmadr"),
                (col("blank.work_ein") == col("empnmadr.empr_id"))
                & (col("blank.empr_addr_pntr") == col("empnmadr.addr_snum")),
                "left",
            )
            .select(
                col("blank.work_ein").alias("work_ein"),
                col("blank.empr_addr_pntr").alias("empr_addr_pntr"),
                col("empnmadr.nm_snum").alias("retrieved_nm_snum"),
            )
        )

        retrieved_names_df = (
            name_lookup_df.filter(col("retrieved_nm_snum").isNotNull())
            .alias("lookup")
            .join(
                emprnm_filtered.alias("emprnm"),
                (col("lookup.work_ein") == col("emprnm.empr_id"))
                & (col("lookup.retrieved_nm_snum") == col("emprnm.nm_snum")),
                "left",
            )
            .select(
                col("lookup.work_ein").alias("work_ein"),
                col("lookup.empr_addr_pntr").alias("empr_addr_pntr"),
                col("lookup.retrieved_nm_snum").alias("retrieved_nm_snum"),
                col("emprnm.empr_nm").alias("retrieved_name"),
            )
            .filter(col("retrieved_name").isNotNull())
            .cache()
        )

        retrieved_count = retrieved_names_df.count()

        # Dedup retrieved_names_df to prevent fan-out in Join #22.
        # EMPNMADR can have multiple names per (empr_id, addr_snum);
        # downstream join only needs one canonical name per
        # (work_ein, empr_addr_pntr). Lowest nm_snum wins (oldest
        # name, matches COBOL first-record-wins behavior).
        retrieved_names_dedup_window = Window.partitionBy(
            "work_ein", "empr_addr_pntr"
        ).orderBy(col("retrieved_nm_snum").asc_nulls_last())
        retrieved_names_df = (
            retrieved_names_df
            .withColumn(
                "_rn", row_number().over(retrieved_names_dedup_window)
            )
            .filter(col("_rn") == 1)
            .drop("_rn")
        )
        LOGGER.info(
            "retrieved_names_df deduped "
            "(one per work_ein + empr_addr_pntr)"
        )
        LOGGER.info(
            "[TIMER] Stage 6 (blank name retrieval) completed in "
            f"{time.time() - blank_name_start:.2f}s"
        )

        if retrieved_count > 0:
            final_df = (
                final_df.alias("main")
                .join(
                    retrieved_names_df.alias("retr"),
                    (col("main.work_ein") == col("retr.work_ein"))
                    & (
                        col("main.empr_addr_pntr")
                        == col("retr.empr_addr_pntr")
                    ),
                    "left",
                )
                .select(
                    col("main.*"),
                    when(
                        col("retr.retrieved_name").isNotNull(),
                        col("retr.retrieved_name"),
                    )
                    .otherwise(col("main.qw_employer_name"))
                    .alias("qw_employer_name_new"),
                    when(
                        col("retr.retrieved_nm_snum").isNotNull(),
                        col("retr.retrieved_nm_snum"),
                    )
                    .otherwise(col("main.empr_name_pntr"))
                    .alias("empr_name_pntr_new"),
                    when(col("retr.retrieved_name").isNotNull(), lit("R"))
                    .otherwise(col("main.empr_name_ind"))
                    .alias("empr_name_ind_new"),
                )
                .drop("qw_employer_name", "empr_name_pntr", "empr_name_ind")
                .withColumnRenamed("qw_employer_name_new", "qw_employer_name")
                .withColumnRenamed("empr_name_pntr_new", "empr_name_pntr")
                .withColumnRenamed("empr_name_ind_new", "empr_name_ind")
                .withColumn(
                    "empr_table_ind", coalesce(col("empr_table_ind"), lit(" "))
                )
            )

    # Get max sequence numbers for pseudo-FEINs
    LOGGER.info("[STAGE] Stage 6b starting: pseudo-FEIN seq + union prep")
    section_start = time.time()
    max_empraddr_snum = broadcast(
        empraddr_df.groupBy("empr_id").agg(
            spark_max("addr_snum").alias("max_empr_snum")
        )
    ).cache()

    window_pseudo = Window.partitionBy("empr_ein").orderBy("input_order")
    pseudo_with_seq = (
        pseudo_fein_df_for_db.join(
            max_empraddr_snum,
            pseudo_fein_df_for_db.empr_ein == max_empraddr_snum.empr_id,
            "left",
        )
        .withColumn("addr_rank", row_number().over(window_pseudo))
        .withColumn(
            "empr_snum",
            coalesce(col("max_empr_snum"), lit(0)) + col("addr_rank"),
        )
        .drop("max_empr_snum", "addr_rank", "empr_id")
    )
    unk_with_match = (
        unk_addr_df.withColumn("name_match", lit(None).cast("string"))
        .withColumn("empr_addr_ind", lit(" "))
        .withColumn("empr_opt_addr_ind", lit(" "))
        .withColumn("empr_id", lit(None).cast("string"))
        .withColumn("empr_name_ind", lit(" "))
        .withColumn("empr_table_ind", lit(" "))
    )

    unk_with_match = (
        unk_with_match.withColumn("qw_empr_forgn_cc", col("empr_forgn_cc"))
        .withColumn("qw_empr_forgn_c_name", col("empr_forgn_c_name"))
        .withColumn("qw_empr_forgn_zip", col("empr_forgn_zip"))
        .withColumn("qw_er_opt_frn_cc", col("opt_empr_forgn_cc"))
        .withColumn("qw_er_opt_frn_c_name", col("opt_empr_forgn_c_name"))
        .withColumn("qw_er_opt_frn_zip", col("opt_empr_forgn_zip"))
    )

    pseudo_for_parquet = pseudo_fein_df.join(
        pseudo_with_seq.select(
            "empr_ein", "qw_employer_name",
            "addr1", "addr2", "addr3", "city", "state", "zip5", "zip4",
            "empr_snum"
        ),
        ["empr_ein", "qw_employer_name",
         "addr1", "addr2", "addr3", "city", "state", "zip5", "zip4"],
        "left"
    ).withColumn("empr_addr_pntr", coalesce(col("empr_snum"), lit(0)))

    pseudo_with_match = (
        pseudo_for_parquet.withColumn("name_match", lit(None).cast("string"))
        .withColumn(
            "empr_addr_ind",
            when(col("vald_ein_cd") == "I", lit("X")).otherwise(lit("N")),
        )
        .withColumn("empr_opt_addr_ind", lit(" "))
        .withColumn("empr_id", lit(None).cast("string"))
        .withColumn("empr_table_ind", lit("O"))
        .withColumn("empr_name_ind", lit(" "))
        .withColumn("qw_empr_forgn_cc", col("empr_forgn_cc"))
        .withColumn("qw_empr_forgn_c_name", col("empr_forgn_c_name"))
        .withColumn("qw_empr_forgn_zip", col("empr_forgn_zip"))
        .withColumn("qw_er_opt_frn_cc", col("opt_empr_forgn_cc"))
        .withColumn("qw_er_opt_frn_c_name", col("opt_empr_forgn_c_name"))
        .withColumn("qw_er_opt_frn_zip", col("opt_empr_forgn_zip"))
    )

    required_indicators = [
        "empr_table_ind",
        "empr_name_ind",
        "empr_addr_ind",
        "empr_opt_addr_ind",
    ]

    for ind_col in required_indicators:
        if ind_col not in final_df.columns:
            LOGGER.warning(f"Adding missing {ind_col} to final_df")
            final_df = final_df.withColumn(ind_col, lit(" "))

    for ind_col in required_indicators:
        if ind_col not in unk_with_match.columns:
            unk_with_match = unk_with_match.withColumn(ind_col, lit(" "))

    for ind_col in required_indicators:
        if ind_col not in pseudo_with_match.columns:
            pseudo_with_match = pseudo_with_match.withColumn(ind_col, lit(" "))

    expected_columns = list(final_df.columns)
    for ind_col in required_indicators:
        if ind_col not in expected_columns:
            expected_columns.append(ind_col)

    foreign_fields = [
        "qw_empr_forgn_cc",
        "qw_empr_forgn_c_name",
        "qw_empr_forgn_zip",
        "qw_er_opt_frn_cc",
        "qw_er_opt_frn_c_name",
        "qw_er_opt_frn_zip",
    ]
    for fld in foreign_fields:
        if fld not in expected_columns:
            expected_columns.append(fld)

    def align(df, cols):
        foreign_fields = [
            "qw_empr_forgn_cc",
            "qw_empr_forgn_c_name",
            "qw_empr_forgn_zip",
            "qw_er_opt_frn_cc",
            "qw_er_opt_frn_c_name",
            "qw_er_opt_frn_zip",
        ]
        for c in cols:
            if c not in df.columns:
                if c in required_indicators:
                    df = df.withColumn(c, lit(" "))
                elif c in foreign_fields:
                    df = df.withColumn(c, lit(None).cast(StringType()))
                else:
                    df = df.withColumn(c, lit(None))
        return df.select(cols)

    dfs_to_union = []
    if unk_cnt > 0:
        unk_with_match = align(unk_with_match, expected_columns)
        dfs_to_union.append(unk_with_match)
    if pseudo_cnt > 0:
        pseudo_with_match = align(pseudo_with_match, expected_columns)
        dfs_to_union.append(pseudo_with_match)
    if non_pseudo_cnt > 0:
        final_df = align(final_df, expected_columns)
        dfs_to_union.append(final_df)

    if len(dfs_to_union) == 0:
        all_records = spark.createDataFrame([], schema=final_df.schema)
    elif len(dfs_to_union) == 1:
        all_records = dfs_to_union[0]
    else:
        all_records = dfs_to_union[0]
        for df in dfs_to_union[1:]:
            all_records = all_records.unionByName(df)

    for ind_col in required_indicators:
        if ind_col not in all_records.columns:
            LOGGER.warning(
                f"Adding missing indicator column after union: {ind_col}"
            )
            all_records = all_records.withColumn(ind_col, lit(" "))

    LOGGER.info(
        "[TIMER] Stage 6 (union construction) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Fan-out safety check. Performed before re-joining the value column
    # to avoid triggering a BroadcastExchangeExec on the ~34 GB value
    # parquet when spark.driver.maxResultSize is 1 GB.
    # eaddr_deduped at both join sites makes duplicates impossible;
    # this assert catches any future regression loudly.
    section_start = time.time()
    all_records = all_records.cache()
    all_records_cnt = all_records.count()
    LOGGER.info(
        f"Materialized all_records: {all_records_cnt} rows "
        f"(expected: {total_cnt})"
    )
    if all_records_cnt != total_cnt:
        raise AssertionError(
            f"Fan-out detected: all_records={all_records_cnt}, "
            f"expected={total_cnt}. Check eaddr dedup at both join "
            "sites before proceeding."
        )

    # Re-join value column from parsed checkpoint for output
    # reconstruction. Value was dropped earlier to save memory.
    # Disable auto-broadcast: value_for_output is ~34 GB (21M rows x
    # 1604-byte fixed-width strings) and must never be broadcast to
    # the driver regardless of autoBroadcastJoinThreshold setting.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    value_for_output = (
        spark.read.parquet(f"{checkpoint_path}parsed/")
        .select("input_order", "value")
    )
    all_records = all_records.join(
        value_for_output, ["input_order"], "left"
    )
    # Restore broadcast threshold for any remaining operations
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)

    all_records = all_records.withColumn(
        "value",
        concat(
            substring(col("value"), 1, 336),
            col("empr_table_ind"),
            col("empr_name_ind"),
            lpad(
                coalesce(col("empr_name_pntr"), lit(0)).cast("string"), 10, "0"
            ),
            col("empr_addr_ind"),
            lpad(
                coalesce(col("empr_addr_pntr"), lit(0)).cast("string"), 10, "0"
            ),
            col("empr_opt_addr_ind"),
            lpad(
                coalesce(col("empr_opt_addr_pntr"), lit(0)).cast("string"),
                10,
                "0",
            ),
            substring(col("value"), 371, 1234),
        ),
    )

    # Prepare parquet output
    parquet_output = all_records.select(
        col("work_ein").alias("qw_employer_ein"),
        col("file_name"),
        trim(col("file_rec_num_id")).cast("int").alias("file_rec_num_id"),
        col("file_addr1_id"),
        col("file_addr2_id"),
        col("connect_direct_node"),
        col("file_node_rdate"),
        col("file_node_ttime"),
        col("state_key_code_full").alias("state_key_code"),
        col("transmission_type"),
        col("dod_code"),
        col("sort_key_date"),
        col("sort_key_time"),
        col("batch_number"),
        col("ver_cntl"),
        col("submitter_datestamp"),
        col("receipt_datestamp"),
        col("hdr_bypass_cd_on"),
        col("is_batch_num_999999_test"),
        col("record_status"),
        col("cntl_fld_odc_or_filler"),
        col("out_err_cnt"),
        col("out_err_cd_1"),
        col("out_err_cd_2"),
        col("out_err_cd_3"),
        col("out_err_cd_4"),
        col("out_err_cd_5"),
        col("ssn_val_code"),
        col("ssn_vrfd_cd"),
        col("ssn_vldtn_cd"),
        col("namesearch_score_11"),
        col("namesearch_score_12"),
        col("final_ssn"),
        col("final_first_nm"),
        col("final_middle_nm"),
        col("final_last_nm"),
        col("final_empr_nm"),
        col("ssn_sequence_num"),
        col("submn_uid"),
        col("empr_ein"),
        col("empr_table_ind"),
        col("empr_name_ind"),
        col("empr_name_pntr"),
        col("qw_frgn_empr_flg_cc"),
        col("qw_frgn_empr_flg_cname"),
        col("qw_frgn_empr_flg_czip"),
        col("qw_frgn_opt_flg_cc"),
        col("qw_frgn_opt_flg_cname"),
        col("qw_frgn_opt_flg_czip"),
        col("vald_ssn_chg_cd"),
        col("vald_ein_cd"),
        col("vald_wage_amt_cd"),
        col("vald_rpt_prd_cd"),
        col("wrk_ssn"),
        col("wrk_first_nm"),
        col("wrk_middle_nm"),
        col("wrk_last_nm"),
        col("wrk_empr_nm"),
        col("wrk_rpt_prd_qtr"),
        col("wrk_rpt_prd_year"),
        col("wrk_wage_amt_x"),
        col("qw_identifier"),
        col("qw_employee_ssn"),
        col("qw_empe_first_name"),
        col("qw_empe_middle_name"),
        col("qw_empe_last_name"),
        col("qw_empe_wage_amt"),
        col("qw_rpt_period_q"),
        col("qw_rpt_period_cc"),
        col("qw_rpt_period_yy"),
        col("qw_employer_st_ein"),
        col("qw_employer_name"),
        col("qw_empr_address1"),
        col("qw_empr_address2"),
        col("qw_empr_address3"),
        col("qw_empr_city"),
        col("qw_empr_state"),
        col("qw_empr_zip_5"),
        col("qw_empr_zip_4"),
        coalesce(col("qw_empr_forgn_cc"), lit("")).alias("qw_empr_forgn_cc"),
        coalesce(col("qw_empr_forgn_c_name"), lit("")).alias(
            "qw_empr_forgn_c_name"
        ),
        coalesce(col("qw_empr_forgn_zip"), lit("")).alias("qw_empr_forgn_zip"),
        col("qw_empr_opt_address1"),
        col("qw_empr_opt_address2"),
        col("qw_empr_opt_address3"),
        col("qw_empr_opt_city"),
        col("qw_empr_opt_state"),
        col("qw_empr_opt_zip_5"),
        col("qw_empr_opt_zip_4"),
        coalesce(col("qw_er_opt_frn_cc"), lit("")).alias("qw_er_opt_frn_cc"),
        coalesce(col("qw_er_opt_frn_c_name"), lit("")).alias(
            "qw_er_opt_frn_c_name"
        ),
        coalesce(col("qw_er_opt_frn_zip"), lit("")).alias("qw_er_opt_frn_zip"),
        col("fnl_empr_address1"),
        col("fnl_empr_address2"),
        col("fnl_empr_address3"),
        col("fnl_empr_city"),
        col("fnl_empr_state"),
        col("fnl_empr_zip_5"),
        col("fnl_empr_zip_4"),
        col("fnl_empr_err_tab"),
        col("fnl_frgn_cc"),
        col("fnl_frgn_c_name"),
        col("fnl_frgn_zip"),
        col("fnl_opt_address1"),
        col("fnl_opt_address2"),
        col("fnl_opt_address3"),
        col("fnl_opt_city"),
        col("fnl_opt_state"),
        col("fnl_opt_zip_5"),
        col("fnl_opt_zip_4"),
        col("fnl_opt_err_tab"),
        col("fnl_opt_frgn_cc"),
        col("fnl_opt_frgn_c_name"),
        col("fnl_opt_frgn_zip"),
        trim(col("file_rec_num_id")).alias("seq_num"),
        col("empr_addr_pntr"),
        col("empr_opt_addr_pntr"),
        col("empr_addr_ind"),
        col("empr_opt_addr_ind"),
    )

    string_cols = [
        col_name
        for col_name in parquet_output.columns
        if dict(parquet_output.dtypes)[col_name] == "string"
    ]
    parquet_output = parquet_output.select(
        [
            trim(col(c)).alias(c)
            if c in string_cols and c != "state_key_code"
            else col(c)
            for c in parquet_output.columns
        ]
    )

    # Extract actual input filename from S3 path
    s3_resource = boto3.resource("s3")
    input_s3_prefix = input_prefix.replace(f"s3://{bucket_name}/", "")
    input_objects = [
        obj for obj in s3_resource.Bucket(bucket_name).objects.filter(
            Prefix=input_s3_prefix
        )
        if not obj.key.endswith("/")
    ]
    if not input_objects:
        raise Exception(
            f"No files found in s3://{bucket_name}/{job_name}/input/"
        )
    input_file_key = input_objects[0].key
    input_filename = input_file_key.split("/")[-1]
    LOGGER.info(f"[INPUT] Input filename: '{input_filename}'")
    output_filename = generate_output_filename(input_filename)
    LOGGER.info(f"Output filename: '{output_filename}'")

    section_start = time.time()
    LOGGER.info("[STAGE] Stage 7 starting: SEQID parquet output")
    LOGGER.info("Writing multi-partition parquet output (snappy)")
    parquet_output.write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet(staging_path)
    LOGGER.info(
        "[TIMER] Stage 7 (parquet write to staging) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Unpersist all_records after parquet output write.
    # DB tables (emprnm_df, eaddr_df, empraddr_df, empnmadr_perm)
    # remain cached for temp table operations - no re-read needed.
    # Eliminates redundant cache clear and duplicate DB re-reads.
    all_records.unpersist()
    LOGGER.info("[CHECKPOINT] all_records unpersisted; DB tables retained")

    LOGGER.info("[STAGE] Stage 8 starting: main temp table write")
    section_start = time.time()
    tmp_table_name = "pndnh.ndnh_qw_update_emp_table_job_tmp"

    def prepare_for_write(df):
        return df.select(
            col("work_ein"),
            col("qw_employer_name"),
            col("addr1"),
            col("addr2"),
            col("addr3"),
            col("city"),
            col("state"),
            col("zip5"),
            col("zip4"),
            col("empr_forgn_cc"),
            col("empr_forgn_c_name"),
            col("empr_forgn_zip"),
            col("opt_addr1"),
            col("opt_addr2"),
            col("opt_addr3"),
            col("opt_city"),
            col("opt_state"),
            col("opt_zip5"),
            col("opt_zip4"),
            col("opt_empr_forgn_cc"),
            col("opt_empr_forgn_c_name"),
            col("opt_empr_forgn_zip"),
            col("batch_number"),
            col("state_key_code"),
            col("empr_name_pntr"),
            col("empr_addr_pntr"),
            col("empr_opt_addr_pntr"),
            col("empr_table_ind"),
            col("empr_name_ind"),
            col("empr_addr_ind"),
            col("empr_opt_addr_ind"),
            col("empr_addr_inp_ind_cd"),
            col("fnl_empr_err_tab"),
        )

    write_dfs = []
    if unk_cnt > 0:
        write_dfs.append(prepare_for_write(unk_with_match))
    if pseudo_cnt > 0:
        write_dfs.append(prepare_for_write(pseudo_with_match))
    if non_pseudo_cnt > 0:
        write_dfs.append(prepare_for_write(final_df))

    if len(write_dfs) == 1:
        final_df_to_write = write_dfs[0].cache()
    else:
        final_df_to_write = write_dfs[0]
        for df in write_dfs[1:]:
            final_df_to_write = final_df_to_write.unionByName(df)
        final_df_to_write = final_df_to_write.cache()

    final_df_to_write = final_df_to_write.withColumn(
        "work_ein", rpad(col("work_ein"), 9, " ")
    )

    overwrite_tmp_table(
        tmp_table_name, final_df_to_write.select(TEMP_TABLE_COLS)
    )
    LOGGER.info(
        "[TIMER] Stage 8 (main temp table write) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EMNMADSC with hash-based predicates for parallelism
    stage9_start = time.time()
    section_start = time.time()
    LOGGER.info("Loading EMNMADSC table")
    predicates_emnmadsc = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    emnmadsc_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_id, nm_snum, addr_snum, prvdr_src_cd, "
            "app_odate FROM pndnh.EMNMADSC WHERE EXISTS (SELECT 1 FROM "
            f"{fein_filter_tmp} f WHERE f.empr_id = EMNMADSC.empr_id)) t"
        ),
        predicates=predicates_emnmadsc,
    )
    LOGGER.info(
        "[TIMER] EMNMADSC read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EMNMADTP with hash-based predicates for parallelism
    section_start = time.time()
    LOGGER.info("Loading EMNMADTP table")
    predicates_emnmadtp = [
        f"MOD(ABS(HASHTEXT(empr_id)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    emnmadtp_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_id, nm_snum, addr_snum, addr_typ_cd, "
            "app_odate FROM pndnh.EMNMADTP WHERE EXISTS (SELECT 1 FROM "
            f"{fein_filter_tmp} f WHERE f.empr_id = EMNMADTP.empr_id)) t"
        ),
        predicates=predicates_emnmadtp,
    )
    LOGGER.info(
        "[TIMER] EMNMADTP read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Read EMPRADDR (full projection) with hash-based predicates
    section_start = time.time()
    LOGGER.info(
        "Loading EMPRADDR table "
        "(full projection for pseudo-FEIN processing)"
    )
    predicates_empraddr_perm = [
        f"MOD(ABS(HASHTEXT(empr_idfr)), {num_jdbc_partitions}) = {i}"
        for i in range(num_jdbc_partitions)
    ]
    empraddr_perm_full = fpls.db.spark_read_jdbc(
        spark.read,
        (
            "(SELECT empr_idfr, empr_snum, empr_nm, "
            "empr_addrln40_1, empr_addrln40_2, empr_addrln40_3, "
            "empr_city25, empr_st, empr_zip5, empr_zip4, "
            "empr_frgn_cntry_cd, empr_frgn_cntry_nm, empr_frgnpzn, "
            "addrck_pmycd, addrck_scycd_1, addrck_scycd_2, app_odate "
            "FROM pndnh.EMPRADDR) t"
        ),
        predicates=predicates_empraddr_perm,
    )
    LOGGER.info(
        "[TIMER] EMPRADDR (full) read completed in "
        f"{time.time() - section_start:.2f}s"
    )

    emnmadsc_perm = emnmadsc_full.cache()
    emnmadsc_count = emnmadsc_perm.count()
    LOGGER.info(
        f"[FILTER] EMNMADSC loaded: {emnmadsc_count:,} records"
    )
    emnmadtp_perm = emnmadtp_full.cache()
    emnmadtp_count = emnmadtp_perm.count()
    LOGGER.info(
        f"[FILTER] EMNMADTP loaded: {emnmadtp_count:,} records"
    )
    empraddr_perm = empraddr_perm_full.join(
        broadcast(unique_empr_ein_df), "empr_idfr", "inner"
    ).cache()
    empraddr_perm_count = empraddr_perm.count()
    LOGGER.info(
        "[FILTER] EMPRADDR (full) filtered to "
        f"{empraddr_perm_count:,} records"
    )
    LOGGER.info(
        "[TIMER] Stage 9 (additional table loads + filtering) "
        f"completed in {time.time() - stage9_start:.2f}s"
    )

    # fein_filter_tmp is left populated after the job for troubleshooting.
    # It is truncated at the start of each run via
    # truncate=true/mode=overwrite.
    LOGGER.info(f"[FILTER] {fein_filter_tmp} retained for troubleshooting")

    empraddr_tmp_start = time.time()
    if pseudo_cnt > 0:
        empraddr_tmp_base = (
            pseudo_with_seq.select(
                col("empr_ein").alias("empr_idfr"),
                col("empr_snum"),
                col("qw_employer_name").alias("empr_nm"),
                col("addr1").alias("empr_addrln40_1"),
                col("addr2").alias("empr_addrln40_2"),
                col("addr3").alias("empr_addrln40_3"),
                col("city").alias("empr_city25"),
                col("state").alias("empr_st"),
                col("zip5").alias("empr_zip5"),
                col("zip4").alias("empr_zip4"),
                col("empr_forgn_cc").alias("empr_frgn_cntry_cd"),
                col("empr_forgn_c_name").alias("empr_frgn_cntry_nm"),
                col("empr_forgn_zip").alias("empr_frgnpzn"),
                when(
                    substring(col("fnl_empr_err_tab"), 1, 1) == "G", lit("GA")
                )
                .when(
                    substring(col("fnl_empr_err_tab"), 1, 1) == "C", lit("CH")
                )
                .otherwise(lit("BA"))
                .alias("addrck_pmycd"),
                lit("").alias("addrck_scycd_1"),
                lit("").alias("addrck_scycd_2"),
                to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
                current_timestamp().alias("insrt_ts"),
                current_timestamp().alias("lu_ts"),
            )
        )

        empraddr_tmp_base_norm = (
            empraddr_tmp_base
            .withColumn("_nm", upper(trim(col("empr_nm"))))
            .withColumn("_addr1", upper(trim(col("empr_addrln40_1"))))
            .withColumn("_addr2", upper(trim(col("empr_addrln40_2"))))
            .withColumn("_addr3", upper(trim(col("empr_addrln40_3"))))
            .withColumn("_city", upper(trim(col("empr_city25"))))
            .withColumn("_st", upper(trim(col("empr_st"))))
            .withColumn("_zip5", upper(trim(col("empr_zip5"))))
            .withColumn("_zip4", upper(trim(col("empr_zip4"))))
        )
        empraddr_perm_norm = (
            empraddr_perm
            .withColumn("_nm", upper(trim(col("empr_nm"))))
            .withColumn("_addr1", upper(trim(col("empr_addrln40_1"))))
            .withColumn("_addr2", upper(trim(col("empr_addrln40_2"))))
            .withColumn("_addr3", upper(trim(col("empr_addrln40_3"))))
            .withColumn("_city", upper(trim(col("empr_city25"))))
            .withColumn("_st", upper(trim(col("empr_st"))))
            .withColumn("_zip5", upper(trim(col("empr_zip5"))))
            .withColumn("_zip4", upper(trim(col("empr_zip4"))))
        )
        empraddr_tmp = (
            empraddr_tmp_base
            .join(empraddr_perm, ["empr_idfr", "empr_snum"], "left_anti")
            .withColumn("db_oper_typ", lit("I"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                "empr_idfr", "empr_snum", "empr_nm",
                "empr_addrln40_1", "empr_addrln40_2", "empr_addrln40_3",
                "empr_city25", "empr_st", "empr_zip5", "empr_zip4",
                "empr_frgn_cntry_cd", "empr_frgn_cntry_nm", "empr_frgnpzn",
                "addrck_pmycd", "addrck_scycd_1", "addrck_scycd_2",
                "app_odate", "insrt_ts", "lu_ts",
            )
            .unionByName(
                empraddr_tmp_base_norm.alias("src")
                .join(
                    empraddr_perm_norm.alias("perm"),
                    (col("src.empr_idfr") == col("perm.empr_idfr"))
                    & (col("src.empr_snum") == col("perm.empr_snum"))
                    & (col("src._nm") == col("perm._nm"))
                    & (col("src._addr1") == col("perm._addr1"))
                    & (col("src._addr2") == col("perm._addr2"))
                    & (col("src._addr3") == col("perm._addr3"))
                    & (col("src._city") == col("perm._city"))
                    & (col("src._st") == col("perm._st"))
                    & (col("src._zip5") == col("perm._zip5"))
                    & (col("src._zip4") == col("perm._zip4"))
                    & (
                        col("perm.app_odate")
                        != to_date(lit(odate), "yyyy-MM-dd")
                    ),
                    "inner",
                )
                .withColumn("db_oper_typ", lit("U"))
                .withColumn("db_sort_num", lit(0).cast("bigint"))
                .withColumn("db_uid", lit(0).cast("bigint"))
                .select(
                    "db_oper_typ", "db_sort_num", "db_uid",
                    col("src.empr_idfr").alias("empr_idfr"),
                    col("src.empr_snum").alias("empr_snum"),
                    col("src.empr_nm").alias("empr_nm"),
                    col("src.empr_addrln40_1").alias("empr_addrln40_1"),
                    col("src.empr_addrln40_2").alias("empr_addrln40_2"),
                    col("src.empr_addrln40_3").alias("empr_addrln40_3"),
                    col("src.empr_city25").alias("empr_city25"),
                    col("src.empr_st").alias("empr_st"),
                    col("src.empr_zip5").alias("empr_zip5"),
                    col("src.empr_zip4").alias("empr_zip4"),
                    col("src.empr_frgn_cntry_cd").alias(
                        "empr_frgn_cntry_cd"
                    ),
                    col("src.empr_frgn_cntry_nm").alias(
                        "empr_frgn_cntry_nm"
                    ),
                    col("src.empr_frgnpzn").alias("empr_frgnpzn"),
                    col("src.addrck_pmycd").alias("addrck_pmycd"),
                    col("src.addrck_scycd_1").alias("addrck_scycd_1"),
                    col("src.addrck_scycd_2").alias("addrck_scycd_2"),
                    col("src.app_odate").alias("app_odate"),
                    col("src.insrt_ts").alias("insrt_ts"),
                    col("src.lu_ts").alias("lu_ts"),
                )
            )
        )

        empraddr_tmp = empraddr_tmp.cache()
        overwrite_tmp_table(
            "pndnh.ndnh_qw_update_emp_table_job_empraddr_tmp", empraddr_tmp
        )
        LOGGER.info(
            "[TIMER] Stage 9 (EMPRADDR tmp prep + write) completed in "
            f"{time.time() - empraddr_tmp_start:.2f}s"
        )

    LOGGER.info(
        "[TIMER] Stage 6b (pseudo-FEIN seq + union prep) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    # Prepare all temp table DataFrames for INSERT
    LOGGER.info("[STAGE] Stage 10b starting: temp table DataFrame prep")
    section_start = time.time()
    emprnm_tmp_base = (
        final_df_to_write
        .filter(col("empr_name_pntr") > 0)
        .filter(col("empr_table_ind") != "O")
        .select(
            col("work_ein").alias("empr_id"),
            col("empr_name_pntr").alias("nm_snum"),
            col("qw_employer_name").alias("empr_nm"),
            to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
            current_timestamp().alias("insrt_ts"),
            current_timestamp().alias("lu_ts"),
            lit(None).cast("date").alias("fcr_mtch_dt"),
        )
    ).cache()

    emprnm_tmp = (
        emprnm_tmp_base
        .join(emprnm_df, ["empr_id", "nm_snum"], "left_anti")
        .withColumn("db_oper_typ", lit("I"))
        .withColumn("db_sort_num", lit(0).cast("bigint"))
        .withColumn("db_uid", lit(0).cast("bigint"))
        .select(
            "db_oper_typ", "db_sort_num", "db_uid",
            "empr_id", "nm_snum", "empr_nm",
            "app_odate", "insrt_ts", "lu_ts", "fcr_mtch_dt",
        )
        .unionByName(
            emprnm_tmp_base.alias("src")
            .join(
                emprnm_df.select(
                    "empr_id", "nm_snum", "app_odate").alias("perm"),
                ["empr_id", "nm_snum"],
                "inner",
            )
            .filter(
                col("perm.app_odate")
                != to_date(lit(odate), "yyyy-MM-dd")
            )
            .withColumn("db_oper_typ", lit("U"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                col("src.empr_id").alias("empr_id"),
                col("src.nm_snum").alias("nm_snum"),
                col("src.empr_nm").alias("empr_nm"),
                col("src.app_odate").alias("app_odate"),
                col("src.insrt_ts").alias("insrt_ts"),
                col("src.lu_ts").alias("lu_ts"),
                col("src.fcr_mtch_dt").alias("fcr_mtch_dt"),
            )
        )
    )
    eaddr_tmp_base = (
        final_df_to_write
        .filter(col("empr_addr_pntr") > 0)
        .filter(col("empr_table_ind") != "O")
        .select(
            col("work_ein").alias("empr_id"),
            col("empr_addr_pntr").alias("addr_snum"),
            col("addr1").alias("empr_addrln40_1"),
            col("addr2").alias("empr_addrln40_2"),
            col("addr3").alias("empr_addrln40_3"),
            col("city").alias("empr_city25"),
            col("state").alias("empr_st"),
            col("zip5").alias("empr_zip5"),
            col("zip4").alias("empr_zip4"),
            col("empr_forgn_cc").alias("empr_frgn_cntry_cd"),
            col("empr_forgn_c_name").alias("empr_frgn_cntry_nm"),
            col("empr_forgn_zip").alias("empr_frgnpzn"),
            when(substring(col("fnl_empr_err_tab"), 1, 1) == "G", lit("GA"))
            .when(substring(col("fnl_empr_err_tab"), 1, 1) == "C", lit("CH"))
            .otherwise(lit("BA"))
            .alias("addrck_pmycd"),
            lit("").alias("addrck_scycd_1"),
            lit("").alias("addrck_scycd_2"),
            to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
            current_timestamp().alias("insrt_ts"),
            current_timestamp().alias("lu_ts"),
        )
    ).cache()

    eaddr_tmp = (
        eaddr_tmp_base
        .join(eaddr_df, ["empr_id", "addr_snum"], "left_anti")
        .withColumn("db_oper_typ", lit("I"))
        .withColumn("db_sort_num", lit(0).cast("bigint"))
        .withColumn("db_uid", lit(0).cast("bigint"))
        .select(
            "db_oper_typ", "db_sort_num", "db_uid",
            "empr_id", "addr_snum",
            "empr_addrln40_1", "empr_addrln40_2",
            "empr_addrln40_3",
            "empr_city25", "empr_st", "empr_zip5", "empr_zip4",
            "empr_frgn_cntry_cd", "empr_frgn_cntry_nm",
            "empr_frgnpzn",
            "addrck_pmycd", "addrck_scycd_1", "addrck_scycd_2",
            "app_odate", "insrt_ts", "lu_ts",
        )
        .unionByName(
            eaddr_tmp_base.alias("src")
            .join(
                eaddr_df.alias("perm"),
                ["empr_id", "addr_snum"],
                "inner",
            )
            .filter(
                col("perm.app_odate")
                != to_date(lit(odate), "yyyy-MM-dd")
            )
            .withColumn("db_oper_typ", lit("U"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                col("src.empr_id").alias("empr_id"),
                col("src.addr_snum").alias("addr_snum"),
                col("src.empr_addrln40_1").alias("empr_addrln40_1"),
                col("src.empr_addrln40_2").alias("empr_addrln40_2"),
                col("src.empr_addrln40_3").alias("empr_addrln40_3"),
                col("src.empr_city25").alias("empr_city25"),
                col("src.empr_st").alias("empr_st"),
                col("src.empr_zip5").alias("empr_zip5"),
                col("src.empr_zip4").alias("empr_zip4"),
                col("src.empr_frgn_cntry_cd").alias("empr_frgn_cntry_cd"),
                col("src.empr_frgn_cntry_nm").alias("empr_frgn_cntry_nm"),
                col("src.empr_frgnpzn").alias("empr_frgnpzn"),
                col("src.addrck_pmycd").alias("addrck_pmycd"),
                col("src.addrck_scycd_1").alias("addrck_scycd_1"),
                col("src.addrck_scycd_2").alias("addrck_scycd_2"),
                col("src.app_odate").alias("app_odate"),
                col("src.insrt_ts").alias("insrt_ts"),
                col("src.lu_ts").alias("lu_ts"),
            )
        )
    )

    empnmadr_tmp_base = (
        final_df_to_write
        .filter(
            (col("empr_name_pntr") > 0)
            & (col("empr_addr_pntr") > 0)
        )
        .filter(col("empr_table_ind") != "O")
        .select(
            col("work_ein").alias("empr_id"),
            col("empr_addr_pntr").alias("addr_snum"),
            col("empr_name_pntr").alias("nm_snum"),
            to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
            current_timestamp().alias("insrt_ts"),
            current_timestamp().alias("lu_ts"),
            lit(None).cast("date").alias("endt"),
            lit(None).cast("date").alias("certd_stdt"),
        )
    ).cache()

    empnmadr_tmp = (
        empnmadr_tmp_base
        .join(
            empnmadr_perm,
            ["empr_id", "nm_snum", "addr_snum"],
            "left_anti",
        )
        .withColumn("db_oper_typ", lit("I"))
        .withColumn("db_sort_num", lit(0).cast("bigint"))
        .withColumn("db_uid", lit(0).cast("bigint"))
        .select(
            "db_oper_typ", "db_sort_num", "db_uid",
            "empr_id", "addr_snum", "nm_snum",
            "app_odate", "insrt_ts", "lu_ts",
            "endt", "certd_stdt",
        )
        .unionByName(
            empnmadr_tmp_base.alias("src")
            .join(
                empnmadr_perm.alias("perm"),
                ["empr_id", "nm_snum", "addr_snum"],
                "inner",
            )
            .filter(
                col("perm.app_odate")
                != to_date(lit(odate), "yyyy-MM-dd")
            )
            .withColumn("db_oper_typ", lit("U"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                col("src.empr_id").alias("empr_id"),
                col("src.addr_snum").alias("addr_snum"),
                col("src.nm_snum").alias("nm_snum"),
                col("src.app_odate").alias("app_odate"),
                col("src.insrt_ts").alias("insrt_ts"),
                col("src.lu_ts").alias("lu_ts"),
                col("src.endt").alias("endt"),
                col("src.certd_stdt").alias("certd_stdt"),
            )
        )
    )

    emnmadsc_tmp_base = (
        final_df_to_write
        .filter(
            (col("empr_name_pntr") > 0)
            & (col("empr_addr_pntr") > 0)
        )
        .filter(col("empr_table_ind") != "O")
        .select(
            col("work_ein").alias("empr_id"),
            col("empr_addr_pntr").alias("addr_snum"),
            col("empr_name_pntr").alias("nm_snum"),
            lit("QW").alias("prvdr_src_cd"),
            to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
            current_timestamp().alias("insrt_ts"),
            current_timestamp().alias("lu_ts"),
            lit(None).cast("date").alias("endt"),
        )
    ).cache()

    emnmadsc_tmp = (
        emnmadsc_tmp_base
        .join(
            emnmadsc_perm,
            ["empr_id", "nm_snum", "addr_snum", "prvdr_src_cd"],
            "left_anti",
        )
        .withColumn("db_oper_typ", lit("I"))
        .withColumn("db_sort_num", lit(0).cast("bigint"))
        .withColumn("db_uid", lit(0).cast("bigint"))
        .select(
            "db_oper_typ", "db_sort_num", "db_uid",
            "empr_id", "addr_snum", "nm_snum", "prvdr_src_cd",
            "app_odate", "insrt_ts", "lu_ts", "endt",
        )
        .unionByName(
            emnmadsc_tmp_base.alias("src")
            .join(
                emnmadsc_perm.alias("perm"),
                ["empr_id", "nm_snum", "addr_snum", "prvdr_src_cd"],
                "inner",
            )
            .filter(
                col("perm.app_odate")
                != to_date(lit(odate), "yyyy-MM-dd")
            )
            .withColumn("db_oper_typ", lit("U"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                col("src.empr_id").alias("empr_id"),
                col("src.addr_snum").alias("addr_snum"),
                col("src.nm_snum").alias("nm_snum"),
                col("src.prvdr_src_cd").alias("prvdr_src_cd"),
                col("src.app_odate").alias("app_odate"),
                col("src.insrt_ts").alias("insrt_ts"),
                col("src.lu_ts").alias("lu_ts"),
                col("src.endt").alias("endt"),
            )
        )
    )

    emnmadtp_tmp_base = (
        final_df_to_write
        .filter(
            (col("empr_name_pntr") > 0)
            & (col("empr_addr_pntr") > 0)
        )
        .filter(col("empr_table_ind") != "O")
        .select(
            col("work_ein").alias("empr_id"),
            col("empr_addr_pntr").alias("addr_snum"),
            col("empr_name_pntr").alias("nm_snum"),
            lit("PR").alias("addr_typ_cd"),
            to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
            current_timestamp().alias("insrt_ts"),
            current_timestamp().alias("lu_ts"),
            lit(None).cast("date").alias("endt"),
        )
    ).cache()

    emnmadtp_tmp = (
        emnmadtp_tmp_base
        .join(
            emnmadtp_perm,
            ["empr_id", "nm_snum", "addr_snum", "addr_typ_cd"],
            "left_anti",
        )
        .withColumn("db_oper_typ", lit("I"))
        .withColumn("db_sort_num", lit(0).cast("bigint"))
        .withColumn("db_uid", lit(0).cast("bigint"))
        .select(
            "db_oper_typ", "db_sort_num", "db_uid",
            "empr_id", "addr_snum", "nm_snum", "addr_typ_cd",
            "app_odate", "insrt_ts", "lu_ts", "endt",
        )
        .unionByName(
            emnmadtp_tmp_base.alias("src")
            .join(
                emnmadtp_perm.alias("perm"),
                ["empr_id", "nm_snum", "addr_snum", "addr_typ_cd"],
                "inner",
            )
            .filter(
                col("perm.app_odate")
                != to_date(lit(odate), "yyyy-MM-dd")
            )
            .withColumn("db_oper_typ", lit("U"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                col("src.empr_id").alias("empr_id"),
                col("src.addr_snum").alias("addr_snum"),
                col("src.nm_snum").alias("nm_snum"),
                col("src.addr_typ_cd").alias("addr_typ_cd"),
                col("src.app_odate").alias("app_odate"),
                col("src.insrt_ts").alias("insrt_ts"),
                col("src.lu_ts").alias("lu_ts"),
                col("src.endt").alias("endt"),
            )
        )
    )

    LOGGER.info(
        "[TIMER] Stage 10b (temp table DataFrame prep) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    section_start = time.time()
    LOGGER.info("[STAGE 10] Writing temp tables")
    LOGGER.info(
        "[STAGE 10] Writing emprnm_tmp "
        "(I=inserts, U=update candidates)"
    )
    overwrite_tmp_table(
        "pndnh.ndnh_qw_update_emp_table_job_emprnm_tmp", emprnm_tmp
    )
    LOGGER.info("[STAGE 10] emprnm_tmp write complete")
    emprnm_tmp.unpersist()
    emprnm_tmp_base.unpersist()
    LOGGER.info(
        "[STAGE 10] Writing eaddr_tmp "
        "(I=inserts, U=update candidates)"
    )
    overwrite_tmp_table("pndnh.ndnh_qw_update_emp_table_job_eaddr_tmp",
                        eaddr_tmp)
    LOGGER.info("[STAGE 10] eaddr_tmp write complete")
    eaddr_tmp.unpersist()
    eaddr_tmp_base.unpersist()
    LOGGER.info(
        "[STAGE 10] Writing empnmadr_tmp "
        "(I=inserts, U=update candidates)"
    )
    overwrite_tmp_table(
        "pndnh.ndnh_qw_update_emp_table_job_empnmadr_tmp", empnmadr_tmp
    )
    LOGGER.info("[STAGE 10] empnmadr_tmp write complete")
    empnmadr_tmp.unpersist()
    empnmadr_tmp_base.unpersist()
    LOGGER.info(
        "[STAGE 10] Writing emnmadsc_tmp "
        "(I=inserts, U=update candidates)"
    )
    overwrite_tmp_table(
        "pndnh.ndnh_qw_update_emp_table_job_emnmadsc_tmp", emnmadsc_tmp
    )
    LOGGER.info("[STAGE 10] emnmadsc_tmp write complete")
    emnmadsc_tmp.unpersist()
    emnmadsc_tmp_base.unpersist()
    LOGGER.info(
        "[STAGE 10] Writing emnmadtp_tmp "
        "(I=inserts, U=update candidates)"
    )
    overwrite_tmp_table(
        "pndnh.ndnh_qw_update_emp_table_job_emnmadtp_tmp", emnmadtp_tmp
    )
    LOGGER.info("[STAGE 10] emnmadtp_tmp write complete")
    emnmadtp_tmp.unpersist()
    emnmadtp_tmp_base.unpersist()
    LOGGER.info("[STAGE 10] All temp tables written")
    LOGGER.info(
        "[TIMER] Stage 10 (temp table writes) completed in "
        f"{time.time() - section_start:.2f}s"
    )

    final_df_to_write.unpersist()

    LOGGER.info("=" * 60)
    LOGGER.info("JOB PROCESSING SUMMARY")
    LOGGER.info("=" * 60)
    LOGGER.info(f"Total records read: {total_cnt}")
    LOGGER.info(f"Valid records: {valid_cnt}")
    LOGGER.info(f"Invalid records: {invalid_cnt}")
    LOGGER.info(f"Unknown address records: {unk_cnt}")
    LOGGER.info(f"Known address records: {knwn_cnt}")
    LOGGER.info(f"Pseudo-FEIN records: {pseudo_cnt}")
    LOGGER.info(f"Non-pseudo-FEIN records: {non_pseudo_cnt}")
    LOGGER.info("=" * 60)

    # Phase 1c: Build optional EADDR tmp rows and write to tmp table.
    # Done before with connection: to keep Spark work outside transaction.
    section_start = time.time()
    LOGGER.info("Phase 1c: Preparing optional address tmp rows")
    eaddr_opt_cnt = 0
    # Cache the filtered subset rather than holding all of final_df_to_write
    # in memory through Phase 1c. final_df_with_opt is a small fraction of
    # records (only those with a non-blank opt_addr1 and empr_opt_addr_pntr>0)
    # so its cache footprint is much smaller than the full DataFrame.
    final_df_with_opt = final_df_to_write.filter(
        (trim(col("opt_addr1")) != "")
        & (col("empr_opt_addr_pntr") > 0)
        & (col("empr_table_ind") != "O")
    ).cache()
    if final_df_with_opt.limit(1).count() > 0:
        eaddr_opt = (
            final_df_with_opt.select(
                col("work_ein").alias("empr_id"),
                col("empr_opt_addr_pntr").alias("addr_snum"),
                col("opt_addr1").alias("empr_addrln40_1"),
                col("opt_addr2").alias("empr_addrln40_2"),
                col("opt_addr3").alias("empr_addrln40_3"),
                col("opt_city").alias("empr_city25"),
                col("opt_state").alias("empr_st"),
                col("opt_zip5").alias("empr_zip5"),
                col("opt_zip4").alias("empr_zip4"),
                col("opt_empr_forgn_cc").alias("empr_frgn_cntry_cd"),
                col("opt_empr_forgn_c_name").alias("empr_frgn_cntry_nm"),
                col("opt_empr_forgn_zip").alias("empr_frgnpzn"),
                when(
                    substring(col("fnl_empr_err_tab"), 1, 1) == "G", lit("GA")
                )
                .when(
                    substring(col("fnl_empr_err_tab"), 1, 1) == "C", lit("CH")
                )
                .otherwise(lit("BA"))
                .alias("addrck_pmycd"),
                lit("").alias("addrck_scycd_1"),
                lit("").alias("addrck_scycd_2"),
                to_date(lit(odate), "yyyy-MM-dd").alias("app_odate"),
                current_timestamp().alias("insrt_ts"),
                current_timestamp().alias("lu_ts"),
            )
            .join(eaddr_df, ["empr_id", "addr_snum"], "left_anti")
            .withColumn("db_oper_typ", lit("I"))
            .withColumn("db_sort_num", lit(0).cast("bigint"))
            .withColumn("db_uid", lit(0).cast("bigint"))
            .select(
                "db_oper_typ", "db_sort_num", "db_uid",
                "empr_id", "addr_snum",
                "empr_addrln40_1", "empr_addrln40_2", "empr_addrln40_3",
                "empr_city25", "empr_st", "empr_zip5", "empr_zip4",
                "empr_frgn_cntry_cd", "empr_frgn_cntry_nm", "empr_frgnpzn",
                "addrck_pmycd", "addrck_scycd_1", "addrck_scycd_2",
                "app_odate", "insrt_ts", "lu_ts",
            )
        )
        eaddr_opt_cnt = eaddr_opt.count()
        if eaddr_opt_cnt > 0:
            eaddr_opt_write_start = time.time()
            append_tmp_table(
                "pndnh.ndnh_qw_update_emp_table_job_eaddr_tmp", eaddr_opt
            )
            LOGGER.info(
                "[PHASE 1c] eaddr_opt tmp write complete "
                f"({eaddr_opt_cnt} rows) in "
                f"{time.time() - eaddr_opt_write_start:.2f}s"
            )
    final_df_with_opt.unpersist()
    if pseudo_cnt > 0:
        empraddr_tmp.unpersist()

    def execute_sql(sql, log_msg):
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                LOGGER.info(f"{log_msg}: {rows_affected} rows")
                return rows_affected
            except Exception as e:
                LOGGER.error(f"{log_msg} FAILED: {str(e)}")
                raise

    # Phase 1a through 2b run in a single transaction.
    # with connection: commits on success, rolls back on exception.
    section_start = time.time()
    with connection:
        upd_empraddr = 0
        upd_emprnm = 0
        upd_eaddr = 0
        LOGGER.info("Phase 1a: Updating base tables")
        if pseudo_cnt > 0:
            LOGGER.info("[UPDATE] EMPRADDR: starting")
            upd_empraddr = execute_sql(
                (
                    "UPDATE pndnh.EMPRADDR p "
                    f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                    "lu_ts = CURRENT_TIMESTAMP "
                    "FROM pndnh.ndnh_qw_update_emp_table_job"
                    "_empraddr_tmp t "
                    "WHERE t.db_oper_typ = 'U' "
                    "AND p.empr_idfr = t.empr_idfr "
                    "AND p.empr_snum = t.empr_snum"
                ),
                "[UPDATE] EMPRADDR",
            )
            LOGGER.info(
                f"[UPDATE] EMPRADDR: {upd_empraddr:,} rows updated"
            )
        LOGGER.info("[UPDATE] EMPRNM: starting")
        upd_emprnm = execute_sql(
            (
                "UPDATE pndnh.EMPRNM p "
                f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                "lu_ts = CURRENT_TIMESTAMP "
                "FROM pndnh.ndnh_qw_update_emp_table_job_emprnm_tmp t "
                "WHERE t.db_oper_typ = 'U' "
                "AND p.empr_id = t.empr_id "
                "AND p.nm_snum = t.nm_snum"
            ),
            "[UPDATE] EMPRNM",
        )
        LOGGER.info("[UPDATE] EADDR: starting")
        upd_eaddr = execute_sql(
            (
                "UPDATE pndnh.EADDR p "
                f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                "lu_ts = CURRENT_TIMESTAMP "
                "FROM pndnh.ndnh_qw_update_emp_table_job_eaddr_tmp t "
                "WHERE t.db_oper_typ = 'U' "
                "AND p.empr_id = t.empr_id "
                "AND p.addr_snum = t.addr_snum"
            ),
            "[UPDATE] EADDR",
        )
        LOGGER.info(
            "[TIMER] Phase 1a (base table updates) completed in "
            f"{time.time() - section_start:.2f} seconds"
        )

        section_start = time.time()
        ins_empraddr = 0
        ins_eaddr_opt = 0
        LOGGER.info("Phase 1b: Inserting into base tables")
        if pseudo_cnt > 0:
            LOGGER.info("[INSERT] EMPRADDR: starting")
            ins_empraddr = execute_sql(
                (
                    "INSERT INTO pndnh.EMPRADDR ("
                    "empr_idfr, empr_snum, empr_nm, "
                    "empr_addrln40_1, empr_addrln40_2, "
                    "empr_addrln40_3, empr_city25, empr_st, "
                    "empr_zip5, empr_zip4, empr_frgn_cntry_cd, "
                    "empr_frgn_cntry_nm, empr_frgnpzn, "
                    "addrck_pmycd, addrck_scycd_1, "
                    "addrck_scycd_2, app_odate) "
                    "SELECT DISTINCT t.empr_idfr, t.empr_snum, "
                    "t.empr_nm, t.empr_addrln40_1, "
                    "t.empr_addrln40_2, t.empr_addrln40_3, "
                    "t.empr_city25, t.empr_st, t.empr_zip5, "
                    "t.empr_zip4, t.empr_frgn_cntry_cd, "
                    "t.empr_frgn_cntry_nm, t.empr_frgnpzn, "
                    "t.addrck_pmycd, t.addrck_scycd_1, "
                    "t.addrck_scycd_2, "
                    f"TO_DATE('{odate}', 'YYYY-MM-DD') "
                    "FROM pndnh.ndnh_qw_update_emp_table_job_"
                    "empraddr_tmp t "
                    "WHERE t.db_oper_typ = 'I' "
                    "ON CONFLICT (empr_idfr, empr_snum) DO NOTHING"
                ),
                "[INSERT] EMPRADDR",
            )
            LOGGER.info(
                f"[INSERT] EMPRADDR: {ins_empraddr:,} rows inserted"
            )
        LOGGER.info("[INSERT] EMPRNM: starting")
        ins_emprnm = execute_sql(
            (
                "INSERT INTO pndnh.EMPRNM ("
                "empr_id, empr_nm, nm_snum, app_odate) "
                "SELECT DISTINCT t.empr_id, t.empr_nm, "
                "t.nm_snum, t.app_odate "
                "FROM pndnh.ndnh_qw_update_emp_table_job_emprnm_tmp t "
                "WHERE t.db_oper_typ = 'I' "
                "ON CONFLICT (empr_id, nm_snum) DO NOTHING"
            ),
            "[INSERT] EMPRNM",
        )
        LOGGER.info(f"[INSERT] EMPRNM: {ins_emprnm:,} rows inserted")
        LOGGER.info("[INSERT] EADDR: starting")
        ins_eaddr = execute_sql(
            (
                "INSERT INTO pndnh.EADDR ("
                "empr_id, addr_snum, empr_addrln40_1, "
                "empr_addrln40_2, empr_addrln40_3, empr_city25, "
                "empr_st, empr_zip5, empr_zip4, "
                "empr_frgn_cntry_cd, empr_frgn_cntry_nm, "
                "empr_frgnpzn, addrck_pmycd, addrck_scycd_1, "
                "addrck_scycd_2, app_odate) "
                "SELECT DISTINCT t.empr_id, t.addr_snum, "
                "t.empr_addrln40_1, t.empr_addrln40_2, "
                "t.empr_addrln40_3, t.empr_city25, t.empr_st, "
                "t.empr_zip5, t.empr_zip4, t.empr_frgn_cntry_cd, "
                "t.empr_frgn_cntry_nm, t.empr_frgnpzn, "
                "t.addrck_pmycd, t.addrck_scycd_1, "
                "t.addrck_scycd_2, t.app_odate "
                "FROM pndnh.ndnh_qw_update_emp_table_job_eaddr_tmp t "
                "WHERE t.db_oper_typ = 'I' "
                "ON CONFLICT (empr_id, addr_snum) DO NOTHING"
            ),
            "[INSERT] EADDR",
        )
        LOGGER.info(f"[INSERT] EADDR: {ins_eaddr:,} rows inserted")

        LOGGER.info(
            "[TIMER] Phase 1b (base table inserts) completed in "
            f"{time.time() - section_start:.2f} seconds"
        )

        # Phase 1c: INSERT optional EADDR rows (tmp table written before
        # with connection: block)
        section_start = time.time()
        LOGGER.info("Phase 1c: Inserting optional addresses")
        if eaddr_opt_cnt > 0:
            ins_eaddr_opt = execute_sql(
                (
                    "INSERT INTO pndnh.EADDR ("
                    "empr_id, addr_snum, empr_addrln40_1, empr_addrln40_2, "
                    "empr_addrln40_3, empr_city25, empr_st, empr_zip5, "
                    "empr_zip4, empr_frgn_cntry_cd, "
                    "empr_frgn_cntry_nm, empr_frgnpzn, addrck_pmycd,"
                    "addrck_scycd_1, addrck_scycd_2, app_odate) "
                    "SELECT DISTINCT t.empr_id, t.addr_snum, "
                    "t.empr_addrln40_1, t.empr_addrln40_2, "
                    "t.empr_addrln40_3, t.empr_city25, t.empr_st,"
                    "t.empr_zip5, t.empr_zip4, "
                    "t.empr_frgn_cntry_cd, t.empr_frgn_cntry_nm, "
                    "t.empr_frgnpzn, t.addrck_pmycd, "
                    "t.addrck_scycd_1, t.addrck_scycd_2, "
                    "t.app_odate "
                    "FROM pndnh.ndnh_qw_update_emp_table_job_eaddr_tmp t "
                    "WHERE t.db_oper_typ = 'I' AND NOT EXISTS "
                    "(SELECT 1 FROM pndnh.EADDR e "
                    "WHERE e.empr_id = t.empr_id "
                    "AND e.addr_snum = t.addr_snum) "
                    "ON CONFLICT (empr_id, addr_snum) DO NOTHING"
                ),
                "[INSERT] EADDR optional",
            )
            LOGGER.info(
                "[INSERT] EADDR optional: "
                f"{ins_eaddr_opt:,} rows inserted"
            )

        # Phase 2a: UPDATE for relationship tables
        section_start = time.time()
        upd_empnmadr = 0
        upd_emnmadsc = 0
        upd_emnmadtp = 0
        LOGGER.info("Phase 2a: Updating relationship tables")
        LOGGER.info("[UPDATE] EMPNMADR: starting")
        upd_empnmadr = execute_sql(
            (
                "UPDATE pndnh.EMPNMADR p "
                f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                "lu_ts = CURRENT_TIMESTAMP "
                "FROM pndnh.ndnh_qw_update_emp_table_job"
                "_empnmadr_tmp t "
                "WHERE t.db_oper_typ = 'U' "
                "AND p.empr_id = t.empr_id "
                "AND p.nm_snum = t.nm_snum "
                "AND p.addr_snum = t.addr_snum"
            ),
            "[UPDATE] EMPNMADR",
        )
        LOGGER.info("[UPDATE] EMNMADSC: starting")
        upd_emnmadsc = execute_sql(
            (
                "UPDATE pndnh.EMNMADSC p "
                f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                "lu_ts = CURRENT_TIMESTAMP "
                "FROM pndnh.ndnh_qw_update_emp_table_job"
                "_emnmadsc_tmp t "
                "WHERE t.db_oper_typ = 'U' "
                "AND p.empr_id = t.empr_id "
                "AND p.nm_snum = t.nm_snum "
                "AND p.addr_snum = t.addr_snum "
                "AND p.prvdr_src_cd = t.prvdr_src_cd"
            ),
            "[UPDATE] EMNMADSC",
        )
        LOGGER.info("[UPDATE] EMNMADTP: starting")
        upd_emnmadtp = execute_sql(
            (
                "UPDATE pndnh.EMNMADTP p "
                f"SET app_odate = TO_DATE('{odate}', 'YYYY-MM-DD'), "
                "lu_ts = CURRENT_TIMESTAMP "
                "FROM pndnh.ndnh_qw_update_emp_table_job"
                "_emnmadtp_tmp t "
                "WHERE t.db_oper_typ = 'U' "
                "AND p.empr_id = t.empr_id "
                "AND p.nm_snum = t.nm_snum "
                "AND p.addr_snum = t.addr_snum "
                "AND p.addr_typ_cd = t.addr_typ_cd"
            ),
            "[UPDATE] EMNMADTP",
        )

        LOGGER.info(
            "[TIMER] Phase 2a (relationship table updates) "
            f"completed in {time.time() - section_start:.2f} seconds"
        )

        # Phase 2b: INSERT into relationship tables
        section_start = time.time()
        LOGGER.info("Phase 2b: Inserting into relationship tables")
        LOGGER.info("[INSERT] EMPNMADR: starting")
        ins_empnmadr = execute_sql(
            (
                "INSERT INTO pndnh.EMPNMADR "
                "(empr_id, nm_snum, addr_snum, app_odate) "
                "SELECT DISTINCT t.empr_id, t.nm_snum, t.addr_snum, "
                "t.app_odate "
                "FROM pndnh.ndnh_qw_update_emp_table_job_empnmadr_tmp t "
                "WHERE t.db_oper_typ = 'I' "
                "ON CONFLICT (empr_id, nm_snum, addr_snum) DO NOTHING"
            ),
            "[INSERT] EMPNMADR",
        )
        LOGGER.info(f"[INSERT] EMPNMADR: {ins_empnmadr:,} rows inserted")
        LOGGER.info("[INSERT] EMNMADSC: starting")
        ins_emnmadsc = execute_sql(
            (
                "INSERT INTO pndnh.EMNMADSC "
                "(empr_id, nm_snum, addr_snum, prvdr_src_cd, app_odate) "
                "SELECT DISTINCT t.empr_id, t.nm_snum, t.addr_snum, "
                "t.prvdr_src_cd, t.app_odate "
                "FROM pndnh.ndnh_qw_update_emp_table_job_emnmadsc_tmp t "
                "WHERE t.db_oper_typ = 'I' "
                "ON CONFLICT "
                "(empr_id, nm_snum, addr_snum, prvdr_src_cd) "
                "DO NOTHING"
            ),
            "[INSERT] EMNMADSC",
        )
        LOGGER.info(f"[INSERT] EMNMADSC: {ins_emnmadsc:,} rows inserted")
        LOGGER.info("[INSERT] EMNMADTP: starting")
        ins_emnmadtp = execute_sql(
            (
                "INSERT INTO pndnh.EMNMADTP "
                "(empr_id, nm_snum, addr_snum, addr_typ_cd, app_odate) "
                "SELECT DISTINCT t.empr_id, t.nm_snum, t.addr_snum, "
                "t.addr_typ_cd, t.app_odate "
                "FROM pndnh.ndnh_qw_update_emp_table_job_emnmadtp_tmp t "
                "WHERE t.db_oper_typ = 'I' "
                "ON CONFLICT "
                "(empr_id, nm_snum, addr_snum, addr_typ_cd) "
                "DO NOTHING"
            ),
            "[INSERT] EMNMADTP",
        )
        LOGGER.info(f"[INSERT] EMNMADTP: {ins_emnmadtp:,} rows inserted")
        LOGGER.info(
            "[TIMER] Stage 11 (DB UPDATE/INSERT phases) completed in "
            f"{time.time() - section_start:.2f}s"
        )

    LOGGER.info("All permanent tables committed")

    LOGGER.info("=" * 60)
    LOGGER.info("DB WRITE SUMMARY")
    LOGGER.info("=" * 60)
    LOGGER.info(
        f"[DB SUMMARY] UPDATEs: EMPRADDR={upd_empraddr:,}, "
        f"EMPRNM={upd_emprnm:,}, EADDR={upd_eaddr:,}, "
        f"EMPNMADR={upd_empnmadr:,}, EMNMADSC={upd_emnmadsc:,}, "
        f"EMNMADTP={upd_emnmadtp:,}"
    )
    LOGGER.info(
        f"[DB SUMMARY] INSERTs: EMPRADDR={ins_empraddr:,}, "
        f"EMPRNM={ins_emprnm:,}, EADDR={ins_eaddr:,}, "
        f"EADDR optional={ins_eaddr_opt:,}, "
        f"EMPNMADR={ins_empnmadr:,}, EMNMADSC={ins_emnmadsc:,}, "
        f"EMNMADTP={ins_emnmadtp:,}"
    )
    LOGGER.info("=" * 60)

    section_start = time.time()

    # Promote parquet from staging to final output path.
    # DB is already committed. If this fails, DB state is correct
    # but Job 4 won't see the file; orphan in staging is cleaned
    # next run. Failure here logs but does not re-raise.
    try:
        s3_client = boto3.client("s3")
        s3_bucket = boto3.resource("s3").Bucket(bucket_name)
        staging_prefix = f"{job_name}/output_staging/{odate_yymmdd}/"
        final_prefix = f"{job_name}/output/"
        staging_objects = list(s3_bucket.objects.filter(Prefix=staging_prefix))
        if not staging_objects:
            raise Exception(
                f"No files found in staging directory: {staging_path}"
            )
        staging_files = [
            obj.key for obj in staging_objects
            if obj.key.endswith(".parquet")
        ]
        if not staging_files:
            raise Exception(
                f"No parquet files found in staging directory: {staging_path}"
            )
        LOGGER.info(
            f"Found {len(staging_files)} parquet files in staging"
        )

        match_full = re.search(
            r"(\.R\d{6}\.T\d{6})$",
            output_filename.replace(".parquet", "")
        )
        match_date = re.search(
            r"(\.R\d{6})$",
            output_filename.replace(".parquet", "")
        )
        if match_full:
            timestamp_part = match_full.group(1)
            dest_dir_name = (
                "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
                f"{timestamp_part}/"
            )
        elif match_date:
            raise Exception(
                "output_filename has date but no time component: "
                f"{output_filename}. Cannot build destination directory."
            )
        else:
            raise Exception(
                "Cannot derive destination directory from "
                f"output_filename: {output_filename}"
            )

        final_dest_prefix = f"{final_prefix}{dest_dir_name}"
        LOGGER.info(
            f"Destination directory: s3://{bucket_name}/{final_dest_prefix}"
        )

        transfer_config = TransferConfig(
            multipart_threshold=5 * 1024 * 1024 * 1024,
            multipart_chunksize=100 * 1024 * 1024,
            use_threads=True,
        )

        copied_files = []
        failed_files = []

        for staging_file_key in staging_files:
            try:
                file_name = staging_file_key.split("/")[-1]
                dest_key = f"{final_dest_prefix}{file_name}"
                copy_source = {
                    "Bucket": bucket_name,
                    "Key": staging_file_key,
                }
                s3_client.copy(
                    copy_source,
                    bucket_name,
                    dest_key,
                    Config=transfer_config,
                )
                copied_files.append(dest_key)
                LOGGER.info(f"Copied: {staging_file_key} -> {dest_key}")
            except Exception as file_error:
                failed_files.append((staging_file_key, str(file_error)))
                LOGGER.error(
                    f"Failed to copy {staging_file_key}: {file_error}"
                )

        if failed_files:
            LOGGER.warning(
                f"Promotion partially succeeded: {len(copied_files)} "
                f"files copied, {len(failed_files)} files failed"
            )
            for failed_file, error in failed_files:
                LOGGER.error(f"  Failed: {failed_file} - {error}")
            LOGGER.error(
                "NOT cleaning up staging due to partial failure. "
                f"Manual intervention needed at {staging_path}"
            )
        else:
            LOGGER.info(
                f"All {len(copied_files)} files promoted successfully"
            )

        LOGGER.info(
            f"Output directory: s3://{bucket_name}/{final_dest_prefix}"
        )
        LOGGER.info(
            "[TIMER] Stage 12 (staging->final promotion) completed "
            f"in {time.time() - section_start:.2f}s"
        )
    except Exception as e:
        LOGGER.error(
            "Parquet promotion from staging to final FAILED "
            f"after DB commit: {e}. DB state is correct. Staging "
            f"file at {staging_path} must be manually promoted "
            "or next run will orphan it."
        )

    fpls.notification(
        f"Job '{job_name}' completed. Processed {valid_cnt} records. "
        f"Output: {output_path}{output_filename}"
    ).subject(f"Job '{job_name}' - Success").send_job_success()

    LOGGER.info(
        "[TIMER] TOTAL job runtime: "
        f"{time.time() - job_start_ts:.2f}s"
    )
    job.commit()


def main():
    with Fpls("ndnh") as fpls:
        try:
            connection = fpls.db.get_connection()
            connection.autocommit = False
            run_job(fpls, connection)
        except Exception as e:
            LOGGER.error("Job failed", exc_info=e)
            sys.exit(1)
        finally:
            fpls.db.release_connection(connection)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOGGER.error("Fatal error", exc_info=e)
        sys.exit(1)
