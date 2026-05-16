from datetime import datetime

import pytest

from qw_helpers import generate_output_filename


FIXED_NOW = datetime(2026, 5, 16, 14, 7, 23)


def test_preserves_full_timestamp_block():
    result = generate_output_filename(
        "EVS.RESP.R260516.T140723.dat", now=FIXED_NOW
    )
    assert result == (
        "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
        ".R260516.T140723.parquet"
    )


def test_appends_current_time_when_only_date_present():
    result = generate_output_filename(
        "EVS.RESP.R260516.dat", now=FIXED_NOW
    )
    assert result == (
        "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
        ".R260516.T140723.parquet"
    )


def test_stamps_now_when_no_pattern_matches():
    result = generate_output_filename("arbitrary_input.dat", now=FIXED_NOW)
    assert result == (
        "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
        ".R260516.T140723.parquet"
    )


def test_filename_with_no_extension_still_works():
    result = generate_output_filename("EVS_RESP_NOEXT", now=FIXED_NOW)
    assert result == (
        "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
        ".R260516.T140723.parquet"
    )


def test_pattern_in_middle_is_ignored():
    # The regex anchors at end-of-base-name, so a mid-string Rxxxxxx
    # should NOT be treated as a timestamp block.
    result = generate_output_filename(
        "EVS.R260516.RESP.txt", now=FIXED_NOW
    )
    assert result == (
        "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS"
        ".R260516.T140723.parquet"
    )


def test_uses_wall_clock_when_now_not_provided():
    result = generate_output_filename("arbitrary.dat")
    assert result.startswith("NDNHI.FPLS.NDNH.QWDATA.EVS.RESP.SEQIDS.R")
    assert result.endswith(".parquet")


@pytest.mark.parametrize(
    "filename",
    [
        "a.R991231.T235959.bin",
        "a.R000101.T000000.bin",
    ],
)
def test_boundary_timestamps_preserved(filename):
    out = generate_output_filename(filename, now=FIXED_NOW)
    assert ".SEQIDS." in out and out.endswith(".parquet")
