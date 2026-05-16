"""Pure-Python helpers extracted from glue_job.py for unit testing.

These functions intentionally have no Spark / AWS / Glue dependencies so
they can be exercised in plain pytest. The Glue job imports from this
module; the module is shipped to the cluster via the job's
``--extra-py-files`` argument.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

FIXED_OUTPUT_PREFIX = "NDNHI.FPLS.NDNH.QWDATA.EVS.RESP"

_FULL_TIMESTAMP_RE = re.compile(r"(\.R\d{6}\.T\d{6})$")
_DATE_ONLY_RE = re.compile(r"(\.R\d{6})$")


def generate_output_filename(
    input_filename: str,
    now: Optional[datetime] = None,
) -> str:
    """Build the SEQID parquet output filename from an input filename.

    Preserves a trailing ``.Ryymmdd.Thhmmss`` block when present; falls
    back to date-only with the current time appended; otherwise stamps
    the current time. ``now`` is injectable so tests don't depend on
    the wall clock.
    """
    if now is None:
        now = datetime.now()

    base_name = (
        input_filename.rsplit(".", 1)[0]
        if "." in input_filename
        else input_filename
    )

    full = _FULL_TIMESTAMP_RE.search(base_name)
    if full:
        return f"{FIXED_OUTPUT_PREFIX}.SEQIDS{full.group(1)}.parquet"

    date_only = _DATE_ONLY_RE.search(base_name)
    if date_only:
        time_part = now.strftime(".T%H%M%S")
        return (
            f"{FIXED_OUTPUT_PREFIX}.SEQIDS"
            f"{date_only.group(1)}{time_part}.parquet"
        )

    stamp = now.strftime(".R%y%m%d.T%H%M%S")
    return f"{FIXED_OUTPUT_PREFIX}.SEQIDS{stamp}.parquet"
