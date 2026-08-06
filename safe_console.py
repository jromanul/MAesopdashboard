"""
Make script output survive a Windows console or a redirected stdout.

On Windows, Python encodes stdout with the legacy ANSI code page (cp1252) unless
told otherwise. Printing any character outside that set — a check mark, an em
dash, a curly quote, an accented sponsor name straight out of a DOL filing —
raises UnicodeEncodeError and kills the script.

That is not cosmetic. In these refresh scripts the crash lands *between* the work
and the reporting: scan_dol_filers.py printed its findings, then died on the
"all rows match" check mark before it wrote dol_scan_<year>_report.csv and before
it reached the --import-new block. The run looked like it had done something and
had in fact imported nothing.

Call enable_utf8_stdout() at the top of any script meant to be run from a
terminal. It is a no-op on platforms that already use UTF-8.
"""
from __future__ import annotations

import sys


def enable_utf8_stdout() -> None:
    """Force UTF-8 on stdout/stderr, replacing anything unencodable."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            # Detached or already-wrapped stream: printing plain ASCII still works.
            pass
