import re
from typing import Optional


_NON_ALNUM_UNDERSCORE = re.compile(r"[^a-z0-9_]+")
_UNDERSCORE_RUN = re.compile(r"_+")


def normalize_identifier(raw: Optional[object], *, prefix_if_digit: str = "tbl") -> str:
    """
    Normalize a user-provided identifier into a safe snake_case token.

    - lowercase
    - replace '%' with 'pct'
    - replace non [a-z0-9_] with '_'
    - collapse '_' runs and trim edges
    - prefix if it starts with a digit

    This is used for table names / layer names / store names that must satisfy
    the styles validation regex: ^[a-zA-Z0-9_]+$
    """
    s = str(raw or "").strip().lower()
    s = s.replace("%", " pct ")
    s = _NON_ALNUM_UNDERSCORE.sub("_", s)
    s = _UNDERSCORE_RUN.sub("_", s).strip("_")
    if not s:
        s = prefix_if_digit
    if s[0].isdigit():
        s = f"{prefix_if_digit}_{s}"
    return s

