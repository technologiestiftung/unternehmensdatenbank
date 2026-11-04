"""`scripts/check_unproven_sites.py --score`: precision of the hand check (#96), overall and per flag."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from check_unproven_sites import score  # noqa: E402

HEADER = "register,name,url,host_exact,email_at_domain,jsonld_name,city_on_page,verdict,note\n"


def test_unreachable_and_unclear_are_left_out_and_flags_split_the_score(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text(HEADER + "\n".join([
        "HRB 1,A,https://a.de,1,1,0,0,own_site,",
        "HRB 2,B,https://b.de,1,0,0,0,directory,",
        "HRB 3,C,https://c.de,0,0,0,0,namesake,",
        "HRB 4,D,https://d.de,1,1,0,0,unreachable,",
        "HRB 5,E,https://e.de,1,1,0,0,,not checked yet",
    ]) + "\n", encoding="utf-8")
    result = score(path)
    assert result["all"] == (1, 3)
    assert result["host_exact"] == (1, 2)
    assert result["host_exact+email_at_domain"] == (1, 1)


def test_a_misspelt_verdict_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text(HEADER + "HRB 1,A,https://a.de,1,0,0,0,own site,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="own site"):
        score(path)


def test_a_file_without_a_verdict_column_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text(HEADER.replace(",", ";") + "HRB 1;A;https://a.de;1;0;0;0;own_site;\n", encoding="utf-8")
    with pytest.raises(ValueError, match="verdict"):
        score(path)
