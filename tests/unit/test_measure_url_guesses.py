"""`scripts/measure_url_guesses.py --round`: a hand-found website, sorted like the 111 misses (#89)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from measure_url_guesses import host_of, miss_reason  # noqa: E402


def test_the_host_is_read_however_the_person_typed_it() -> None:
    assert host_of("https://www.beta24.de/impressum") == "beta24.de"
    assert host_of(" Beta24.de ") == "beta24.de"
    assert host_of("www.beta24.de/kontakt") == "beta24.de"


def test_each_miss_lands_in_one_bucket() -> None:
    guessed = ["alpha-beta.de", "alphabeta.de", "alphabeta.com"]
    assert miss_reason("alphabeta.de", guessed, {"alphabeta.de"}) == "lost at the page check"
    assert miss_reason("alphabeta.de", guessed, set()) == "guessed, lost at the DNS lookup or probe"
    assert miss_reason("alpha-beta.berlin", guessed, set()) == "another TLD"
    assert miss_reason("beta24.de", guessed, set()) == "never generated"
