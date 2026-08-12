"""How sure are we that this URL is this company's website? One number, one place.

**Confidence** here is a number between 0 and 1. 1 means "we downloaded the page and it prints
this company's commercial-register number". 0.2 means "a program guessed and nothing was
checked". The rest of the system reads that number and nothing else, so this file is where
every argument about trust belongs.

Why it is its own module: the same URL can be proposed by three different methods, each with a
different reason. Scattering the arithmetic across those three methods is how you end up with
a directory page recorded at confidence 1.0 — which has happened in this repo before. One
function, one table of weights, one set of hard rules.

## How the number is built

1. Start from a **base**, decided by the strongest method that proposed the URL.
2. Add a weight for each piece of **evidence** collected about the page.
3. Apply the **hard rules**: some evidence disqualifies a URL outright, and an unverified
   guess is capped below the threshold at which anything downstream will believe it.
4. Clamp to the range 0.0 - 0.99. 1.0 is deliberately unreachable: we are never certain.

Worked example. "Wilkening Bauunternehmung GmbH", guessed domain `wilkening-bau.de`, page
fetched, Impressum found, register number on it matches:

    base url_pattern (unique name)   0.45
    + register number matches        0.45
    + postcode matches               0.10
    + Impressum page exists          0.08
    ------------------------------------
    = 1.08, clamped to               0.99

Same company, but the page has no Impressum and never names the company:

    base url_pattern (unique name)   0.45
    - no Impressum found             0.05
    ------------------------------------
    = 0.40, then capped to           0.34

The cap fires because none of the evidence is *verifying* evidence — see `_VERIFYING_EVIDENCE`
below. The page was downloaded, but it confirmed nothing: no Impressum, no register number, no
postcode, and the company's name never appears. So the arithmetic reaching 0.40 is not enough.
The result is 0.34, one notch under the 0.35 promotion bar, which means the URL is kept as a
lead and never becomes the company's website. A guessed domain that answers HTTP 200 is not
evidence on its own — parked pages and unrelated businesses answer 200 too.

## The thresholds that matter

- **0.35** — the number the rest of the platform uses to decide whether a website observation
  may become the company's canonical website. Anything below it is kept as a candidate and
  never promoted. Not invented here; it is what `pipeline_coordinator` already applies.
- **0.34** — the cap on a URL that was never verified by downloading it. One notch under 0.35,
  on purpose, so an unchecked guess can never be promoted no matter how much else is stacked
  on top of it.

## What can never happen

Two rules override all arithmetic, because both have produced wrong data before:

- A **directory host** is never a website. `northdata.de` describes companies for a living.
- A **register-number mismatch** is fatal. If the page prints HRB 12345 and the company is
  HRB 99999, this is a different company's site, however good the name match looked.

## What the language model is and is not allowed to do

A local model may read a downloaded page and say whether it thinks the page belongs to the
company (see `llm_judge.py`). Its opinion arrives here as ordinary evidence — `llm_says_own_site`
and friends — and is weighted like anything else. Three limits are deliberate:

1. Its opinion is **not** in `_VERIFYING_EVIDENCE`. So a page with no Impressum, no postcode and
   no company name stays capped at 0.34 no matter how sure the model sounds. The model can
   strengthen a page that already proved something; it cannot be the proof.
2. It cannot lift a **disqualifier**. A model calling a directory page a company's own site
   changes nothing.
3. It never produces a register-number *mismatch*. A mismatch is fatal, and a model misreading
   one digit would kill a correct website. Only the regex extraction, which quotes the page
   verbatim, may do that.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------------------
# Evidence names. Strings, not an enum, because they are written to CSV and JSON and read by
# people. Changing one is a breaking change to the output format.
# ---------------------------------------------------------------------------------------

REGISTER_MATCH = "register_number_match"
REGISTER_MISMATCH = "register_number_mismatch"
POSTCODE_MATCH = "postcode_match"
CITY_ON_PAGE = "city_on_page"
NAME_ON_PAGE = "company_name_on_page"
HOST_EXACT = "host_is_exact_name"
HOST_PARTIAL_NAME = "host_is_name_fragment"
IMPRINT_FOUND = "imprint_found"
NO_IMPRINT = "no_imprint_found"
OSM_STRONG_MATCH = "osm_match_strong"
#: OpenStreetMap holds this business's email address, and this domain is the part after the @.
#: A business's email domain is normally its website. The important property is that this evidence
#: owes NOTHING to guessing the name — which is why it is allowed to lift the name-mismatch
#: refusal below, and why it is only ever emitted for a strong name+address match.
OSM_EMAIL_DOMAIN = "osm_email_domain"
REDIRECT_OFF_HOST = "redirected_to_other_host"
AGREE_TWO = "two_methods_agree"
AGREE_THREE = "three_methods_agree"
DIRECTORY_HOST = "directory_host"
HOST_NAME_MISMATCH = "host_name_mismatch"
GENERIC_HOST = "host_is_an_industry_not_a_company"

#: What a local language model concluded after reading the downloaded page. Three verdicts, and
#: the middle one is the whole reason this exists: a page can print a company's name, address and
#: register number and still belong to somebody else — a directory, a register extract, a news
#: article. "Is this page ABOUT the company or IS it the company's" is the judgement the
#: hand-written string matching cannot make.
LLM_OWN_SITE = "llm_says_own_site"
LLM_ABOUT_COMPANY = "llm_says_page_is_about_company"
LLM_UNRELATED = "llm_says_unrelated"

#: Methods, strongest first. The base is set by the strongest method that proposed the URL.
METHOD_URL_PATTERN = "url_pattern"
METHOD_OSM = "osm"
METHOD_LOCAL_LLM = "local_llm"

# ---------------------------------------------------------------------------------------
# The weights. Every one of these is a judgement call, not a measurement. They are grouped so
# that the relative order is the argument: a register match must outweigh everything else put
# together, and no amount of soft evidence may reach the register-match band on its own.
# ---------------------------------------------------------------------------------------

#: OpenStreetMap: a person mapped this exact business at this exact address and typed its
#: website into the map. Stronger than a name guess, weaker than a register number.
BASE_OSM = float(os.getenv("COMBINED_BASE_OSM", "0.45"))

#: A language model reading search snippets. Weakest: it has read text about the company, not
#: the company's own page. Capped further down unless the page was actually fetched.
BASE_LOCAL_LLM = float(os.getenv("COMBINED_BASE_LOCAL_LLM", "0.15"))

#: URL pattern matching scales with how distinctive the name is, because that is exactly what
#: decides whether the guess could have hit someone else's domain. "Zweirad Liebig" guessed
#: right is worth more than "Nordbau" guessed right.
BASE_URL_PATTERN_MIN = float(os.getenv("COMBINED_BASE_URL_PATTERN_MIN", "0.20"))
BASE_URL_PATTERN_RANGE = float(os.getenv("COMBINED_BASE_URL_PATTERN_RANGE", "0.25"))

WEIGHTS: dict[str, float] = {
    # Hard proof: the page prints the company's own commercial-register number.
    REGISTER_MATCH: 0.45,
    # Address agreement. The postcode is on the Impressum; the city may be anywhere on the page.
    POSTCODE_MATCH: 0.10,
    CITY_ON_PAGE: 0.04,
    # The page writes the company's name out in full.
    NAME_ON_PAGE: 0.10,
    # The domain is the name, not merely contains it: `zweirad-liebig.de` for Zweirad Liebig.
    HOST_EXACT: 0.08,
    # The opposite: the domain is one word out of the name, or its initials. `judentum.de` was
    # guessed for "Judentum begreifen - Projekt für Schule und Gemeinde" from the first word
    # alone, and the page mentions the same words because it is about the same subject — not
    # because it belongs to that organisation. The less of the name is in the domain, the less
    # a name match on the page proves.
    HOST_PARTIAL_NAME: -0.10,
    # An Impressum exists at all. Every German business site must have one, so its absence is
    # a small mark against the page being a business site.
    IMPRINT_FOUND: 0.08,
    NO_IMPRINT: -0.05,
    # The OSM entry matched on name AND postcode, not just name.
    OSM_STRONG_MATCH: 0.15,
    # The mapped business's own email address is at this domain. Worth about what a strong map
    # match is worth: somebody stood in front of the shop and typed its contact details in.
    OSM_EMAIL_DOMAIN: float(os.getenv("COMBINED_W_OSM_EMAIL_DOMAIN", "0.15")),
    # The guessed domain forwards somewhere else. Often legitimate (a parent company), often
    # not (an expired domain resold). Small penalty, never fatal.
    REDIRECT_OFF_HOST: -0.05,
    # Independent methods reaching the same host is the strongest soft signal available,
    # because the methods share no inputs beyond the company name.
    AGREE_TWO: 0.12,
    AGREE_THREE: 0.20,
    # A model read the page and said it belongs to this company. Deliberately worth less than
    # NAME_ON_PAGE (0.10) and IMPRINT_FOUND (0.08) together: when the page itself carries facts,
    # those facts outrank an opinion about them. Enough to lift a `low` row into `medium` when
    # real evidence is already present, never enough to carry a row on its own — see
    # `_VERIFYING_EVIDENCE`.
    LLM_OWN_SITE: float(os.getenv("COMBINED_W_LLM_OWN_SITE", "0.12")),
    # The model says the page is about the company without belonging to it. This is the failure
    # mode that has produced wrong data in this repo more than any other, so it is a real penalty
    # — but a penalty, not a disqualification. A model is not given the power to delete a row.
    LLM_ABOUT_COMPANY: float(os.getenv("COMBINED_W_LLM_ABOUT_COMPANY", "-0.15")),
    # The model says this is somebody else's business entirely.
    LLM_UNRELATED: float(os.getenv("COMBINED_W_LLM_UNRELATED", "-0.20")),
}

#: The platform's existing bar for promoting a website observation to canonical.
WEBSITE_MIN_CONFIDENCE = float(os.getenv("COMBINED_WEBSITE_MIN_CONFIDENCE", "0.35"))

#: A URL nobody downloaded cannot go above this. One notch below WEBSITE_MIN_CONFIDENCE.
UNVERIFIED_CAP = float(os.getenv("COMBINED_UNVERIFIED_CAP", "0.34"))

#: A page a model read and said belongs to somebody else cannot go above this either.
#:
#: Measured 2026-08-12 on 100 companies: **5 of the 11 promoted websites were pages the judge had
#: explicitly rejected**, and every one of those rejections was right — a DomainMarket sales
#: listing, a US haircare brand, a company in Ohio, a yacht charter, and a differently-owned
#: winery. A -0.20 penalty moved them from `medium` to `low` and left them above the 0.35 bar, so
#: the arithmetic quietly overruled the model.
#:
#: A cap rather than a disqualification, deliberately. The row keeps its evidence and stays in the
#: CSV where somebody can argue with it; it just cannot become a company's website. That is the
#: same shape as UNVERIFIED_CAP, and it holds the line that a model may refuse to vouch for a page
#: without being able to delete it. The known failure mode is a model that is too strict — it
#: called `bruecklyn.de` "unrelated" while the page printed the company's own register number — so
#: `_JUDGE_SKIP_EVIDENCE` in `pipeline.py` never shows it a page that is already proven.
NOT_OWN_SITE_CAP = float(os.getenv("COMBINED_NOT_OWN_SITE_CAP", "0.34"))

#: Ceiling. Certainty is not on the menu.
MAX_CONFIDENCE = 0.99

#: Evidence that only exists because a page was downloaded and read.
#:
#: `LLM_OWN_SITE` is missing from this set on purpose, and that omission is load-bearing. A model
#: reading a page technically means the page was downloaded — but "the model was convinced" is not
#: the same kind of fact as "the page prints this postcode". Adding it here would let a page with
#: no Impressum, no name and no address be promoted on the model's word alone, which is exactly
#: the arrangement this pipeline is built to avoid. Set COMBINED_LLM_VERIFIES=true to change that
#: once there are measured numbers saying the model deserves it.
_VERIFYING_EVIDENCE = frozenset(
    {REGISTER_MATCH, POSTCODE_MATCH, NAME_ON_PAGE, IMPRINT_FOUND}
    | ({LLM_OWN_SITE} if os.getenv("COMBINED_LLM_VERIFIES", "false").lower() == "true" else set())
)


def register_digits(text: str | None) -> str:
    """The number out of a commercial-register string. "Amtsgericht Osnabrück HRB 213220" -> "213220".

    Lives here because this module owns what REGISTER_MATCH means, and two callers need to agree
    on it exactly: the regex extraction in `pipeline.py` and the model transcription in
    `llm_judge.py`. If those two ever disagreed about which digits count, the same page would
    match for one and mismatch for the other.
    """
    match = re.search(r"\d+", text or "")
    return match.group(0) if match else ""

#: The model's two ways of saying "this page is not this company's". Both cap; neither disqualifies.
_NOT_OWN_SITE = frozenset({LLM_UNRELATED, LLM_ABOUT_COMPANY})

#: Evidence that ends the discussion.
_DISQUALIFIERS = {
    REGISTER_MISMATCH: "the page prints a different company's register number",
    DIRECTORY_HOST: "the host is a business directory, not a company",
    HOST_NAME_MISMATCH: "the domain does not carry the company's name",
    GENERIC_HOST: "the domain names an industry, which cannot identify one company",
}

#: Which refusals can be lifted, and by what. Two entries only, and each one had to earn its place
#: by answering the exact objection the refusal makes.
#:
#: 1. **An industry domain** — `anlagenbau.de`, `automobile.com`, `judentum.de` — is refused
#:    because hundreds of companies fit it equally well. A commercial-register number printed on
#:    the page answers precisely that: the page says which company it belongs to. Nothing softer
#:    counts, because a name on the page is what an industry domain carries for every company in
#:    that industry.
#: 2. **A domain that does not carry the company's name** is refused because a guess that missed
#:    the name is a guess that landed on somebody else. But an email domain was never a guess —
#:    OpenStreetMap holds this business's own email address, and a business's mail domain does not
#:    have to look like its name. `stallkamp.de` would survive the name test; plenty do not.
#:
#: `DIRECTORY_HOST` and `REGISTER_MISMATCH` appear in neither list and never will. A register
#: number on a directory page proves the page is ABOUT the company, never that it IS its site, and
#: a business whose mapped email sits at a directory is a mapping error rather than a website.
_LIFTED_BY: dict[str, frozenset[str]] = {
    GENERIC_HOST: frozenset({REGISTER_MATCH}),
    HOST_NAME_MISMATCH: frozenset({OSM_EMAIL_DOMAIN}),
}

BANDS = (
    (0.80, "high"),
    (0.50, "medium"),
    (WEBSITE_MIN_CONFIDENCE, "low"),
    (0.0, "reject"),
)


def band_for(value: float) -> str:
    """Turn the number into a word, for people who have to read 1000 rows of it.

    high    the page was downloaded and it identifies the company
    medium  several independent signals agree
    low     above the promotion threshold, but resting on one weak signal
    reject  below the threshold; kept as a candidate, never promoted
    """
    for floor, name in BANDS:
        if value >= floor:
            return name
    return "reject"


@dataclass
class Verdict:
    """The confidence number, plus everything needed to argue with it."""

    value: float
    band: str
    #: The single fact that most justifies the number. Mirrors the platform's existing
    #: `website_verified_by` column, so a row here can be compared with a row from a DAG.
    verified_by: str
    evidence: list[str] = field(default_factory=list)
    methods: list[str] = field(default_factory=list)
    #: Plain-language arithmetic, e.g. "base url_pattern 0.45 + register_number_match 0.45".
    why: str = ""
    disqualified_because: str | None = None

    @property
    def promotable(self) -> bool:
        """True when this URL clears the platform's bar for becoming the canonical website."""
        return self.value >= WEBSITE_MIN_CONFIDENCE


def base_for(method: str, name_uniqueness: float) -> float:
    """Starting confidence for the strongest method that proposed a URL.

    `name_uniqueness` runs 0 (a single generic word) to 1 (long, multi-word, distinctive) and
    is computed by `url_discovery.scorer.name_uniqueness`. It only moves the URL-pattern base,
    because it only describes the risk that a *guessed* domain belongs to somebody else.
    """
    if method == METHOD_OSM:
        return BASE_OSM
    if method == METHOD_LOCAL_LLM:
        return BASE_LOCAL_LLM
    return BASE_URL_PATTERN_MIN + BASE_URL_PATTERN_RANGE * max(0.0, min(1.0, name_uniqueness))


def _verified_by(evidence: set[str], methods: list[str]) -> str:
    """Name the strongest reason to believe this URL."""
    if REGISTER_MATCH in evidence:
        return REGISTER_MATCH
    if AGREE_THREE in evidence or AGREE_TWO in evidence:
        return "method_agreement"
    if POSTCODE_MATCH in evidence or NAME_ON_PAGE in evidence:
        return "page_content_match"
    if METHOD_OSM in methods:
        return "osm_website_tag"
    if IMPRINT_FOUND in evidence:
        return "imprint_found"
    # Below `page_content_match` and `imprint_found` on purpose: whenever the page itself supplies
    # a reason, that reason is named instead. A row labelled `llm_judge` means the model's opinion
    # really was the best thing we had, which is precisely the row a reviewer should look at.
    if LLM_OWN_SITE in evidence:
        return "llm_judge"
    if METHOD_LOCAL_LLM in methods and METHOD_URL_PATTERN not in methods:
        return "llm_snippet"
    return "url_pattern"


def score(
    *,
    methods: list[str],
    evidence: set[str] | list[str],
    name_uniqueness: float = 0.5,
) -> Verdict:
    """Turn evidence into a confidence number.

    `methods` — which of url_pattern / osm / local_llm proposed this URL. More than one is
    what triggers the agreement bonus, and the caller must add AGREE_TWO / AGREE_THREE to the
    evidence itself; this function does not infer it, so that "agreement" always means what
    the pipeline decided it means.

    `evidence` — the names defined at the top of this module.
    """
    evidence_set = set(evidence)
    ordered_methods = [
        m for m in (METHOD_URL_PATTERN, METHOD_OSM, METHOD_LOCAL_LLM) if m in methods
    ]

    # A register number is only proof of ownership when nothing says the page is merely quoting it.
    # This is the oldest rule in this repo — a register number on a page proves the page is ABOUT a
    # company, never that it IS that company's site — and until the judge existed, the only way to
    # spot a quoting page was a hand-maintained list of directory hostnames. A model that has read
    # the page and calls it a directory is the same claim, arrived at by reading.
    #
    # Measured: a hand-written FirmenWissen listing, correctly identified by the model, still scored
    # 0.805 and landed in the `high` band, because +0.45 for the register match dwarfed the -0.15
    # penalty. Suppressing the award drops it to `medium` and leaves it promotable on its other
    # merits, which is the intended balance — the model demotes, it does not delete.
    register_is_proof = REGISTER_MATCH in evidence_set and LLM_ABOUT_COMPANY not in evidence_set

    for name, reason in _DISQUALIFIERS.items():
        if name in evidence_set:
            lifters = _LIFTED_BY.get(name, frozenset())
            # A register match only lifts anything while it counts as proof — a number the page
            # merely quotes answers nothing about whose page it is.
            if REGISTER_MATCH in lifters and register_is_proof:
                continue
            if (lifters - {REGISTER_MATCH}) & evidence_set:
                continue
            return Verdict(
                value=0.0,
                band="reject",
                verified_by="disqualified",
                evidence=sorted(evidence_set),
                methods=ordered_methods,
                why=f"disqualified: {reason}",
                disqualified_because=name,
            )

    # The strongest method wins the base. OSM outranks a name guess; a name guess outranks a
    # model reading snippets.
    if METHOD_OSM in ordered_methods:
        strongest = METHOD_OSM
    elif METHOD_URL_PATTERN in ordered_methods:
        strongest = METHOD_URL_PATTERN
    else:
        strongest = METHOD_LOCAL_LLM

    value = base_for(strongest, name_uniqueness)
    parts = [f"base {strongest} {value:.2f}"]

    # AGREE_THREE replaces AGREE_TWO rather than stacking on it.
    applicable = set(evidence_set)
    if AGREE_THREE in applicable:
        applicable.discard(AGREE_TWO)

    # The evidence LIST still reports the register match, because it really was on the page and
    # hiding that would make the row impossible to argue with. It just does not earn anything.
    quoted_note = ""
    if REGISTER_MATCH in applicable and not register_is_proof:
        applicable.discard(REGISTER_MATCH)
        quoted_note = (
            " (the register number on the page is not counted: a model that read the page says the"
            " page only quotes it)"
        )

    for name in sorted(applicable):
        weight = WEIGHTS.get(name)
        if not weight:
            continue
        value += weight
        parts.append(f"{'+' if weight > 0 else '-'} {name} {abs(weight):.2f}")

    capped_note = ""
    if not (evidence_set & _VERIFYING_EVIDENCE) and value > UNVERIFIED_CAP:
        value = UNVERIFIED_CAP
        capped_note = f" -> capped at {UNVERIFIED_CAP} (nothing was downloaded and checked)"
    if evidence_set & _NOT_OWN_SITE and value > NOT_OWN_SITE_CAP:
        value = NOT_OWN_SITE_CAP
        capped_note = (
            f" -> capped at {NOT_OWN_SITE_CAP} (a model read the page and said it belongs to"
            " somebody else)"
        )

    value = round(max(0.0, min(MAX_CONFIDENCE, value)), 4)
    return Verdict(
        value=value,
        band=band_for(value),
        # `applicable`, not `evidence_set`: a suppressed register match must not be named as the
        # thing that proved the row, or the CSV would credit proof the number never received.
        verified_by=_verified_by(applicable, ordered_methods),
        evidence=sorted(evidence_set),
        methods=ordered_methods,
        why=" ".join(parts) + f" = {value}{capped_note}{quoted_note}",
    )
