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

The cap fires because none of the evidence is *verifying* evidence — see `identifies_company`
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
#: A Geschäftsführer named on the page's Impressum is one of this company's officers in the register.
#: Identity evidence of the same kind as the register number: a fact about THIS company that a
#: directory or another company's site has no reason to carry on its own Impressum.
OFFICER_MATCH = "officer_name_on_imprint"
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
#: The Impressum prints the phone number OpenStreetMap holds for this business. Like
#: OSM_EMAIL_DOMAIN, its value is that it owes nothing to guessing the name: somebody stood in front
#: of the shop and typed the number in, and the page prints the same number.
PHONE_MATCHES_OSM = "phone_matches_osm"
#: The Impressum's own email address is at the domain being tested — `info@stallkamp.de` on
#: stallkamp.de. Weak by design: nearly every real company site does this, so it says the page is a
#: genuine business site without saying WHICH business. Same class of fact as IMPRINT_FOUND.
IMPRINT_EMAIL_ON_DOMAIN = "imprint_email_at_this_domain"
#: A German VAT identification number appears on the page. Also the "real business site" class —
#: a company quoting somebody else's VAT number is rare, but so is it proving anything about whose
#: site this is.
VAT_ID_FOUND = "vat_id_found"
#: The page's own structured data — the schema.org JSON block search engines read — names this
#: company, or gives this company's postcode. Stronger in kind than the same fact found in the page
#: text: this is the site stating who it belongs to as data, in the place a site puts that claim for
#: machines to read, rather than a string that happened to appear somewhere in the markup.
JSONLD_NAME_MATCH = "jsonld_org_name_match"
JSONLD_POSTCODE_MATCH = "jsonld_postcode_match"
REDIRECT_OFF_HOST = "redirected_to_other_host"
AGREE_TWO = "two_methods_agree"
AGREE_THREE = "three_methods_agree"
DIRECTORY_HOST = "directory_host"
HOST_NAME_MISMATCH = "host_name_mismatch"
GENERIC_HOST = "host_is_an_industry_not_a_company"

#: Methods, strongest first. The base is set by the strongest method that proposed the URL.
METHOD_URL_PATTERN = "url_pattern"
METHOD_OSM = "osm"
#: A website Wikidata lists for the company (property P856, issue #26). Trusted like an OSM website
#: tag: a curated record proposed it, but it may be dead or hijacked, so only the page can prove it.
METHOD_WIKIDATA = "wikidata"
#: A domain whose Impressum in the Common Crawl names the company's register number (issue #56). Not a
#: curated record, so it starts like a guessed domain: a register number proves a page is ABOUT the
#: company, and only the page read again can prove it is the company's own.
METHOD_COMMON_CRAWL = "common_crawl"

# ---------------------------------------------------------------------------------------
# The weights. Every one of these is a judgement call, not a measurement. They are grouped so
# that the relative order is the argument: a register match must outweigh everything else put
# together, and no amount of soft evidence may reach the register-match band on its own.
# ---------------------------------------------------------------------------------------

#: OpenStreetMap: a person mapped this exact business at this exact address and typed its
#: website into the map. Stronger than a name guess, weaker than a register number.
BASE_OSM = float(os.getenv("COMBINED_BASE_OSM", "0.45"))

#: URL pattern matching scales with how distinctive the name is, because that is exactly what
#: decides whether the guess could have hit someone else's domain. "Zweirad Liebig" guessed
#: right is worth more than "Nordbau" guessed right.
BASE_URL_PATTERN_MIN = float(os.getenv("COMBINED_BASE_URL_PATTERN_MIN", "0.20"))
BASE_URL_PATTERN_RANGE = float(os.getenv("COMBINED_BASE_URL_PATTERN_RANGE", "0.25"))

WEIGHTS: dict[str, float] = {
    # Hard proof: the page prints the company's own commercial-register number.
    REGISTER_MATCH: 0.45,
    # Slightly below the register number: officers change and the register snapshot is 2017–2019,
    # so a match is strong but a miss says little.
    OFFICER_MATCH: float(os.getenv("COMBINED_W_OFFICER_MATCH", "0.40")),
    # Address agreement. The postcode is on the Impressum; the city may be anywhere on the page.
    POSTCODE_MATCH: 0.10,
    CITY_ON_PAGE: 0.04,
    # The page writes the company's name out in full.
    NAME_ON_PAGE: 0.10,
    # The same claim, but made in the page's structured data. Priced slightly above NAME_ON_PAGE
    # because of where it is: a name in the page text can be a customer reference, a supplier list or
    # a news item, while the schema.org block is where a site says who IT is. Priced only slightly
    # above, because it is still the page talking about itself and anybody can write anything there.
    JSONLD_NAME_MATCH: float(os.getenv("COMBINED_W_JSONLD_NAME_MATCH", "0.12")),
    JSONLD_POSTCODE_MATCH: float(os.getenv("COMBINED_W_JSONLD_POSTCODE_MATCH", "0.08")),
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
    # The page prints the phone number the map holds for this business. Two independent records of
    # the same company agreeing on a contact detail, so worth as much as the name on the page —
    # and unlike the name, a phone number is not a word anybody could have guessed.
    PHONE_MATCHES_OSM: float(os.getenv("COMBINED_W_PHONE_MATCHES_OSM", "0.10")),
    # The last two are the "this is a real business site" class, the same class as IMPRINT_FOUND,
    # and they are priced like it. Neither says which company the site belongs to, so neither is in
    # `identifies_company` and neither can carry a row past the cap on its own. They only ever
    # refine a row that already has a fact naming the company.
    IMPRINT_EMAIL_ON_DOMAIN: float(os.getenv("COMBINED_W_IMPRINT_EMAIL_ON_DOMAIN", "0.06")),
    VAT_ID_FOUND: float(os.getenv("COMBINED_W_VAT_ID_FOUND", "0.04")),
    # The guessed domain forwards somewhere else. Often legitimate (a parent company), often
    # not (an expired domain resold). Small penalty, never fatal.
    REDIRECT_OFF_HOST: -0.05,
    # Independent methods reaching the same host is the strongest soft signal available,
    # because the methods share no inputs beyond the company name.
    AGREE_TWO: 0.12,
    AGREE_THREE: 0.20,
}

#: The platform's existing bar for promoting a website observation to canonical.
WEBSITE_MIN_CONFIDENCE = float(os.getenv("COMBINED_WEBSITE_MIN_CONFIDENCE", "0.35"))

#: A URL nobody downloaded cannot go above this. One notch below WEBSITE_MIN_CONFIDENCE.
UNVERIFIED_CAP = float(os.getenv("COMBINED_UNVERIFIED_CAP", "0.34"))

#: Ceiling. Certainty is not on the menu.
MAX_CONFIDENCE = 0.99

#: What identifies the downloaded page as THIS company's. Only this lifts `UNVERIFIED_CAP`, so only
#: this can make a page the company's website (rule agreed 2026-10-28, issue #18):
#:
#:   - the page prints the company's own register number (`REGISTER_MATCH`), or
#:   - a Geschäftsführer on its Impressum is a register officer of the company (`OFFICER_MATCH`), or
#:   - the page names the company AND carries its postcode (name and place together).
#:
#: Name alone and postcode alone used to be enough each, and were not: checked against their
#: Impressum, only 34 of 106 Berlin websites had a register number or an officer, and most of the
#: rest rested on the company's name appearing on the page, which a reseller, a namesake or a group
#: page also does. One name-like fact plus one place fact is far harder to meet by accident.
#:
#: `IMPRINT_FOUND`, a VAT number and an email at the domain stay out for good: every German business
#: site has all three, so they say the page is *a* business, never *which*. Letting `IMPRINT_FOUND`
#: lift the cap once promoted 190 wrong websites.
_PROOF_EVIDENCE = frozenset({REGISTER_MATCH, OFFICER_MATCH})
_NAME_EVIDENCE = frozenset({NAME_ON_PAGE, JSONLD_NAME_MATCH})
_PLACE_EVIDENCE = frozenset({POSTCODE_MATCH, JSONLD_POSTCODE_MATCH})


def identifies_company(evidence: set[str] | frozenset[str]) -> bool:
    """True when the evidence ties the page to THIS company (see the rule above)."""
    evidence = set(evidence)
    return bool(evidence & _PROOF_EVIDENCE) or bool(evidence & _NAME_EVIDENCE and evidence & _PLACE_EVIDENCE)


def register_digits(text: str | None) -> str:
    """The number out of a commercial-register string. "Amtsgericht Osnabrück HRB 213220" -> "213220".

    Lives here because this module owns what REGISTER_MATCH means, so every caller agrees on which
    digits count and the same page can never match for one and mismatch for another.
    """
    match = re.search(r"\d+", text or "")
    return match.group(0) if match else ""


#: Which of the five German court registers an entry belongs to. Uppercased for comparison, so
#: `GnR` and `GNR` are one thing.
_REGISTER_PREFIX_RE = re.compile(r"\b(HRA|HRB|VR|GnR|PR)\b", re.IGNORECASE)


def register_parts(text: str | None) -> tuple[str, str]:
    """Which register, and which number. `"Amtsgericht Osnabrück HRB 213220"` -> `("HRB", "213220")`.

    The register matters as much as the number, because the five registers number independently: an
    association can be `VR 213220` while a company is `HRB 213220`, and those are not the same entity.
    Comparing digits alone would call that a match.

    The prefix comes back `""` when the string does not name a register — plenty of rows in
    `public.companies` hold a bare number — and callers then have to fall back to comparing digits.
    """
    prefix = _REGISTER_PREFIX_RE.search(text or "")
    return (prefix.group(1).upper() if prefix else ""), register_digits(text)


def standing_disqualifiers(evidence: set[str] | list[str]) -> set[str]:
    """Which refusals in this evidence set actually still stand.

    A disqualifier can be lifted by other evidence in the same set — see `_LIFTED_BY` — so "is
    `HOST_NAME_MISMATCH` present" is not the same question as "is this row refused".
    """
    present = set(evidence)
    return {
        name
        for name in _DISQUALIFIERS
        if name in present and not (present & _LIFTED_BY.get(name, frozenset()))
    }


def register_number_is_on_page(digits: str, text: str) -> bool:
    """Do these digits appear in the downloaded text as a number of their own?

    Here for the same reason as `register_digits`: `pipeline.py` reads the register number off a
    page that has no findable Impressum, and this module decides what that evidence is worth.

    Word boundaries matter, and only digit boundaries count: "882" must not be found inside
    "172907" or a phone number, or a company with a short register number would match half the
    internet. Letters either side are fine — "HRB213220" carries the number.
    """
    if not digits or not text:
        return False
    return re.search(rf"(?<!\d){re.escape(digits)}(?!\d)", text) is not None

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
#: 3. **Structured data naming the company** answers the name objection in the same way an email
#:    domain does — it owes nothing to guessing the domain. The page states, in the block search
#:    engines read, which organisation it belongs to. Measured over 1,000 companies: 3 pages refused
#:    for their domain declared the company in their JSON-LD — `5am-agua.com`, `thebigbash.de`, and
#:    `gmh-gruppe.de`, which names both "Georgsmarienhütte Holding" and "GMH Recycling GmbH".
#:
#:    That last one shows the limit of it: a group's site naming a subsidiary is genuinely arguable,
#:    and this lift does not settle it. Lifting the refusal only puts the row back in the running —
#:    it still has to clear 0.35 and identify the company. `pipeline._page_evidence` also refuses to
#:    emit this evidence for a page whose PATH looks like a directory listing, since a listing's
#:    structured data names the company it is listing.
#:
#: `DIRECTORY_HOST` and `REGISTER_MISMATCH` appear in neither list and never will. A register
#: number on a directory page proves the page is ABOUT the company, never that it IS its site, and
#: a business whose mapped email sits at a directory is a mapping error rather than a website.
_LIFTED_BY: dict[str, frozenset[str]] = {
    GENERIC_HOST: frozenset({REGISTER_MATCH}),
    HOST_NAME_MISMATCH: frozenset({OSM_EMAIL_DOMAIN, JSONLD_NAME_MATCH}),
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
    if method in (METHOD_OSM, METHOD_WIKIDATA):
        return BASE_OSM
    return BASE_URL_PATTERN_MIN + BASE_URL_PATTERN_RANGE * max(0.0, min(1.0, name_uniqueness))


def _verified_by(evidence: set[str], methods: list[str]) -> str:
    """Name the strongest reason to believe this URL."""
    if REGISTER_MATCH in evidence:
        return REGISTER_MATCH
    if OFFICER_MATCH in evidence:
        return "officer_match"
    if AGREE_THREE in evidence or AGREE_TWO in evidence:
        return "method_agreement"
    if POSTCODE_MATCH in evidence or NAME_ON_PAGE in evidence:
        return "page_content_match"
    if METHOD_OSM in methods:
        return "osm_website_tag"
    if METHOD_WIKIDATA in methods:
        return "wikidata_website"
    if IMPRINT_FOUND in evidence:
        return "imprint_found"
    return "url_pattern"


def score(
    *,
    methods: list[str],
    evidence: set[str] | list[str],
    name_uniqueness: float = 0.5,
) -> Verdict:
    """Turn evidence into a confidence number.

    `methods` — which of url_pattern / osm / wikidata proposed this URL. More than one is
    what triggers the agreement bonus, and the caller must add AGREE_TWO / AGREE_THREE to the
    evidence itself; this function does not infer it, so that "agreement" always means what
    the pipeline decided it means.

    `evidence` — the names defined at the top of this module.
    """
    evidence_set = set(evidence)
    ordered_methods = [
        m for m in (METHOD_URL_PATTERN, METHOD_OSM, METHOD_WIKIDATA, METHOD_COMMON_CRAWL) if m in methods
    ]

    for name, reason in _DISQUALIFIERS.items():
        if name in evidence_set:
            if _LIFTED_BY.get(name, frozenset()) & evidence_set:
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

    # The strongest method wins the base: a curated record (OSM, Wikidata) outranks a name guess.
    strongest = next((m for m in (METHOD_OSM, METHOD_WIKIDATA) if m in ordered_methods), METHOD_URL_PATTERN)

    value = base_for(strongest, name_uniqueness)
    parts = [f"base {strongest} {value:.2f}"]

    # AGREE_THREE replaces AGREE_TWO rather than stacking on it.
    applicable = set(evidence_set)
    if AGREE_THREE in applicable:
        applicable.discard(AGREE_TWO)

    for name in sorted(applicable):
        weight = WEIGHTS.get(name)
        if not weight:
            continue
        value += weight
        parts.append(f"{'+' if weight > 0 else '-'} {name} {abs(weight):.2f}")

    capped_note = ""
    if not identifies_company(evidence_set) and value > UNVERIFIED_CAP:
        value = UNVERIFIED_CAP
        capped_note = f" -> capped at {UNVERIFIED_CAP} (nothing on the page identifies the company)"

    value = round(max(0.0, min(MAX_CONFIDENCE, value)), 4)
    return Verdict(
        value=value,
        band=band_for(value),
        verified_by=_verified_by(applicable, ordered_methods),
        evidence=sorted(evidence_set),
        methods=ordered_methods,
        why=" ".join(parts) + f" = {value}{capped_note}",
    )
