"""Guard the hand-written parts of website discovery: what it reads, and what that is worth.

Free to run. No search API, no language model, no database, no network — every case here is a
string in, a value out. Run it after touching `url_patterns.py`, `confidence.py`,
`structured_data.py`, `pipeline._page_evidence`, or `imprint_extractor`:

    python scripts/check_website_discovery.py

Four things are guarded, and the last two are where wrong answers have actually come from:

    URL guessing        which domains get generated, and which are refused
    confidence          what each piece of evidence is worth, and what cannot be promoted
    page reading        what `_page_evidence` takes from a page — and refuses to take
    extraction          postcodes and structured data, including the malformed shapes

Same idea as `scripts/check_own_site_filter.py`: the rules these two modules encode are
judgement calls that look arbitrary six months later, so each case says what it is protecting
against, and a failure prints the case rather than just a line number.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

from enrichment import confidence as conf  # noqa: E402
from enrichment import name_frequency as nf  # noqa: E402
from enrichment import pipeline as pl  # noqa: E402
from enrichment import structured_data as sd  # noqa: E402
from enrichment import url_patterns as up  # noqa: E402
from enrichment import imprint_extractor as ie  # noqa: E402

failures: list[str] = []


def check(name: str, got: object, want: object) -> None:
    if got != want:
        failures.append(f"  {name}\n      got  {got!r}\n      want {want!r}")


def check_true(name: str, got: bool) -> None:
    check(name, bool(got), True)


# ---------------------------------------------------------------------------------------
# Name cleaning
# ---------------------------------------------------------------------------------------

check("legal form removed", up.name_words("Zweirad Liebig GmbH"), ["zweirad", "liebig"])
check(
    "GmbH & Co. KG removed as one unit",
    up.name_words("Auto-Verkehr Heiter GmbH & Co. KG"),
    ["auto", "verkehr", "heiter"],
)
check("umlauts spelled out", up.name_words("Müller Straßenbau AG"), ["mueller", "strassenbau"])
check("hyphens split words", up.name_words("Auto-Verkehr"), ["auto", "verkehr"])

# A set lookup catches "Beteiligungs" and misses "Beteiligungsgesellschaft"; prefix matching
# catches both. Measured consequence: the company-name-on-page check looked for a compound no
# page prints, so a correct website scored 0.10 lower than it should.
check_true("compound shell word is low signal", up.is_low_signal("beteiligungsgesellschaft"))
check_true("plain shell word is low signal", up.is_low_signal("verwaltungs"))
check("a real word is not low signal", up.is_low_signal("dermatologie"), False)
check("'und' does not swallow 'unternehmen'", up.is_low_signal("unternehmensberatung"), False)


# ---------------------------------------------------------------------------------------
# Candidate generation
# ---------------------------------------------------------------------------------------


def hosts(name: str, city: str | None = None) -> list[str]:
    return [c.host for c in up.generate_candidates(name, city)]


def patterns(name: str, city: str | None = None) -> dict[str, str]:
    return {c.host: c.pattern for c in up.generate_candidates(name, city)}


guesses = hosts("Zweirad Liebig GmbH", "Osnabrück")
check("best guess is the full hyphenated name on .de", guesses[0], "zweirad-liebig.de")
check_true("the joined form is guessed too", "zweiradliebig.de" in guesses)

# German companies, German domains. Shape-major ordering spent the probe budget on
# `firma-name.com`, owned by a domain reseller, before reaching `firma.de`.
de_positions = [i for i, h in enumerate(guesses) if h.endswith(".de")]
com_positions = [i for i, h in enumerate(guesses) if h.endswith(".com")]
check_true("every .de guess comes before every .com guess", max(de_positions) < min(com_positions))

# `karl.de` belongs to somebody else. The name stays in the multi-word guesses, where it is
# harmless; only the single-word rule is blocked.
karl = hosts("Karl Widera, Gaststättenbetriebe")
check("a first name is not guessed on its own", "karl.de" in karl, False)
check_true("the first name still appears in a multi-word guess", "karl-widera.de" in karl)

# `bau.de` is not "Bau Meyer GmbH"'s website.
check("a generic word is not guessed on its own", "bau.de" in hosts("Bau Meyer GmbH"), False)

# The shell company's website is the operating company's.
wilkening = patterns("Wilkening Verwaltungs GmbH")
check("shell words are dropped for a second guess", wilkening.get("wilkening.de"), "core_words")

# "Müller Hoch- und Tiefbau" -> muellertiefbau.de, which a first-two-words rule cannot reach.
check_true("first+last word is guessed", "muellertiefbau.de" in hosts("Müller Hoch- und Tiefbau GmbH"))

# A 52-character label makes the resolver raise "label empty or too long" instead of returning
# a failure. Uncaught, one company took its whole row down.
long_name = "Judentum begreifen - Projekt für Schule und Gemeinde e.V."
check_true(
    "over-long domain labels are never generated",
    all(len(h.split(".")[0]) <= up.MAX_LABEL_LENGTH for h in hosts(long_name)),
)
check("a name that yields nothing yields no guesses", hosts("GmbH"), [])

# ---------------------------------------------------------------------------------------
# Umlaut spellings
#
# 4,016 of the 14,443 company names (28%) carry an umlaut or ß, and a German business may have
# registered any of three spellings of it. All three resolve in reality: mueller.de, muller.de and
# müller.de (which DNS carries as xn--mller-kva.de). Generating one of them probed a third of the
# namespace; generating three without teaching the NAME CHECK about them would have found real
# domains and then refused every one for "not carrying the company's name".
# ---------------------------------------------------------------------------------------

spellings = dict(up.name_word_spellings("Müller Straßenbau AG"))
check("the transliterated spelling is still first", spellings[""], ["mueller", "strassenbau"])
check("the dropped umlaut is offered", spellings[up.SPELLING_DROPPED], ["muller", "strassenbau"])
check("and the umlaut kept as itself", spellings[up.SPELLING_IDN], ["müller", "strassenbau"])

# 72% of the table has no umlaut. Those names must produce exactly one spelling — otherwise every
# company pays for a feature only some of them need.
check("a name without an umlaut yields one spelling", len(up.name_word_spellings("Zweirad Liebig GmbH")), 1)

# ß is the exception, and deliberately so. DENIC permits `straßen.de`, but Python's built-in `idna`
# codec is IDNA2003, whose nameprep step maps ß to ss — so `straßen.de`.encode("idna") returns
# `strassen.de`, exactly the spelling the transliteration already produced. Chasing the real
# `xn--straen-3ya.de` needs the IDNA2008 library, for a form German businesses rarely register when
# `ss` is available. So a ß-only name yields ONE spelling, and the third variant collapses harmlessly
# onto the first rather than costing a lookup.
check("a ß-only name yields one spelling", len(up.name_word_spellings("Straßen GmbH")), 1)
check(
    "and a name with both gets all three",
    len(up.name_word_spellings("Müller Straßenbau AG")),
    3,
)

# The variant GENERATION is off by default — measured on 100 companies it produced 5 fetched pages
# and no winner. Both states are guarded, so switching it on for a bigger sample does not mean
# re-deriving any of this. The verification half further down is always on.
check(
    "default off: an umlaut name guesses only the transliterated spelling",
    [h for h in hosts("Bäckerei Schäfer GmbH") if "backerei" in h or h.startswith("xn--")],
    [],
)
check("and the default is off", up.UMLAUT_VARIANTS, False)

up.UMLAUT_VARIANTS = True  # the rest of this block tests the capability, not the default
umlaut_hosts = hosts("Bäckerei Schäfer GmbH")
check_true("the transliterated domain is guessed", "baeckerei-schaefer.de" in umlaut_hosts)
check_true("the dropped-umlaut domain is guessed", "backerei-schafer.de" in umlaut_hosts)
check_true(
    "the umlaut domain is guessed, punycode-encoded for DNS",
    any(h.startswith("xn--") for h in umlaut_hosts),
)
# An umlaut domain is a .de registry feature. Encoding them for every TLD only spends lookups.
check(
    "punycode is only tried on .de",
    [h for h in umlaut_hosts if h.startswith("xn--") and not h.endswith(".de")],
    [],
)
# The spelling travels to the output so its yield is measurable rather than assumed.
check(
    "the winning pattern names the spelling",
    patterns("Bäckerei Schäfer GmbH").get("backerei-schafer.de"),
    "full_name+umlaut_dropped",
)

up.UMLAUT_VARIANTS = False  # back to the shipped default before anything else runs

# The verification half, which is ALWAYS on and is a correctness fix in its own right: a search
# provider handing us `müller.de` for "Müller GmbH" was previously refused for not carrying the
# company's name. The host has to land in the same alphabet as the name for any of that to work.
check("punycode decodes and transliterates", up.host_stem("xn--mller-kva.de"), "mueller")
check("a dropped-umlaut host is left as it is", up.host_stem("muller.de"), "muller")
check("an ordinary host is unchanged", up.host_stem("https://www.zweirad-liebig.de/x"), "zweiradliebig")
for host in ("https://mueller.de", "https://muller.de", "https://xn--mller-kva.de"):
    check(f"{host} fully covers the name", up.name_coverage(host, "Müller GmbH"), 1.0)
# And the loosening must not leak: a different company is still a different company.
check("a different name is still not covered", up.name_coverage("https://schmidt.de", "Müller GmbH"), 0.0)


# ---------------------------------------------------------------------------------------
# Parked-domain detection
# ---------------------------------------------------------------------------------------

check_true(
    "a for-sale page is parked",
    up.looks_parked("<html><body><h1>Diese Domain steht zum Verkauf</h1>" + "x " * 200 + "</body></html>"),
)
check_true("an almost empty page is parked", up.looks_parked("<html><body>Hallo</body></html>"))
# A domain broker's listing is a long, busy page — the "under 200 characters" rule never fires on
# it. Real text from goldrushproductions.com, which was scored as a live website for "Goldrush
# Productions GmbH" until 2026-08-12.
check_true(
    "a domain broker's sales page is parked",
    up.looks_parked(
        "<html><body>GoldRushProductions.com - Entertainment Domains for Sale - Buy Premium "
        "Media &amp; Events Domain Names | Domain for Sale. Call 888-694-6735. This premium "
        "domain is available for purchase from DomainMarket.com. Price $2,895. " + "x " * 300
        + "</body></html>"
    ),
)
# ...but a shop that sells things is not, which is why the looser "is available for purchase" was
# left out of the marker list.
check(
    "a shop selling products is not parked",
    up.looks_parked(
        "<html><body>Unsere Fahrräder. Das neue Modell ist ab sofort verfügbar und "
        "is available for purchase in unserem Ladengeschäft in Osnabrück. " + "x " * 300
        + "</body></html>"
    ),
    False,
)
check(
    "a real page is not parked",
    up.looks_parked("<html><body>" + "Wir bauen seit 1953 Häuser in Osnabrück. " * 20 + "</body></html>"),
    False,
)

# ---------------------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------------------

# The top of the ladder: the page prints the company's own register number.
top = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.REGISTER_MATCH, conf.POSTCODE_MATCH, conf.HOST_EXACT},
    name_uniqueness=0.9,
)
check("a register match lands in the high band", top.band, "high")
check("confidence never reaches 1.0", top.value <= conf.MAX_CONFIDENCE, True)
check("the reason is recorded", top.verified_by, conf.REGISTER_MATCH)

# Three things that must never produce a website, whatever else is true. Each of these has
# put wrong data in this database before.
for killer in (conf.DIRECTORY_HOST, conf.REGISTER_MISMATCH, conf.HOST_NAME_MISMATCH):
    verdict = conf.score(
        methods=[conf.METHOD_URL_PATTERN, conf.METHOD_OSM],
        evidence={killer, conf.IMPRINT_FOUND, conf.REGISTER_MATCH, conf.AGREE_TWO},
        name_uniqueness=1.0,
    )
    check(f"{killer} disqualifies outright", verdict.value, 0.0)
    check(f"{killer} is not promotable", verdict.promotable, False)
    check(f"{killer} says why", verdict.disqualified_because, killer)

# A model reading search snippets has read text ABOUT the company, not the company's page.
llm_only = conf.score(methods=[conf.METHOD_LOCAL_LLM], evidence=set(), name_uniqueness=1.0)
check_true("an unchecked model pick stays below the promotion bar", not llm_only.promotable)
check("an unchecked model pick is capped", llm_only.value <= conf.UNVERIFIED_CAP, True)

# ...but the same URL, once downloaded and verified, is allowed up the ladder.
llm_verified = conf.score(
    methods=[conf.METHOD_LOCAL_LLM],
    evidence={conf.IMPRINT_FOUND, conf.REGISTER_MATCH, conf.NAME_ON_PAGE},
    name_uniqueness=1.0,
)
check_true("a verified model pick is promotable", llm_verified.promotable)

# A guessed domain that answers, but where nothing on the page ties it to the company, is not
# an answer. This is the case the old scorer accepted at 0.35+ and this one refuses.
bare_guess = conf.score(
    methods=[conf.METHOD_URL_PATTERN], evidence={conf.NO_IMPRINT}, name_uniqueness=0.3
)
check_true("a guess with no page evidence is not promotable", not bare_guess.promotable)

# Two methods that share no inputs beyond the name reaching the same host is real evidence.
#
# All three rows below carry NAME_ON_PAGE, and that is not decoration. Since 2026-08-14 an Impressum
# no longer lifts UNVERIFIED_CAP, so without a fact naming this company every one of these rows would
# sit at 0.34 and the arithmetic being tested here would be invisible under the cap.
alone = conf.score(
    methods=[conf.METHOD_OSM],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE},
    name_uniqueness=0.5,
)
together = conf.score(
    methods=[conf.METHOD_OSM, conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.AGREE_TWO},
    name_uniqueness=0.5,
)
check_true("agreement raises confidence", together.value > alone.value)
check("agreement is named as the reason", together.verified_by, "method_agreement")

# Three-way agreement replaces the two-way bonus rather than stacking with it.
three = conf.score(
    methods=[conf.METHOD_OSM, conf.METHOD_URL_PATTERN, conf.METHOD_LOCAL_LLM],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.AGREE_TWO, conf.AGREE_THREE},
    name_uniqueness=0.5,
)
check(
    "three-way agreement is worth exactly its own weight",
    round(three.value - alone.value, 4),
    conf.WEIGHTS[conf.AGREE_THREE],
)

# A distinctive name guessed right is worth more than a generic one guessed right, because
# only the generic one could plausibly have hit somebody else's domain.
generic = conf.score(
    methods=[conf.METHOD_URL_PATTERN], evidence={conf.IMPRINT_FOUND}, name_uniqueness=0.05
)
unique = conf.score(
    methods=[conf.METHOD_URL_PATTERN], evidence={conf.IMPRINT_FOUND}, name_uniqueness=1.0
)
check_true("a distinctive name scores higher than a generic one", unique.value > generic.value)

# The bands are what a person reads instead of 1000 numbers.
check("0.85 is high", conf.band_for(0.85), "high")
check("0.60 is medium", conf.band_for(0.60), "medium")
check("0.40 is low", conf.band_for(0.40), "low")
check("0.30 is a reject", conf.band_for(0.30), "reject")
check("the promotion bar is the platform's 0.35", conf.WEBSITE_MIN_CONFIDENCE, 0.35)
check_true("the unverified cap sits below it", conf.UNVERIFIED_CAP < conf.WEBSITE_MIN_CONFIDENCE)

# ---------------------------------------------------------------------------------------
# Soft evidence never promotes a page
# ---------------------------------------------------------------------------------------
# Every German business site has an Impressum, a VAT number and an email at its own domain, so none
# of these says WHICH company a page belongs to. If any of them could reach the promotable band
# without a fact naming the company, a wrong website gets through — which is exactly what happened
# while IMPRINT_FOUND was still treated as verification (190 wrong websites). With the model check
# removed, this rule is the only thing that stops it.
for _soft in (conf.IMPRINT_FOUND, conf.HOST_EXACT, conf.VAT_ID_FOUND, conf.IMPRINT_EMAIL_ON_DOMAIN):
    check_true(
        f"{_soft} cannot promote a page on its own",
        not conf.score(
            methods=[conf.METHOD_URL_PATTERN], evidence={_soft}, name_uniqueness=1.0
        ).promotable,
    )
check_true(
    "not even all of them together can",
    not conf.score(
        methods=[conf.METHOD_OSM],
        evidence={
            conf.IMPRINT_FOUND,
            conf.HOST_EXACT,
            conf.VAT_ID_FOUND,
            conf.IMPRINT_EMAIL_ON_DOMAIN,
            conf.OSM_STRONG_MATCH,
        },
        name_uniqueness=1.0,
    ).promotable,
)
check_true(
    "one fact naming the company changes that",
    conf.score(
        methods=[conf.METHOD_OSM],
        evidence={conf.IMPRINT_FOUND, conf.HOST_EXACT, conf.OSM_STRONG_MATCH, conf.NAME_ON_PAGE},
        name_uniqueness=1.0,
    ).promotable,
)

# ---------------------------------------------------------------------------------------
# A domain that names an industry cannot identify one company
#
# Every case below was a real row in a 100-company run: a page that downloaded fine, printed
# words from the company's name, and belonged to somebody else entirely.
# ---------------------------------------------------------------------------------------

# Which words are trades is MEASURED, not listed — `name_frequency.common_words` counts how many
# company names contain each word. This corpus stands in for the 14,443-row table, so the
# threshold is scaled to it: a word in 2 of these 12 names plays the part of `immobilien` in 391
# of 14,443. No case in this file names a website.
CORPUS = [
    "Anlagenbau & Technischer Service Alte Poststrasse GmbH",
    "Meyer Anlagenbau GmbH",
    "S & E Automobile Verwaltungs- und Beteiligungsgesellschaft mbH",
    "VR-automobile GmbH",
    "Gartencenter Planta Hans Siewe GmbH",
    "Gartencenter Nord GmbH",
    "Zweirad Liebig Verwaltungs GmbH",
    "Brücklyn GmbH & Co. KG",
    "Judentum begreifen -Projekt für Schule und Gemeinde",
    "The Living Proof Agency GmbH",
    "Müller Hoch- und Tiefbau GmbH",
    "Uniformen Hinnersmann Inh. Kerstin Meyer",
]
common = nf.common_words(CORPUS, min_companies=2)

check_true("a word two companies share is measured as a trade", "anlagenbau" in common)
check_true("and so is automobile", "automobile" in common)
check_true("and gartencenter", "gartencenter" in common)
check_true("a word only one company has is not", "bruecklyn" not in common)
check_true("judentum appears once, so counting cannot flag it", "judentum" not in common)

check_true("anlagenbau.de is a trade, not a business", up.is_generic_stem(up.host_stem("https://www.anlagenbau.de"), common))
check_true("automobile.com is a trade", up.is_generic_stem(up.host_stem("https://www.automobile.com"), common))
check_true("zweirad-liebig.de is a business", not up.is_generic_stem(up.host_stem("https://www.zweirad-liebig.de"), common))

# What counting cannot catch, coverage must: `judentum` and `uniformen` are one company's whole
# name each, so no frequency rule will ever flag them. The domain carries a fragment of the name.
check_true(
    "a one-word domain from a longer name is a fragment",
    up.name_coverage("http://judentum.de", "Judentum begreifen -Projekt für Schule und Gemeinde") < 0.5,
)
check_true(
    "uniformen.de carries half of Uniformen Hinnersmann",
    up.name_coverage("https://www.uniformen.de", "Uniformen Hinnersmann, Inh. Kerstin Meyer") <= 0.5,
)
check_true(
    "muellertiefbau.de is not a fragment — it is most of the name",
    up.name_coverage("https://muellertiefbau.de", "Müller Hoch- und Tiefbau GmbH") >= 0.6,
)
check_true(
    "an article never becomes the domain",
    not any(h.startswith("theagency.") for h in hosts("The Living Proof Agency GmbH")),
)

# How much of the name the domain accounts for. The honest version of "the domain matches".
check("the full name covers all of it", up.name_coverage("https://bruecklyn.de", "Brücklyn GmbH & Co. KG"), 1.0)
check_true(
    "one word out of three covers a third",
    abs(up.name_coverage("http://theagency.de", "The Living Proof Agency GmbH") - 1 / 3) < 0.01,
)

# An industry domain is refused outright — unless the page prints THIS company's register
# number, which answers the question of whose site it is by itself.
industry = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.GENERIC_HOST, conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.CITY_ON_PAGE},
)
check("an industry domain is refused", industry.value, 0.0)
check("and says why", industry.disqualified_because, conf.GENERIC_HOST)

overruled = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.GENERIC_HOST, conf.IMPRINT_FOUND, conf.REGISTER_MATCH},
)
check_true("a register number on the page overrules it", overruled.promotable)
check("and the refusal is gone", overruled.disqualified_because, None)

# A register number never rescues the other three. A directory page quotes register numbers for
# a living; that is what makes it a directory.
directory = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.DIRECTORY_HOST, conf.REGISTER_MATCH},
)
check("a register number does not rescue a directory", directory.value, 0.0)


# ---------------------------------------------------------------------------------------
# The OpenStreetMap email domain
#
# A mapped business's email domain is normally its website, and it says so without anybody having
# guessed at the name. Two things must hold: a mail provider must never become a company's website,
# and a real email domain must survive the name check that a guessed domain has to pass.
# ---------------------------------------------------------------------------------------

check("info@stallkamp.de gives the domain", nf.email_domain("info@stallkamp.de"), "stallkamp.de")
check("case and trailing dots are cleaned", nf.email_domain("Info@Stallkamp.DE."), "stallkamp.de")
check("something that is not an address gives nothing", nf.email_domain("stallkamp.de"), None)
check("two @ signs give nothing", nf.email_domain("a@b@c.de"), None)
check("nothing in, nothing out", nf.email_domain(None), None)

# Counted, never listed. The point of measuring is the domains no list would hold: in Osnabrück the
# most common one is `osnanet.de`, a LOCAL provider used by 23 mapped businesses — three times more
# common there than web.de. A hand-written list of the national providers would miss it entirely.
mapped = (
    [{"email": f"info@firma{i}.de"} for i in range(20)]
    + [{"email": f"kontakt{i}@osnanet.de"} for i in range(23)]
    + [{"email": f"a{i}@web.de"} for i in range(7)]
    + [{"email": "x@zweimal.de"}, {"email": "y@zweimal.de"}]
    + [{"email": None}, {"email": "broken"}, {}]
)
shared = nf.common_email_domains(mapped)
check_true("a provider used by many businesses is refused", "osnanet.de" in shared)
check_true("so is a national free-mail host, by the same count", "web.de" in shared)
check_true("a domain used by one business is kept", "firma3.de" not in shared)
check_true(
    "a domain on exactly two objects is kept — usually one business mapped twice",
    "zweimal.de" not in shared,
)
check("missing and malformed addresses do not crash it", type(shared), set)

# An email domain does not have to look like the company's name — that is the entire value of it.
# Without a lift, the name-mismatch refusal would throw away every one of them.
email_site = conf.score(
    methods=[conf.METHOD_OSM],
    evidence={conf.HOST_NAME_MISMATCH, conf.OSM_EMAIL_DOMAIN, conf.OSM_STRONG_MATCH, conf.IMPRINT_FOUND},
)
check("an email domain survives the name check", email_site.disqualified_because, None)

# Surviving the name check is not the same as clearing the bar, and this pair records a real change
# of behaviour on 2026-08-14. This row used to reach 0.83 — `high` — on the strength of a map entry
# and the existence of an Impressum, with nothing on the page about this company at all. Now that an
# Impressum no longer lifts UNVERIFIED_CAP it stops at 0.34. The map entry gets the domain past the
# name refusal; the PAGE still has to confirm whose it is.
check("but an email-domain page that names nobody caps", email_site.value, conf.UNVERIFIED_CAP)
check_true("so a map entry alone cannot promote it", not email_site.promotable)
email_confirmed = conf.score(
    methods=[conf.METHOD_OSM],
    evidence={
        conf.HOST_NAME_MISMATCH,
        conf.OSM_EMAIL_DOMAIN,
        conf.OSM_STRONG_MATCH,
        conf.IMPRINT_FOUND,
        conf.NAME_ON_PAGE,
    },
)
check_true("and once the page names the company, it can", email_confirmed.promotable)

# It lifts that refusal and nothing else. A business mapped with an email at a directory is a
# mapping mistake, not a website.
check(
    "an email domain cannot rescue a directory host",
    conf.score(
        methods=[conf.METHOD_OSM],
        evidence={conf.DIRECTORY_HOST, conf.OSM_EMAIL_DOMAIN, conf.OSM_STRONG_MATCH},
    ).value,
    0.0,
)
check(
    "nor a register-number mismatch",
    conf.score(
        methods=[conf.METHOD_OSM],
        evidence={conf.REGISTER_MISMATCH, conf.OSM_EMAIL_DOMAIN, conf.OSM_STRONG_MATCH},
    ).value,
    0.0,
)
# And the reverse: a register match still lifts an industry domain, which is the rule that existed
# before the table had two rows in it.
check(
    "a register match still lifts an industry domain",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.GENERIC_HOST, conf.REGISTER_MATCH, conf.IMPRINT_FOUND},
    ).disqualified_because,
    None,
)
# A name mismatch with no email domain behind it is still refused.
check(
    "a guessed domain that missed the name is still refused",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.HOST_NAME_MISMATCH, conf.IMPRINT_FOUND, conf.NAME_ON_PAGE},
    ).value,
    0.0,
)
# The flag on its own, with nothing downloaded, must not clear the bar.
check(
    "an email domain nobody could download is capped",
    conf.score(
        methods=[conf.METHOD_OSM], evidence={conf.OSM_EMAIL_DOMAIN, conf.NO_IMPRINT}
    ).promotable,
    False,
)


# ---------------------------------------------------------------------------------------
# The judge — what a local language model reading the page is allowed to change
#
# Every case here exists because the alternative has cost this project data before: a page that
# merely mentions a company became that company's website. A model is confident and fluent and
# will say "yes, this is their site" about a directory listing, so the limits are enforced in code
# rather than asked for in the prompt.
# ---------------------------------------------------------------------------------------

# The model likes the page, but the page proved nothing at all — no Impressum, no name, no
# postcode. It must stay under the promotion bar. This is THE case: if it ever passes, the model
# has become the evidence instead of weighing it.
opinion_only = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.LLM_OWN_SITE, conf.NO_IMPRINT},
    name_uniqueness=1.0,
)
check_true("a model's opinion alone cannot promote a page", not opinion_only.promotable)
check("and it is the unverified cap that stops it", opinion_only.value, conf.UNVERIFIED_CAP)

# With real page evidence present, the opinion is allowed to add to it.
with_facts = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.LLM_OWN_SITE},
    name_uniqueness=1.0,
)
without_facts = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE},
    name_uniqueness=1.0,
)
check_true("with real evidence present, the opinion adds to it", with_facts.value > without_facts.value)
check_true("and the row is promotable", with_facts.promotable)

# A model calling a directory page a company's own website changes nothing.
directory_but_model_likes_it = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.DIRECTORY_HOST, conf.LLM_OWN_SITE, conf.IMPRINT_FOUND},
)
check("a model cannot rescue a directory page", directory_but_model_likes_it.value, 0.0)
check(
    "and the refusal still names the real reason",
    directory_but_model_likes_it.disqualified_because,
    conf.DIRECTORY_HOST,
)

# "This page is about the company but is not theirs" is a penalty, not a death sentence. The model
# is not given the power to delete a row that the page's own facts support.
about = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.LLM_ABOUT_COMPANY},
    name_uniqueness=1.0,
)
check_true("'about the company' costs the row something", about.value < without_facts.value)
check("but does not disqualify it", about.disqualified_because, None)

# A page the model says belongs to somebody else may not become a company's website, however much
# other evidence it carries.
#
# Measured 2026-08-12 on 100 companies, before this cap existed: 5 of the 11 promoted websites were
# pages the judge had explicitly rejected — a DomainMarket sales listing, a US haircare brand, a
# company in Ohio, a yacht charter, a differently-owned winery. A -0.20 penalty moved them from
# `medium` to `low` and left them above the 0.35 bar, so the arithmetic overruled the model.
for verdict_flag in (conf.LLM_UNRELATED, conf.LLM_ABOUT_COMPANY):
    rejected = conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.HOST_EXACT, verdict_flag},
        name_uniqueness=1.0,
    )
    check(f"{verdict_flag} caps the row", rejected.value, conf.NOT_OWN_SITE_CAP)
    check(f"{verdict_flag} row is not promotable", rejected.promotable, False)
    # A cap, not a disqualification: the row keeps its evidence and stays readable.
    check(f"{verdict_flag} is not a disqualifier", rejected.disqualified_because, None)
    check_true(
        f"{verdict_flag} row still lists what was found",
        conf.IMPRINT_FOUND in rejected.evidence,
    )
    check_true("and the arithmetic says why it was capped", "belongs to somebody else" in rejected.why)

# The cap binds even against the strongest evidence there is. A register number on a page the model
# calls a directory is the exact combination that scored 0.805 and reached `high` before any of
# this existed.
strongest = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.REGISTER_MATCH, conf.POSTCODE_MATCH, conf.IMPRINT_FOUND, conf.LLM_ABOUT_COMPANY},
    name_uniqueness=1.0,
)
check("even a register match cannot beat the cap", strongest.promotable, False)

# And it must not touch a page the model approved of, or one it never saw.
check_true(
    "an approved page is untouched by the cap",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE, conf.HOST_EXACT, conf.LLM_OWN_SITE},
        name_uniqueness=1.0,
    ).promotable,
)
check_true(
    "a page the judge never read is untouched by the cap",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.IMPRINT_FOUND, conf.REGISTER_MATCH},
        name_uniqueness=1.0,
    ).promotable,
)

# THE case this whole stage exists for. A directory prints the name, the address and the
# commercial-register number of every company it lists, so a register match on a directory page is
# worthless — and worth +0.45, which used to be enough to reach the `high` band on its own. Measured
# on a hand-written FirmenWissen listing: 0.805 and `high` before this rule existed.
quoting_directory = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={
        conf.REGISTER_MATCH,
        conf.POSTCODE_MATCH,
        conf.IMPRINT_FOUND,
        conf.LLM_ABOUT_COMPANY,
    },
    name_uniqueness=1.0,
)
publishing_own_site = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.REGISTER_MATCH, conf.POSTCODE_MATCH, conf.IMPRINT_FOUND},
    name_uniqueness=1.0,
)
check_true(
    "a quoted register number scores far below a published one",
    quoting_directory.value < publishing_own_site.value - 0.4,
)
check("and no longer reaches the high band", quoting_directory.band != "high", True)
check(
    "and is never credited as register-number proof",
    quoting_directory.verified_by != conf.REGISTER_MATCH,
    True,
)
check_true(
    "but the row survives on its other merits rather than being deleted",
    quoting_directory.value > 0.0 and quoting_directory.disqualified_because is None,
)
check_true(
    "and the arithmetic says out loud why the number earned nothing",
    "only quotes it" in quoting_directory.why,
)
# The evidence list still reports the register number. Hiding it would make the row unarguable.
check_true(
    "the suppressed register match is still listed as evidence",
    conf.REGISTER_MATCH in quoting_directory.evidence,
)

# An industry domain is overruled by a register match only when that match is real proof. A
# directory sitting on a generic domain must stay refused.
check(
    "a quoted register number cannot overrule an industry domain",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.GENERIC_HOST, conf.REGISTER_MATCH, conf.LLM_ABOUT_COMPANY},
    ).value,
    0.0,
)

# `unsure` must be indistinguishable from not having asked. A judge with no opinion may not move
# the number in either direction.
unsure = conf.score(
    methods=[conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.NAME_ON_PAGE},
    name_uniqueness=1.0,
)
check("an unsure judge changes nothing", unsure.value, without_facts.value)

# `verified_by` must never credit the model when the page itself supplied the reason.
check(
    "a page fact outranks the model in verified_by",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN],
        evidence={conf.NAME_ON_PAGE, conf.IMPRINT_FOUND, conf.LLM_OWN_SITE},
    ).verified_by,
    "page_content_match",
)
check(
    "the model is credited only when it is the best reason we have",
    conf.score(
        methods=[conf.METHOD_URL_PATTERN], evidence={conf.LLM_OWN_SITE}
    ).verified_by,
    "llm_judge",
)

# One register-number reading, shared by the regex and the model, so the same page can never match
# for one and mismatch for the other.
check("the register digits come out of a full register string", conf.register_digits("Amtsgericht Osnabrück HRB 213220"), "213220")
check("a bare number survives", conf.register_digits("HRB 882"), "882")
check("nothing in, nothing out", conf.register_digits(None), "")


# ---------------------------------------------------------------------------------------
# Postcodes: a five-digit number is not an address
# ---------------------------------------------------------------------------------------
#
# This used to be the first five-digit run anywhere on the page. A postcode agreeing with the
# company's is treated as verification and lifts the confidence cap, so a price or an article number
# was enough to promote a website.

check("a postcode with its town is found", ie.extract_postcodes("Musterstr. 1\n49074 Osnabrück"), ["49074"])
check("the country prefix is allowed", ie.extract_postcodes("D-49074 Osnabrück"), ["49074"])
check("a two-word town works", ie.extract_postcodes("49214 Bad Rothenfelde"), ["49214"])
check("an abbreviated town works", ie.extract_postcodes("66606 St. Wendel"), ["66606"])
check("a town in capitals works", ie.extract_postcodes("49074 OSNABRÜCK"), ["49074"])
check("a price is not an address", ie.extract_postcodes("Preis: 12500 EUR"), [])
check("an article number is not an address", ie.extract_postcodes("Artikel 49074 bestellen"), [])
check("a phone number is not an address", ie.extract_postcodes("Tel. 0541 1234567"), [])
check("00xxx is not a German postcode", ie.extract_postcodes("00123 Nowhere"), [])
check(
    "both addresses on an Impressum are returned",
    ie.extract_postcodes("49074 Osnabrück\nPostfach 49009 Osnabrück"),
    ["49074", "49009"],
)
check("the single-value helper still answers", ie.extract_postcode("49074 Osnabrück"), "49074")
check("and returns None when there is no address", ie.extract_postcode("nothing here"), None)

# ---------------------------------------------------------------------------------------
# Register numbers: a thousands separator is not the end of the number
# ---------------------------------------------------------------------------------------
#
# ok-personalprofis.de prints `HRB 222 442`. A bare `(\d+)` read `222`, `score_candidate` compared
# it to 222442, and applied the -200 MISMATCH penalty — the one meaning "this page belongs to a
# different company". A correct website was punished harder than one with no number at all.
# Measured over the stored runs before the fix: 13 candidates across 8 companies.
#
# The second half of this block is the reason the grouped branch demands a separator. A greedy
# rule would read "HRB 12345 49074 Osnabrück" as one number and invent an entry no register holds.

check("a space as thousands separator", ie.extract_register_numbers("HRB 222 442"), ["HRB 222442"])
check("a dot as thousands separator", ie.extract_register_numbers("HRB 222.442"), ["HRB 222442"])
check("a non-breaking space", ie.extract_register_numbers("HRB 222 442"), ["HRB 222442"])
check("a narrow no-break space", ie.extract_register_numbers("HRB 222 442"), ["HRB 222442"])
check("two groups", ie.extract_register_numbers("HRA 12 345 678"), ["HRA 12345678"])
check("a plain number is unchanged", ie.extract_register_numbers("HRB 213220"), ["HRB 213220"])
check("a short number is unchanged", ie.extract_register_numbers("VR 2217"), ["VR 2217"])
# The trap: the next number on the page is an address, not a continuation of the register entry.
check(
    "a postcode after the number is NOT swallowed",
    ie.extract_register_numbers("HRB 12345 49074 Osnabrück"),
    ["HRB 12345"],
)
check(
    "…nor is a four-digit group, which no separator produces",
    ie.extract_register_numbers("HRB 222 4425"),
    ["HRB 222"],
)
check(
    "several entries still come back in order",
    ie.extract_register_numbers("HRB 222 442 und HRB 213220"),
    ["HRB 222442", "HRB 213220"],
)
check("VR-Bank is still not a register entry", ie.extract_register_numbers("VR-Bank eG"), [])

# ---------------------------------------------------------------------------------------
# Structured data: the business card a page publishes for search engines
# ---------------------------------------------------------------------------------------
#
# Every one of these shapes is real. The parser must never raise: broken JSON-LD is common, and a
# page with a broken block has to behave exactly like a page with none.

_LD = '<script type="application/ld+json">%s</script>'

check(
    "a plain business is read",
    sd.names(_LD % '{"@type":"LocalBusiness","name":"Zweirad Liebig GmbH","address":{"postalCode":"49074"}}'),
    ["Zweirad Liebig GmbH"],
)
check(
    "the postcode comes out too",
    sd.postcodes(_LD % '{"@type":"LocalBusiness","name":"X GmbH","address":{"postalCode":"49074"}}'),
    ["49074"],
)
# `@graph` is how most content-management plugins publish it, with the business buried among a
# WebSite and a WebPage node.
check(
    "a business inside @graph is found",
    sd.names(
        _LD
        % '{"@graph":[{"@type":"WebSite","name":"Startseite"},{"@type":"Dentist","name":"Neue Zähne GmbH","telephone":"+49 541 1"}]}'
    ),
    ["Neue Zähne GmbH"],
)
# Several hundred schema.org subtypes describe a local business. Matching on the keys a node has
# rather than on a list of type names is what makes `Dentist` and `Bakery` work without naming them.
check(
    "an unlisted subtype still counts",
    sd.names(_LD % '{"@type":"Bakery","name":"Bäckerei Meyer","vatID":"DE123456789"}'),
    ["Bäckerei Meyer"],
)
check(
    "a type given as a list works",
    sd.names(_LD % '{"@type":["Organization","Store"],"name":"Laden GmbH","email":"a@b.de"}'),
    ["Laden GmbH"],
)
check(
    "the legal name is collected as well",
    sd.names(_LD % '{"@type":"Organization","name":"Meyer","legalName":"Meyer Verwaltungs GmbH","email":"a@b.de"}'),
    ["Meyer", "Meyer Verwaltungs GmbH"],
)
check(
    "a product is not a business",
    sd.names(_LD % '{"@type":"Product","name":"Fahrrad XL","offers":{"price":"499"}}'),
    [],
)
check("malformed JSON is silent", sd.names(_LD % '{"@type":"Organization","name": }'), [])
check("an empty block is silent", sd.names(_LD % "   "), [])
check("no block at all is silent", sd.names("<html><body>nichts</body></html>"), [])
check("empty input is silent", sd.names(""), [])
# Wrappers used to stop old browsers executing the block. All four styles occur.
for _label, _wrapped in [
    ("//<![CDATA[", '//<![CDATA[\n{"@type":"Organization","name":"W GmbH","telephone":"1"}\n//]]>'),
    ("/* <![CDATA[ */", '/* <![CDATA[ */{"@type":"Organization","name":"W GmbH","telephone":"1"}/* ]]> */'),
    ("bare CDATA", '<![CDATA[{"@type":"Organization","name":"W GmbH","telephone":"1"}]]>'),
    ("an HTML comment", '<!--{"@type":"Organization","name":"W GmbH","telephone":"1"}-->'),
]:
    check(f"a block wrapped in {_label} is read", sd.names(_LD % _wrapped), ["W GmbH"])
# Two JSON documents concatenated, or a PHP notice printed after the JSON, is a very common breakage.
check(
    "trailing junk after the JSON is tolerated",
    sd.names(_LD % '{"@type":"Organization","name":"J GmbH","telephone":"1"}<br/>Notice: undefined'),
    ["J GmbH"],
)

# ---------------------------------------------------------------------------------------
# Reading a page: what the rules take from it, and what they refuse to take
# ---------------------------------------------------------------------------------------


class _PageCompany:
    """The fields `pipeline._page_evidence` reads."""

    def __init__(self) -> None:
        self.company_name = "Zweirad Liebig GmbH"
        self.address = "Musterstr. 1, 49074 Osnabrück"
        self.city = "Osnabrück"
        self.postcode = "49074"
        self.registration_number = "Amtsgericht Osnabrück HRB 213220"


_OWN_HOST = "https://zweirad-liebig.de"


def _evidence(html: str, extracted: dict | None = None) -> set[str]:
    got, _seen = pl._page_evidence(_OWN_HOST, _PageCompany(), html=html, extracted=extracted)
    return got


# The register number is often printed in a page footer while the Impressum itself cannot be found —
# the link is an image, or it is behind JavaScript. Reading it with a regex is free; the model used
# to be the only route to it, and the judge gate no longer shows it pages like this.
check_true(
    "a register number in the page text is read without a model",
    conf.REGISTER_MATCH
    in _evidence("<html>Zweirad Liebig GmbH, Osnabrück. Handelsregister HRB 213220</html>"),
)
# Digit boundaries: a short register number must not be found inside a longer number.
check_true(
    "a register number inside a longer number does not count",
    conf.REGISTER_MATCH not in _evidence("<html>Zweirad Liebig GmbH Osnabrück Nr 9213220456</html>"),
)
# Precedence. A number found on the Impressum is the better source, and a MISMATCH found there must
# not be argued away by some other number elsewhere on the page — pages quote other companies.
_mismatch = {"register_number": "HRB 999999", "postcode": None, "postcodes": [], "text_snippet": ""}
_got = _evidence("<html>Zweirad Liebig GmbH Osnabrück HRB 213220</html>", _mismatch)
check_true("an Impressum mismatch stands", conf.REGISTER_MISMATCH in _got)
check_true("and is not overturned by the page text", conf.REGISTER_MATCH not in _got)

# A register number proves a page is ABOUT a company, never that it IS that company's site. What
# makes reading it off the page safe is that `_page_evidence` returns early unless the HOST carries
# the company's name. This is the repo's most repeated bug, so it is checked rather than trusted.
_directory, _ = pl._page_evidence(
    "https://www.northdata.de/Zweirad+Liebig,+Osnabrück/HRB+213220",
    _PageCompany(),
    html="<html>Zweirad Liebig GmbH, Osnabrück, HRB 213220</html>",
    extracted=None,
)
check("a directory gets no evidence beyond being a directory", _directory, {conf.DIRECTORY_HOST})
_other_host, _ = pl._page_evidence(
    "https://irgendwas-anderes.de",
    _PageCompany(),
    html="<html>Zweirad Liebig GmbH, Osnabrück, HRB 213220</html>",
    extracted=None,
)
check("a host that does not carry the name gets nothing either", _other_host, {conf.HOST_NAME_MISMATCH})

# Structured data reaches the score. The name in the JSON-LD is the company's registered name written
# differently — matched on distinctive words, because comparing the strings exactly would fail on
# every German legal-form abbreviation.
_jsonld = _evidence(
    _LD % '{"@type":"Bakery","name":"Zweirad Liebig Gesellschaft mbH","address":{"postalCode":"49074"}}'
)
check_true("a JSON-LD name match becomes evidence", conf.JSONLD_NAME_MATCH in _jsonld)
check_true("so does a JSON-LD postcode match", conf.JSONLD_POSTCODE_MATCH in _jsonld)
check_true(
    "somebody else's business card is not a match",
    conf.JSONLD_NAME_MATCH
    not in _evidence(_LD % '{"@type":"Organization","name":"Ganz Andere Firma AG","telephone":"1"}'),
)

# Fields that were being extracted and thrown away. All three are the "this is a real business site"
# class — none of them says WHICH business, so none may lift the cap.
_fields = {
    "register_number": None,
    "postcode": "49074",
    "postcodes": ["49074"],
    "vat_id": "DE123456789",
    "email": "info@zweirad-liebig.de",
    "phone": "0541 7607780",
    "text_snippet": "",
}
_with_fields = _evidence("<html>x</html>", _fields)
check_true("a VAT id on the page counts", conf.VAT_ID_FOUND in _with_fields)
check_true("an Impressum email at the same domain counts", conf.IMPRINT_EMAIL_ON_DOMAIN in _with_fields)
check_true(
    "an Impressum email elsewhere does not",
    conf.IMPRINT_EMAIL_ON_DOMAIN
    not in _evidence("<html>x</html>", {**_fields, "email": "kontakt@ganz-woanders.de"}),
)
check_true("both postcodes are compared, not just the first",
    conf.POSTCODE_MATCH in _evidence("<html>x</html>", {**_fields, "postcodes": ["12345", "49074"]}))

# Phone numbers are written every possible way, and the parts that differ are at the front.
check_true("the same number written two ways matches", pl._same_phone("+49 541 7607780", "0541/760 77 80"))
check_true("brackets and spacing do not matter", pl._same_phone("(0541) 7607780", "+49 541 7607780"))
check_true("a different number does not match", not pl._same_phone("0541 7607780", "0541 7607781"))
check_true("a number too short to identify a line does not match", not pl._same_phone("12345", "+49 541 12345"))
check_true("a missing number does not match", not pl._same_phone(None, "+49 541 7607780"))

# ---------------------------------------------------------------------------------------
# All five German court registers, and every number on the page
# ---------------------------------------------------------------------------------------
#
# Only HRA/HRB used to be read. An `e. V.` has a `VR` number, a cooperative `eG` a `GnR`, a
# `Partnerschaft mbB` a `PR` — and missing them cost a real website: CF-Selbsthilfe Osnabrück e. V.
# prints `VR 2217` on its Impressum, so the page carried hard proof that nothing could read, and the
# decision was left to a language model that got it wrong.

check("a Handelsregister entry is read", ie.extract_register_numbers("Amtsgericht Osnabrück HRB 213220"), ["HRB 213220"])
check("an association's register entry is read", ie.extract_register_numbers("Vereinsregister VR 2217"), ["VR 2217"])
check("a cooperative's is read", ie.extract_register_numbers("Genossenschaftsregister GnR 155"), ["GnR 155"])
check("a partnership's is read", ie.extract_register_numbers("Partnerschaftsregister PR 712"), ["PR 712"])
check("the -Nr. form is read", ie.extract_register_numbers("HRB-Nr. 12345"), ["HRB 12345"])
check("lower case is read and normalised", ie.extract_register_numbers("hrb 12345"), ["HRB 12345"])
# "VR-Bank" is one of the most common German bank names. The separator class excludes the hyphen so
# it cannot be read as a register number.
check("VR-Bank is not a register number", ie.extract_register_numbers("VR-Bank Osnabrück eG"), [])
check(
    "every entry on the page comes back, in order",
    ie.extract_register_numbers("HRB 16630 ... und HRB 5121 ... sowie VR 2217"),
    ["HRB 16630", "HRB 5121", "VR 2217"],
)
check("a repeated entry appears once", ie.extract_register_numbers("HRB 999 HRB 999"), ["HRB 999"])
check("the single-value helper still answers", ie.extract_register_number("HRB 213220"), "HRB 213220")
check("and returns None when there is none", ie.extract_register_number("nothing here"), None)

# Which register, not just which number — the five registers number independently.
check("the register and the number are separated", conf.register_parts("Amtsgericht Osnabrück HRB 213220"), ("HRB", "213220"))
check("an association's is too", conf.register_parts("VR 2217"), ("VR", "2217"))
check("a bare number has no register", conf.register_parts("213220"), ("", "213220"))
check("nothing in, nothing out", conf.register_parts(None), ("", ""))

# The false mismatch this fixed. Felix Schoeller's Impressum prints 16630 and 5121; ours is 16630,
# and reading only the first entry made the page somebody else's and disqualified it.
_multi = {
    "register_number": "HRB 5121",
    "register_numbers": ["HRB 5121", "HRB 16630"],
    "postcode": None,
    "postcodes": [],
    "text_snippet": "",
}


class _RegCompany(_PageCompany):
    def __init__(self, register: str) -> None:
        super().__init__()
        self.registration_number = register


def _reg_evidence(register: str, extracted: dict) -> set[str]:
    got, _seen = pl._page_evidence(_OWN_HOST, _RegCompany(register), html="<html>x</html>", extracted=extracted)
    return got


_ours_second = _reg_evidence("Amtsgericht Osnabrück HRB 16630", _multi)
check_true("our entry is found even when it is not the first", conf.REGISTER_MATCH in _ours_second)
check_true("and that is not also a mismatch", conf.REGISTER_MISMATCH not in _ours_second)
# Still a mismatch when none of the entries is ours.
check_true(
    "a page listing only other companies is still a mismatch",
    conf.REGISTER_MISMATCH in _reg_evidence("Amtsgericht Osnabrück HRB 777777", _multi),
)
# A different register is not a mismatch. `VR 2217` on the page of an HRB company says nothing about
# that company either way, and a mismatch disqualifies — too strong a conclusion to draw.
_other_register = {**_multi, "register_number": "VR 2217", "register_numbers": ["VR 2217"]}
_got_other = _reg_evidence("Amtsgericht Osnabrück HRB 16630", _other_register)
check_true("an unrelated register is not a mismatch", conf.REGISTER_MISMATCH not in _got_other)
check_true("and not a match either", conf.REGISTER_MATCH not in _got_other)
# Same digits in a different register is a coincidence, not a match.
_same_digits = {**_multi, "register_number": "VR 16630", "register_numbers": ["VR 16630"]}
check_true(
    "the same number in a different register is not a match",
    conf.REGISTER_MATCH not in _reg_evidence("Amtsgericht Osnabrück HRB 16630", _same_digits),
)

# Reading the entry off the page text, when no Impressum could be found, needs the whole entry —
# never the bare digits. Not hypothetical: Bäko-Osnabrück's register entry is `GnR 205`, and
# searching a page for "205" hits a price, a house number or part of a phone number. That awarded
# hard proof, worth 0.45 and able to overrule a disqualifier, on a coincidence.
_bare = pl._page_evidence(
    _OWN_HOST,
    _RegCompany("Amtsgericht Osnabrück GnR 205"),
    html="<html>Zweirad Liebig GmbH, Osnabrück. Artikel 205 kostet 205 Euro.</html>",
    extracted=None,
)[0]
check_true("bare digits in the page text are not a register match", conf.REGISTER_MATCH not in _bare)
check_true(
    "the whole entry in the page text is",
    conf.REGISTER_MATCH
    in pl._page_evidence(
        _OWN_HOST,
        _RegCompany("Amtsgericht Osnabrück GnR 205"),
        html="<html>Zweirad Liebig GmbH, Osnabrück. Genossenschaftsregister GnR 205.</html>",
        extracted=None,
    )[0],
)
check_true(
    "and a different register in the page text is not",
    conf.REGISTER_MATCH
    not in pl._page_evidence(
        _OWN_HOST,
        _RegCompany("Amtsgericht Osnabrück GnR 205"),
        html="<html>Zweirad Liebig GmbH, Osnabrück. Handelsregister HRB 205.</html>",
        extracted=None,
    )[0],
)

# ---------------------------------------------------------------------------------------
# A domain that does not carry the name can still be the company's
# ---------------------------------------------------------------------------------------
#
# Measured over 1,000 companies: 3 pages refused on their domain declared the company in their
# structured data. That is not a guess about the domain, so it lifts the refusal — but only puts the
# row back in the running, and only for a real name mismatch, never for a directory listing.

_ORG = _LD % '{"@type":"Organization","name":"Zweirad Liebig GmbH","telephone":"+49 541 1"}'

_lifted, _ = pl._page_evidence("https://ganz-anderer-name.de", _PageCompany(), html=_ORG, extracted=None)
check_true("the domain is still recorded as not carrying the name", conf.HOST_NAME_MISMATCH in _lifted)
check_true("but the structured data is read anyway", conf.JSONLD_NAME_MATCH in _lifted)
check("and the refusal no longer stands", conf.standing_disqualifiers(_lifted), set())
check("so the row is not disqualified", conf.score(methods=[conf.METHOD_OSM], evidence=_lifted).disqualified_because, None)

# A listing page's structured data names the company it is listing. This is the repo's oldest trap
# wearing a new hat, so the evidence is never emitted there.
_listing, _ = pl._page_evidence(
    "https://irgendein-portal.de/firma/zweirad-liebig", _PageCompany(), html=_ORG, extracted=None
)
check_true("a listing page gets no structured-data credit", conf.JSONLD_NAME_MATCH not in _listing)
check("so its refusal stands", conf.standing_disqualifiers(_listing), {conf.HOST_NAME_MISMATCH})
# A known directory host returns before any of this and stays refused.
_dir, _ = pl._page_evidence(
    "https://www.northdata.de/Zweirad+Liebig", _PageCompany(), html=_ORG, extracted=None
)
check("a directory host is untouched by structured data", _dir, {conf.DIRECTORY_HOST})
check("and nothing lifts it", conf.standing_disqualifiers(_dir), {conf.DIRECTORY_HOST})
# An email domain must keep lifting the same refusal — it was there first.
check(
    "an OSM email domain still lifts the name refusal",
    conf.standing_disqualifiers({conf.HOST_NAME_MISMATCH, conf.OSM_EMAIL_DOMAIN}),
    set(),
)
# And a register match must keep lifting the industry-domain refusal.
check(
    "a register match still lifts an industry domain",
    conf.standing_disqualifiers({conf.GENERIC_HOST, conf.REGISTER_MATCH}),
    set(),
)

if failures:
    print(f"\n{len(failures)} FAILED:\n")
    print("\n".join(failures))
    sys.exit(1)
print("all website-discovery checks passed")
