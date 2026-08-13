"""Guard the two hand-written parts of combined discovery: URL guessing and confidence.

Free to run. No search API, no language model, no database, no network — every case here is a
string in, a value out. Run it after touching `url_patterns.py` or `confidence.py`:

    python scripts/check_combined_discovery.py

Same idea as `scripts/check_own_site_filter.py`: the rules these two modules encode are
judgement calls that look arbitrary six months later, so each case says what it is protecting
against, and a failure prints the case rather than just a line number.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

from combined_discovery import confidence as conf  # noqa: E402
from combined_discovery import llm_judge as lj  # noqa: E402
from combined_discovery import name_frequency as nf  # noqa: E402
from combined_discovery import url_patterns as up  # noqa: E402

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

# The host has to land in the same alphabet as the name, or none of the above counts for anything.
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
alone = conf.score(
    methods=[conf.METHOD_OSM], evidence={conf.IMPRINT_FOUND}, name_uniqueness=0.5
)
together = conf.score(
    methods=[conf.METHOD_OSM, conf.METHOD_URL_PATTERN],
    evidence={conf.IMPRINT_FOUND, conf.AGREE_TWO},
    name_uniqueness=0.5,
)
check_true("agreement raises confidence", together.value > alone.value)
check("agreement is named as the reason", together.verified_by, "method_agreement")

# Three-way agreement replaces the two-way bonus rather than stacking with it.
three = conf.score(
    methods=[conf.METHOD_OSM, conf.METHOD_URL_PATTERN, conf.METHOD_LOCAL_LLM],
    evidence={conf.IMPRINT_FOUND, conf.AGREE_TWO, conf.AGREE_THREE},
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
check_true("and can be promoted", email_site.promotable)

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
# The judge's own trust rules, tested without a model or a network
# ---------------------------------------------------------------------------------------


class _FakeCompany:
    """Just the four fields `llm_judge.flags_for` reads."""

    def __init__(self, register: str | None, postcode: str | None) -> None:
        self.company_name = "Zweirad Liebig GmbH"
        self.address = "Musterstr. 1"
        self.city = "Osnabrück"
        self.postcode = postcode
        self.registration_number = register


def _result(**kwargs: object) -> "lj.JudgeResult":
    return lj.JudgeResult(**kwargs)  # type: ignore[arg-type]


company = _FakeCompany("Amtsgericht Osnabrück HRB 213220", "49080")
page_with_number = "Impressum. Zweirad Liebig GmbH. Amtsgericht Osnabrück HRB 213220. 49080."
page_without = "Impressum. Zweirad Liebig GmbH. Osnabrück."

# The model is TOLD the company's register number in the prompt, so it can hand it straight back
# for a page that never carried one. Without the on-page check that manufactures hard proof out of
# nothing — and hard proof is the one thing that overrules a disqualifier.
check_true(
    "a register number the page really carries counts",
    conf.REGISTER_MATCH
    in lj.flags_for(
        _result(verdict=lj.VERDICT_OWN_SITE, register_number="HRB 213220"),
        company,
        page_text=page_with_number,
    ),
)
check_true(
    "a register number the page does NOT carry is refused",
    conf.REGISTER_MATCH
    not in lj.flags_for(
        _result(verdict=lj.VERDICT_OWN_SITE, register_number="HRB 213220"),
        company,
        page_text=page_without,
    ),
)

# The model saying "this page is about the company" IS the statement that the number is quoted, so
# it must not hand out a register match at the same time. Both halves of the rule are needed: this
# stops the model manufacturing proof, and the check in `confidence.score` stops the regex
# extraction doing it on a page the model has flagged.
check_true(
    "a page the model calls a directory gets no register match",
    conf.REGISTER_MATCH
    not in lj.flags_for(
        _result(verdict=lj.VERDICT_ABOUT_COMPANY, register_number="HRB 213220", postcode="49080"),
        company,
        page_text=page_with_number,
    ),
)
check_true(
    "nor a postcode match",
    conf.POSTCODE_MATCH
    not in lj.flags_for(
        _result(verdict=lj.VERDICT_ABOUT_COMPANY, register_number="HRB 213220", postcode="49080"),
        company,
        page_text=page_with_number,
    ),
)

# A mismatch disqualifies a URL outright, so the model is never allowed to declare one. One misread
# digit would otherwise throw away a correct website.
check_true(
    "the model can never produce a register MISMATCH",
    conf.REGISTER_MISMATCH
    not in lj.flags_for(
        _result(verdict=lj.VERDICT_OWN_SITE, register_number="HRB 999999"),
        company,
        page_text="Impressum. Irgendeine Firma. HRB 999999.",
    ),
)

# Postcodes go through the same door.
check_true(
    "a postcode on the page counts",
    conf.POSTCODE_MATCH
    in lj.flags_for(
        _result(verdict=lj.VERDICT_OWN_SITE, postcode="49080"), company, page_text=page_with_number
    ),
)
check_true(
    "a postcode the page lacks does not",
    conf.POSTCODE_MATCH
    not in lj.flags_for(
        _result(verdict=lj.VERDICT_OWN_SITE, postcode="49080"), company, page_text=page_without
    ),
)

# A short register number must not be found inside a longer one. "882" lives inside "172907"?
# No — but it does live inside "18829", and a substring test would match it.
check_true(
    "a register number is not matched inside a longer number",
    not lj._number_is_on_page("882", "Telefon 018829 44 55"),
)
check_true("and is matched when it stands alone", lj._number_is_on_page("882", "HRB 882, Osnabrück"))

# A failed or empty model call contributes nothing whatsoever.
check("a model error contributes no evidence", lj.flags_for(_result(error="boom"), company, page_text=page_with_number), set())
check(
    "an unsure verdict contributes no evidence",
    lj.flags_for(_result(verdict=lj.VERDICT_UNSURE), company, page_text=page_with_number),
    set(),
)

# The judge must never be recorded as a METHOD. It read the page the guesser found, so counting it
# as a second opinion would hand every judged row the two-methods-agree bonus.
check_true(
    "the judge is not one of the proposing methods",
    conf.METHOD_LOCAL_LLM not in lj._VERDICT_FLAGS.values(),
)

# ---------------------------------------------------------------------------------------

if failures:
    print(f"\n{len(failures)} FAILED:\n")
    print("\n".join(failures))
    sys.exit(1)
print("all combined-discovery checks passed")
