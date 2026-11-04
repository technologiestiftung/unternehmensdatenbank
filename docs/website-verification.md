# How website discovery decides a page belongs to a company

Discovery guesses domains from a company's name, downloads what answers, and decides whether each
page is that company's own website. Finding is easy. **Deciding is what matters**, and this file is
about deciding. The code is in `airflow/dags/enrichment/` (`confidence.py`, `own_site.py`,
`pipeline.py`).

> The numbers below were measured on 1,000 companies of an earlier example data set (August 2026),
> since replaced by the Berlin data. The rules still hold; Berlin numbers replace them as they are measured.

Words used below:

- **candidate**: a URL some step proposed for a company.
- **evidence**: a named fact about a candidate, like `company_name_on_page`. `confidence.py` turns a
  set of them into one number between 0 and 1.
- **promotable**: scored at or above **0.35**, the bar for being reported as a website. A URL that was
  never downloaded is capped at 0.34.

## The two rules everything else follows

1. **A register number on a page proves the page is *about* a company, never that it *is* that
   company's site.** Registry pages, insolvency trackers and directories all quote register numbers.
   `own_site.is_own_site_candidate` is the single place that decides "could this be its own site".
   A looser check anywhere else is the recurring bug.
2. **"A real business site" is not "THAT company's site."** Every German business site has an
   Impressum, a VAT number and a contact email. That evidence may add score, but only
   `confidence.identifies_company` can make a page the company's website. It asks for one of:
   - the company's own register number on the page,
   - a Geschäftsführer named on the page who is an officer in the register,
   - the company's name AND its postcode, together.

   When the Impressum alone still counted as verification, 190 wrong websites were promoted.

**Genericness is measured, never listed.** `name_frequency.common_words` counts which words are common
in company names, `common_email_domains` which email domains are shared. Never grow a list of bad
domains or words by hand.

## Most companies are not worth asking about

On the earlier example data, 53% of companies were `dissolved` (erloschen) and returned 4% of the websites.
`DISCOVERY_ACTIVE_ONLY=true` skips every company whose register status is not `active`; for Berlin it is
on. A dissolved company's domain is the classic resold-domain case, so a website found for one deserves
extra suspicion.

## Reading the page

- **All five registers, every entry.** `HRA`/`HRB` (Handelsregister), `VR` (e. V.), `GnR` (eG), `PR`
  (Partnerschaft). A holding company's Impressum often prints two entries; reading only the first made
  the company's own number look like somebody else's. Fixing that alone recovered 5 websites at 0.99.
- **The register has to match, not just the number.** The five registers number independently, so
  `VR 213220` and `HRB 213220` are different entities. A number from a different register is neither
  a match nor a mismatch.
- **Never search a page for bare digits.** `GnR 205` searched as "205" finds a price or a house number.
- **A number with a space** (`HRB 794 28`) is also read as `HRB 79428`, but only a 1–3 digit tail is
  joined, and the joined number counts only if it is exactly the company's own.
- **A Berlin number with its B glued on** (`HRB 132889B`) is read as `HRB 132889` (#89). Before, such
  a page could never prove its company: 4 of the 91 stored pages with a register entry write it so.
- **A brand before the legal form** ("3pc GmbH Neue Kommunikation") is guessed first
  (`url_patterns.brand_stem`). A host that *equals* the brand carries the name; one that merely
  contains it does not.
- **Former names are guessed too** (#55), up to 3 per company (`pipeline.MAX_FORMER_NAMES`): 32,170
  Berlin companies have one, and a domain often keeps the old name. The domain is judged against the
  name it was guessed from; the page must still identify the company as it is today. Measured on
  1,000 active Berlin companies with a former name (2026-10-30): 113 websites proven, 5 of them only
  through a former name (all 5 checked: 2 by the own register number, 1 by a Geschäftsführer, 2
  plausible), for 76% more page loads on these companies. All 5 came from the first former name;
  trying only one would save 16% of the extra loads.
- **Structured data (JSON-LD)** is the only place a site states its own name as data
  (`structured_data.py`). It may lift the "host does not match the name" refusal
  (`confidence._LIFTED_BY`), but never on a listing page: a listing's structured data names the
  company it lists.
- **A lifted refusal must not stop the page being read.** Otherwise the company's own register number
  further down the page is never seen.
- **Email** and **VAT ID** only say "a real business site" and cannot lift the cap.

## The guesses discovery missed (#89)

On play (2026-11-03), Wikidata and Common Crawl proved 110 websites of active companies that
discovery never found. Discovery had reached all of them; for 89 it never guessed the domain. Three
rules, added after every older guess so those keep their rank, on `.de` only (97 of the 110 are
`.de`), at most `MAX_EXTRA_CANDIDATES` (12) per company:

| rule | example | misses it adds (of 110, each on top of the first row) |
|---|---|---|
| legal forms and owner clauses removed (`gGmbH`, `gUG`, `eG`, "gemeinnützige", "Inhaber …") | `aktiongegendenhunger.de` | 3 |
| single letters and digits kept | `8greenbottles.de`, `a-zelte.de` | +4 |
| runs of 1–3 name words from anywhere in the name; a run of only common words never | `orbitalsystems.de`, `technologiestiftung-berlin.de` | +9 |
| learned affixes (`name_frequency.common_affixes`): what at least 8 published websites add to their name, measured each run | `navitas-ggmbh.de`, `akanthusberlin.de` | +3 |

Together: 38 of the 110 guessed instead of 20, and 2,747 instead of 2,729 of 3,000 sampled websites
discovery had found (none lost). The learned affixes on play are `{}-berlin`, `{}-{legal}` (the
company's own legal form: 36 websites) and `{}berlin`.

**Cost and yield**, measured on 1,000 random active companies without a website (play, seed 89):
4.3 more DNS lookups per company (19.6 → 23.9, on 300 of them), 0.36 more page loads (the new guesses only get the
probes the older ones leave), and **8 new websites proven** (0.8%), each by the own register number, a
register officer or name and postcode.

A site found by hand can belong to a sister company: `bebra-wissenschaft.de` prints BeBra Verlag
GmbH, HRB 103407 B, not HRB 51617, the company it was listed for, so verification was right to
refuse it.

Still never guessed: a reordered compound
(`bildungsakademie-berlin.de` for "Akademie für berufliche Weiterbildung"), a brand unrelated to the
name (`visitberlin.de`). These need a source that does not guess: Common Crawl, or a search API (#92).

**Measuring a change:** `scripts/url_discovery_test_set.csv` (register entry and host only; sole traders
left out, because their domain can carry the owner's name) holds 106 of the 110, the websites found
by hand, and 292 of the 300 sampled hits; `scripts/measure_url_guesses.py` reports
how many each group gets guessed. `--draw-hand-sample` draws the 200 companies for the hand search.

## Sites that name the company but prove nothing (#96)

A guessed site whose page carries the company's name, but no register number, no register officer
and no matching postcode, stays at 0.34 and is never shown. Two patterns from the hand search (#89):
the company moved since the 2017–2019 snapshot, so its postcode no longer matches; or the domain does
not carry the name and is refused before its Impressum is read (`tub.co`).

**How many**, local Berlin database, 2026-11-04: **7,014** active companies (sole traders left out)
without a proven website have such a site. On 3,696 the domain is exactly the name, on 1,171 the
Impressum gives an email at the domain, on 1,527 the page's structured data names the company. On play
(2026-11-03) the issue counted 21,641 without these filters.

**Nothing is shown before a hand check.** `scripts/check_unproven_sites.py --draw` draws 50 (the
`review_sample` first, then a fixed order) into a CSV for a person to mark `own_site`, `directory`,
`parent`, `namesake` or `other`; `--score` gives the share of own sites, overall and per flag (for
example exact-name domain plus email at the domain). Only at about 9 in 10 is a shown value
defensible; otherwise these sites stay suggestions in the claims list, which already lists them under
`website` (`v_company_claims`).

**Checked by hand on play, 2026-11-04: 2 of 40 are the company's own site (5%).** 50 drawn, 10 not
judgeable (5 parked or broken, 5 without enough on the page). Of the 40: 2 own sites, 29 another
business with the same name (about 16 abroad: US, Canada, Denmark, Austria, Vietnam, Dubai; about 10
in another German city), 5 a parent or group company, 4 another organisation. No flag lifts it near the
bar: exact-name domain 2 of 21, exact-name domain plus email at the domain 1 of 5. A model suggested
verdicts first; a person checked every row, and only the person's verdicts are counted.

**Decision: these sites stay hidden, as today.** Neither shown nor offered as a suggestion. Instead,
the signals against a site get sharper, so the wrong ones drop out of the 0.34 pool (#111, #112, #113):
an Impressum abroad, register numbers we cannot read yet (`HR B 61871`, Swiss `CH-…`), a page that
proves another company in our database, and register numbers we do not have at all. Two right sites
the guesses missed: `hma-berlin.de` (abbreviation plus "berlin", #89) and a "Legal Notice" link
that was never followed (#109).

## Pages are cached, so extraction changes are cheap to try

`COMBINED_PAGE_CACHE_DIR` turns on a disk cache of raw HTML and status, for measurement runs. Inside
Airflow the cache is the company corpus in S3 instead: a page that loaded is served for 90 days, a failed
download is not kept (`enrichment/page_cache.py`). Only raw pages are cached, never conclusions, so a
change to extraction applies on the next read. About 150–300 MB per 1,000 companies, under the git-ignored `airflow/data/`; cached
Impressum pages carry managing-director names.

## Lessons that cost real time

- **A gate that looks free can hide lost rejections.** Count lost discoveries *and* lost demotions.
- **A model's stated reason is not evidence.** A former LLM judge once "quoted" a register number that
  appears nowhere on the page; it had been given the number in its prompt. The judge stage has since
  been removed.
- **Search-engine discovery was measured and removed** (paid APIs, SearXNG, Ollama). Free scraped search
  fails from this network on CAPTCHAs within a dozen queries. A new search source must feed the same
  verification and `confidence.py`, never a scorer of its own.
- **Wikidata** (DAG `wikidata`, #26) proposes the official website (P856) of Berlin organisations it
  matches to a company. Each URL is checked like a guessed domain
  (`pipeline.verify_proposed`, method `wikidata`, the curated-record base `BASE_OSM`, named after the
  removed OSM step); nothing on the page means below the
  bar. Measured on a copy of the Berlin database on 2026-10-30: 531 companies matched (31 by register
  number and name), 229 websites proven (209 by the register number on the page), 139 of them for
  companies discovery had found nothing for; 57 agreed with discovery's URL, 34 named another one.
  Shown websites went from 6,015 to 6,229. No result was hand-checked. The page, not the match,
  proves a website: an item that stops matching keeps its record, inactive, and is still checked
  every 90 days. A URL drops to 0 only when no Wikidata record of that company still proves it
  (`proven_urls` in the record): its page stopped naming the company, or its item now matches
  another company.
- **Common Crawl** (DAG `common_crawl`, #56) reads Impressum pages from the public web archive and
  proposes a host to the company whose register number its Impressum names. A register number proves
  the page is about the company, so each host is checked again like a guessed domain (method
  `common_crawl`, the URL-pattern base, not the OSM one: no curated record proposed it). Pilot on
  2026-10-30, 5,000 of 33,465 Impressum hosts of one index file: 57 hosts for companies without a
  website, 21 companies proven, all by the own register number; 7 of them discovery had checked and
  missed. Refused: institute subdomains, a film distributor's film sites, a newspaper's subscription shop.
  End to end on a copy of the Berlin database (CC-MAIN-2026-43, 2026-10-30): 640 of 38,118 `.de`
  index blocks, 6,706 Impressum pages, 154 proposals, 50 companies proven (42 had no website); all
  by the own register number. Roughly 3,000 per crawl if the rest of `.de` is alike. A page naming
  more than 2 of our companies is a group page and proposes nothing: in that run, pages naming 1
  company gave 106 proposals (47 proven), 2 gave 12 (3 proven: a GmbH & Co. KG and its
  Verwaltungs-GmbH, e.g. abion-hotel.de), and one page naming 26 gave 26 (0 proven).
- **Wirtschaftsatlas** (DAG `wirtschaftsatlas`, #105) proposes the website Berlin Partner's map lists
  for a company matched by name and postcode (or by a name only one live company has). A name is not
  an identifier, so each URL starts from the URL-pattern base (method `wirtschaftsatlas`) and only
  the page can prove it. Started by hand only, while Berlin Partner's consent is asked. Local run on
  2026-11-04: 7,625 entries, 2,253 matched; of the first 50 websites checked, 21 proven (18 by the
  register number, 3 by an officer), 15 for companies without a website. No result was hand-checked.
- **OpenStreetMap** was removed on 2026-10-30 (#57): the public overpass-api.de refused our
  requests, and it found 3 websites in 1,000 active Berlin companies. Its scoring stays in
  `confidence.py` (Wikidata starts from the same base, and stored rows keep their evidence).

## Not known yet

**No website has been hand-checked.** Without labelled rows nobody can say how many promoted websites
are right; comparing two versions only measures agreement. The offline cases in
`scripts/check_website_discovery.py` cover the rules above, not real-world precision.
