# How website discovery decides a page belongs to a company

Discovery guesses domains from a company's name, downloads what answers, and decides whether each
page is that company's own website. Finding is easy. **Deciding is what matters**, and this file is
about deciding. The code is in `airflow/dags/enrichment/` (`confidence.py`, `own_site.py`,
`pipeline.py`).

> The numbers below were measured on the Osnabrück example data (1,000 companies, August 2026), which
> has been removed. The rules still hold; Berlin numbers replace them as they are measured.

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

On the Osnabrück data, 53% of companies were `dissolved` (erloschen) and returned 4% of the websites.
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
  matches to a company. Each URL is checked exactly like an OSM `website` tag
  (`pipeline.verify_proposed`, method `wikidata`, the OSM base); nothing on the page means below the
  bar. Measured on a copy of the Berlin database on 2026-10-30: 531 companies matched (31 by register
  number and name), 229 websites proven (209 by the register number on the page), 139 of them for
  companies discovery had found nothing for; 57 agreed with discovery's URL, 34 named another one.
- **Common Crawl** (DAG `common_crawl`, #56) reads Impressum pages from the public web archive and
  proposes a host to the company whose register number its Impressum names. A register number proves
  the page is about the company, so each host is checked again like a guessed domain (method
  `common_crawl`, the URL-pattern base, not the OSM one: no curated record proposed it). Pilot on
  2026-10-30, 5,000 of 33,465 Impressum hosts of one index file: 57 hosts for companies without a
  website, 21 companies proven, all by the own register number; 7 of them discovery had checked and
  missed. Refused: institute subdomains, a film distributor's film sites, a newspaper's subscription shop.
  End to end on a copy of the Berlin database (CC-MAIN-2026-43, 2026-10-30): 640 of 38,118 `.de`
  index blocks, 6,706 Impressum pages, 154 proposals, 50 companies proven (42 had no website); all
  by the own register number. Roughly 3,000 per crawl if the rest of `.de` is alike.
  Shown websites went from 6,015 to 6,229. No result was hand-checked. The page, not the match,
  proves a website: an item that stops matching keeps its record, inactive, and is still checked
  every 90 days. A URL drops to 0 only when no Wikidata record of that company still proves it
  (`proven_urls` in the record): its page stopped naming the company, or its item now matches
  another company.
- **OpenStreetMap** was removed on 2026-10-30 (#57): the public overpass-api.de refused our
  requests, and it found 3 websites in 1,000 active Berlin companies. Its scoring stays in
  `confidence.py` (Wikidata starts from the same base, and stored rows keep their evidence).

## Not known yet

**No website has been hand-checked.** Without labelled rows nobody can say how many promoted websites
are right; comparing two versions only measures agreement. The offline cases in
`scripts/check_website_discovery.py` cover the rules above, not real-world precision.
