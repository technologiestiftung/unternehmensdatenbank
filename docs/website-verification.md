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
- **Structured data (JSON-LD)** is the only place a site states its own name as data
  (`structured_data.py`). It may lift the "host does not match the name" refusal
  (`confidence._LIFTED_BY`), but never on a listing page: a listing's structured data names the
  company it lists.
- **A lifted refusal must not stop the page being read.** Otherwise the company's own register number
  further down the page is never seen.
- **Phone** is scored against the number OpenStreetMap holds for the business. **Email** and **VAT ID**
  only say "a real business site" and cannot lift the cap.

## Pages are cached, so extraction changes are cheap to try

`COMBINED_PAGE_CACHE_DIR` turns on a disk cache of raw HTML and status. It is **off in Airflow**: a DAG
must see today's web. Only raw pages are cached, never conclusions, so a change to extraction applies on
the next read. About 150–300 MB per 1,000 companies, under the git-ignored `airflow/data/`; cached
Impressum pages carry managing-director names.

## Lessons that cost real time

- **A gate that looks free can hide lost rejections.** Count lost discoveries *and* lost demotions.
- **A model's stated reason is not evidence.** A former LLM judge once "quoted" a register number that
  appears nowhere on the page; it had been given the number in its prompt. The judge stage has since
  been removed.
- **Search-engine discovery was measured and removed** (paid APIs, SearXNG, Ollama). Free scraped search
  fails from this network on CAPTCHAs within a dozen queries. A new search source must feed the same
  verification and `confidence.py`, never a scorer of its own.
- **OpenStreetMap** stays in code but is off (`WEBSITE_DISCOVERY_OSM=false`): the public
  overpass-api.de refused our requests, and it found 3 websites in 1,000 active Berlin companies.

## Not known yet

**No website has been hand-checked.** Without labelled rows nobody can say how many promoted websites
are right; comparing two versions only measures agreement. The offline cases in
`scripts/check_website_discovery.py` cover the rules above, not real-world precision.
