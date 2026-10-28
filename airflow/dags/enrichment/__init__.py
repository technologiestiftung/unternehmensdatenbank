"""The building blocks that fill a company's fields from the open web. No API, no key, no bill.

This package is a library, not a DAG. The DAGs that run it are being rebuilt (issue #18); until
then `manager_ner` is its only caller in Airflow.

Website discovery
  url_patterns.py     guess domains from the company name, check DNS, download the live ones
  osm_snapshot.py,    match the company to OpenStreetMap; shops and offices there often carry a
  osm_lookup.py       `website` tag. `overpass_client.py` and `osm_scorer.py` do the querying
                      and the name matching
  pipeline.py         runs both methods for one company and merges what they say
  confidence.py       the ONLY place that turns evidence into a number between 0 and 1
  own_site.py         the single predicate for "is this page the company's own site", as
                      opposed to a directory or register page ABOUT it
  structured_data.py, evidence read off the page itself; `name_frequency.py` measures which
  name_frequency.py   name words are too common to identify a company
  url_scorer.py       how distinctive a company name is

Reading pages
  http_client.py, imprint_probe.py   find and download the Impressum
  imprint_extractor.py               regex: register number, VAT ID, Geschäftsführer, contact
  page_cache.py, content_pages.py    cache downloaded pages; fetch the services pages

Industry classification (three columns that must never be mixed: docs/branch-classification.md)
  branch_classifier.py, branch_taxonomy.yaml   Technologiefeld from the words a company publishes
  branch_textcat.py                            spaCy text classifier, seeded by the keywords
  purpose_branch.py                            the same 77 fields from the register purpose
  wz_cluster.py, wz_titles.yaml                the official WZ cluster, a lookup

Writing
  observations.py, branch_observations.py   turn results into `company_field_observations`
"""
