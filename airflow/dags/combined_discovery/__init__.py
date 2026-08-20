"""Cheap website-discovery methods, run together, with one confidence number at the end.

Three methods propose URLs, and a fourth stage judges what they proposed. In the order the
pipeline runs them:

1. **URL pattern matching** (`url_patterns.py`) — guess the domain from the company name,
   then ask the internet whether that domain exists. No API, no key, no bill.
2. **OSM address discovery** (`osm_lookup.py`) — look the company up in OpenStreetMap, the
   free world map anyone can edit. Shops and offices there often carry a `website` tag.
3. **Ollama + SearXNG** (`local_llm.py`) — a search engine you host yourself (SearXNG) plus a
   language model you host yourself (Ollama). Also free, but it needs both services running, and
   the search engines SearXNG forwards to serve a CAPTCHA to office networks. Usually finds
   nothing here.
4. **The judge** (`llm_judge.py`) — a language model you host yourself, reading the pages the
   three methods above already downloaded, answering "is this page this company's own site, or a
   page about it?". It needs Ollama and no search engine at all. It proposes no URLs, so it
   cannot invent a domain: it only ever votes on pages already in hand.

`pipeline.py` runs them, merges what they say, and hands the merged evidence to
`confidence.py`, which is the ONLY place that turns evidence into a number between 0 and 1.
The judge is no exception — it emits named evidence like everything else, and `confidence.py`
decides what that is worth.

Nothing in this package writes to the database. It reads companies, probes the web, and
returns results. Storing them is a separate decision.
"""
