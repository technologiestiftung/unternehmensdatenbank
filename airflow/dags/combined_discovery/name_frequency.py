"""Which words name a business, and which only name a trade — measured, not listed.

Three different jobs in this pipeline need the same judgement:

- **Matching a company to a map object.** "K & M Bauunternehmen" and "Bauunternehmen Fennker"
  share a word, and it proves nothing.
- **Believing a guessed domain.** `anlagenbau.de` cannot be inferred to belong to "Anlagenbau &
  Technischer Service Alte Poststraße GmbH", because it describes a trade that many companies
  are in.
- **Believing an email domain.** A business mapped with `kontakt@osnanet.de` is telling you who
  its internet provider is, not what its website is.

All three come down to: does this thing name ONE business, or something many businesses share?

## Why this is measured instead of listed

A hand-written list of industry words was tried first and it does not survive contact with the
data. German glues nouns together, so the list needs `bau`, `anlagenbau`, `tiefbau`, `hochbau`,
`bauunternehmen`, `baugesellschaft` and so on without end. Worse, a list grows by whatever
happened to be in the last sample — which is fitting the sample, not the problem.

Counting is better, because the company table already knows. Run over the 14,443 Osnabrück
companies, words appearing in at least 8 names include:

    immobilien 391   bau 209   service 197   consulting 128   automobile 24   anlagenbau 12

Those are trades. Against that, `judentum`, `african`, `kinokunst` and `herten` each appear in
exactly **one** company name, `uniformen` in two. No frequency rule will ever catch those, and
that is the point: they are not industry words, they are ordinary words that happen to be one
company's whole name. A different rule has to cover them — see `url_patterns.name_coverage`,
which asks how much of the company's name the domain actually accounts for.

So: counting handles industries, coverage handles single words. Neither needs a list of cases.
"""
from __future__ import annotations

import os
from collections import Counter

from combined_discovery import url_patterns

#: How many company names a word has to appear in before sharing it proves nothing. 8 out of
#: 14,443 is deliberately low: the cost of calling a word generic is that a company genuinely
#: named after it needs stronger proof, and the cost of missing one is a wrong website.
MIN_COMPANIES = int(os.getenv("COMBINED_COMMON_WORD_MIN", "8"))

#: The shortest word allowed to identify a business on its own.
MIN_DISTINCTIVE_WORD_LENGTH = 4


def common_words(company_names: list[str], min_companies: int = MIN_COMPANIES) -> set[str]:
    """Words appearing in at least `min_companies` of the names.

    Pass the WHOLE table, not the sample being measured: how common a word is describes the town,
    not the 100 companies that happen to be under the microscope.
    """
    counter: Counter[str] = Counter()
    for name in company_names:
        counter.update(set(url_patterns.name_words(name)))
    return {word for word, count in counter.items() if count >= min_companies}


#: How many DIFFERENT mapped businesses have to share an email domain before it stops identifying
#: any of them. Counted, not listed, for the same reason the words above are.
#:
#: 3 rather than 2. A domain on exactly two objects is usually one business mapped twice — the same
#: cinema entered as a node and as a building. At 3 the rule also drops the domains of chains
#: (`minden.edeka.de`, `combi.buenting.de`), which is right on both readings: a shared domain is
#: either a mail provider, which is useless, or a chain's corporate site, which is a poor answer for
#: one branch.
MIN_BUSINESSES_PER_EMAIL_DOMAIN = int(os.getenv("COMBINED_COMMON_EMAIL_DOMAIN_MIN", "3"))


def email_domain(email: str | None) -> str | None:
    """`info@stallkamp.de` -> `stallkamp.de`. Anything without exactly one @ becomes None.

    Only the part after the @ is ever kept. The part before it is frequently a person's name, and
    these values travel into CSV files that get emailed around.
    """
    if not email or email.count("@") != 1:
        return None
    domain = email.split("@", 1)[1].strip().lower().rstrip(".")
    return domain or None


def common_email_domains(
    records: list[dict], min_businesses: int = MIN_BUSINESSES_PER_EMAIL_DOMAIN
) -> set[str]:
    """Email domains shared by so many mapped businesses that they identify none of them.

    Pass every record in the city snapshot, not the companies being measured — how many businesses
    use a mail host describes the town.

    Why this is counted instead of a list of `gmail.com, gmx.de, web.de, t-online.de`: measured over
    the 2,001-object Osnabrück snapshot, the most common email domain by a wide margin is
    **`osnanet.de` with 23 businesses** — a local internet provider, three times more common here
    than `web.de`. No hand-written list of the obvious national providers would have contained it,
    and the next town's local provider would be different again. 358 of the 384 domains in that
    snapshot belong to exactly one business, so the rule throws away very little.
    """
    counter: Counter[str] = Counter()
    for record in records:
        domain = email_domain(record.get("email"))
        if domain:
            counter[domain] += 1
    return {domain for domain, count in counter.items() if count >= min_businesses}


def distinctive_words(name: str, city: str | None, common: set[str]) -> set[str]:
    """The words of a name that could identify ONE business.

    Dropped: the legal form (by `name_words`), ownership words like "Verwaltungs" (by
    `is_low_signal`), the town — every company here is in the same one — and any word measured
    as common across the table.
    """
    city_words = set(url_patterns.name_words(city or ""))
    return {
        word
        for word in url_patterns.name_words(name)
        if len(word) >= MIN_DISTINCTIVE_WORD_LENGTH
        and not url_patterns.is_low_signal(word)
        and word not in url_patterns.GENERIC_WORDS
        and word not in city_words
        and word not in common
    }
