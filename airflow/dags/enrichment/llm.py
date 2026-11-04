"""Technologiefeld labels from a large language model, as a teacher for the text classifier (#87).

WHAT IT IS FOR

Most companies with a website get no Technologiefeld: none of the keywords fired (`no_keyword`).
An LLM reads the same text the keywords read and names one of Berlin's 77 fields, or `NONE`. Its
label is a **teacher label**: stored in the source record, never as an observation, so it never
reaches the app. It counts only after people have checked a sample (rule 7: agreement is not
correctness).

WHAT IS SENT (TSB AI rules; it-compliance's OK confirmed 2026-11-04)

The services pages and the homepage (`branch_textcat.classifier_text`), never the Impressum, at most
`MAX_CHARS` characters. Before sending, `redact` replaces every span spaCy's German model tags as a
person, and every email address and phone number, with a placeholder.

HOW (Claude API)

- Message Batches: asynchronous, half the price; a run waits for its batch.
- Structured output: a JSON object whose one field is an enum of the 77 English names plus `NONE`, so
  every answer parses.
- The system prompt (fields and their keywords) is identical for every request and cached.
- `PROMPT_VERSION` is a hash of the system prompt, stored with every label next to the model, so a
  label always says what made it (TSB rule 4).

No database here; `llm_labels` does the I/O. A self-hosted model would replace `submit` and
`results` and keep the rest.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable, Iterable, Iterator

from enrichment import branch_classifier as bc

PROVIDER = "anthropic"
#: Characters of page text per company, as `branch_textcat.MAX_CHARS` reads.
MAX_CHARS = 5000
#: Thinking counts towards it; at effort "low" a one-word answer needs far less.
MAX_TOKENS = 2048
EFFORT = "low"

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PHONE = re.compile(r"(?:\+|\b0)\d[\d /().-]{6,}\d")

_INSTRUCTIONS = """\
You classify Berlin companies into one of Berlin's 77 technology fields (Technologiefelder), using
the text of the company's own website (German or English). Answer with the field the company itself
WORKS in: what it makes, develops or offers. Not its customers' fields, not a field it only mentions.

Answer NONE when the text does not show what the company does, when it shows a business none of the
fields covers (for example a restaurant, a law firm, a property manager), or when you are unsure.
NONE is better than a guess.

Names, email addresses and phone numbers were replaced by [NAME], [EMAIL] and [PHONE].

The fields: the English name you answer with, the German name, and words that typically show it.
"""


def system_prompt(taxonomy: bc.Taxonomy) -> str:
    """The instructions and every field with its keywords, in a fixed order, so the cache holds."""
    lines = [
        f"- {meta['name_en']} (German: {name}): {', '.join(sorted(meta['keywords']))}"
        for name, meta in sorted(taxonomy.branches.items(), key=lambda item: item[1]["name_en"])
    ]
    return _INSTRUCTIONS + "\n" + "\n".join(lines)


def prompt_version(system: str) -> str:
    return hashlib.sha256(system.encode("utf-8")).hexdigest()[:12]


def answers(taxonomy: bc.Taxonomy) -> list[str]:
    """What the model may answer: the 77 English names and `NONE`."""
    return [*taxonomy.english_names(), bc.NONE]


def redact(text: str, person_spans: Callable[[str], Iterable[tuple[int, int]]]) -> str:
    """The text cut to `MAX_CHARS`, with people's names, emails and phone numbers replaced.

    `person_spans` gives (start, end) of each person's name in the text (spaCy's PER entities in the
    DAG); a parameter so this stays testable without the model. Emails and phones are replaced
    before the cut, so one crossing `MAX_CHARS` is never sent half.
    """
    text = _PHONE.sub("[PHONE]", _EMAIL.sub("[EMAIL]", text))[:MAX_CHARS]
    for start, end in sorted(person_spans(text), reverse=True):
        text = text[:start] + "[NAME]" + text[end:]
    return text


def request(custom_id: str, text: str, *, model: str, system: str, choices: list[str]) -> dict[str, Any]:
    """One batch request: the cached system prompt, the company's text, an answer from `choices`."""
    return {
        "custom_id": custom_id,
        "params": {
            "model": model,
            "max_tokens": MAX_TOKENS,
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": f"Website text:\n\n{text}"}],
            "output_config": {
                "effort": EFFORT,
                "format": {
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {"field": {"type": "string", "enum": choices}},
                        "required": ["field"],
                        "additionalProperties": False,
                    },
                },
            },
        },
    }


def label_of(stop_reason: str | None, texts: list[str], choices: list[str]) -> tuple[str | None, str]:
    """(label, outcome) of one answer. Outcome: `labelled`, `refused`, `truncated` or `unparsable`."""
    if stop_reason == "refusal":
        return None, "refused"
    if stop_reason == "max_tokens":
        return None, "truncated"
    try:
        field = json.loads(texts[0])["field"]
    except (IndexError, ValueError, KeyError, TypeError):
        return None, "unparsable"
    return (field, "labelled") if field in choices else (None, "unparsable")


def usage_tokens(usage: Any) -> int:
    """Every token a request was billed for: input, cache writes and reads, output."""
    return sum(getattr(usage, name, 0) or 0 for name in (
        "input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens"))


# --- Claude API ----------------------------------------------------------------------------------


def _client() -> Any:
    import anthropic

    return anthropic.Anthropic()  # reads ANTHROPIC_API_KEY


def submit(requests: list[dict[str, Any]]) -> str:
    return _client().messages.batches.create(requests=requests).id


def ended(batch_id: str) -> bool:
    return _client().messages.batches.retrieve(batch_id).processing_status == "ended"


def results(batch_id: str, choices: list[str]) -> Iterator[tuple[str, str | None, str, int]]:
    """(custom_id, label, outcome, tokens) per request. A request that errored or expired has
    outcome `errored` or `expired` and is sent again by the next run."""
    for item in _client().messages.batches.results(batch_id):
        if item.result.type != "succeeded":
            yield item.custom_id, None, item.result.type, 0
            continue
        message = item.result.message
        texts = [block.text for block in message.content if block.type == "text"]
        label, outcome = label_of(message.stop_reason, texts, choices)
        yield item.custom_id, label, outcome, usage_tokens(message.usage)
