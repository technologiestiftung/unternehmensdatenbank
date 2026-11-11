"""`review_round_requests` (#190): a request the app wrote becomes a round, or a failure the app can show."""
from __future__ import annotations

import re

from review_round_requests.main import draw_request

REQUEST = ("req-1", "website", "gap", "", 200, "measure", "")


class Database:
    """A stand-in for Postgres that keeps what each statement changed, and undoes it on ROLLBACK TO SAVEPOINT."""

    def __init__(self) -> None:
        self.changes: list[tuple] = []
        self.savepoints: dict[str, int] = {}

    def execute(self, sql: str, params: tuple = ()) -> None:
        words = sql.split()
        if words[0] == "SAVEPOINT":
            self.savepoints[words[1]] = len(self.changes)
        elif words[:3] == ["ROLLBACK", "TO", "SAVEPOINT"]:
            del self.changes[self.savepoints[words[3]]:]
        elif words[0] == "INSERT":
            self.changes.append(("insert", words[2]))
        elif words[0] == "UPDATE":
            table, assignments = re.fullmatch(r"UPDATE (\S+) SET (.+) WHERE id = %s", " ".join(words)).groups()
            values = iter(params)
            columns = {}
            for assignment in assignments.split(","):
                column, value = (part.strip() for part in assignment.split("="))
                columns[column] = next(values) if value == "%s" else value.strip("'")
            self.changes.append(("update", table, next(values), columns))
        else:
            raise AssertionError(f"unexpected statement: {sql}")

    def request(self, request_id: str) -> dict:
        row: dict = {}
        for change in self.changes:
            if change[:3] == ("update", "public.review_round_requests", request_id):
                row.update(change[3])
        return row


def test_a_drawn_request_points_at_its_round_and_empty_form_fields_are_none() -> None:
    seen = {}

    def fake_draw(cur, **kwargs):  # noqa: ANN001, ANN202
        seen.update(kwargs)
        return "round-1", 200

    db = Database()
    assert draw_request(db, REQUEST, fake_draw) == "drawn"
    assert seen == {"field": "website", "kind": "gap", "source": None, "size": 200, "mode": "measure", "title": None}
    row = db.request("req-1")
    assert (row["status"], row["round_id"]) == ("drawn", "round-1")
    assert "error" not in row


def test_a_refused_draw_is_rolled_back_and_its_reason_kept() -> None:
    def refuse(cur, **kwargs):  # noqa: ANN001, ANN202
        cur.execute("INSERT INTO public.review_rounds (title) VALUES ('half a round')")
        raise ValueError("nothing to draw for website · gap · None")

    db = Database()
    assert draw_request(db, REQUEST, refuse) == "failed"
    assert ("insert", "public.review_rounds") not in db.changes
    row = db.request("req-1")
    assert (row["status"], row["error"]) == ("failed", "nothing to draw for website · gap · None")
    assert "round_id" not in row


def test_no_role_is_named_admin_because_hasura_never_checks_its_permissions() -> None:
    """Hasura treats the role `admin` as its superuser: a Keycloak realm role of that name would bypass every
    permission (measured locally, 2026-11-11: it could set a request's status)."""
    import json
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    tables = [yaml.safe_load(path.read_text()) for path in (root / "hasura/metadata/databases/default/tables").glob("*.yaml")]
    roles = {permission["role"] for table in tables if isinstance(table, dict)  # tables.yaml is the list of includes
             for kind, permissions in table.items() if kind.endswith("_permissions") for permission in permissions}
    realm = json.loads((root / "keycloak/datahub-realm.json").read_text())
    assert "admin" not in roles
    assert "admin" not in {role["name"] for role in realm["roles"]["realm"]}
