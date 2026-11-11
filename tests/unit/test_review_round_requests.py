"""`review_round_requests` (#190): a request the app wrote becomes a round, or a failure the app can show."""
from __future__ import annotations

from review_round_requests.main import draw_request

REQUEST = ("req-1", "website", "gap", "", 200, "measure", "")


class Cursor:
    def __init__(self) -> None:
        self.sql: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.sql.append((" ".join(sql.split()), params))


def test_a_drawn_request_points_at_its_round_and_empty_form_fields_are_none() -> None:
    seen = {}

    def fake_draw(cur, **kwargs):  # noqa: ANN001, ANN202
        seen.update(kwargs)
        return "round-1", 200

    cur = Cursor()
    assert draw_request(cur, REQUEST, fake_draw) == "drawn"
    assert seen == {"field": "website", "kind": "gap", "source": None, "size": 200, "mode": "measure", "title": None}
    assert cur.sql[-1] == ("UPDATE public.review_round_requests SET status = 'drawn', round_id = %s, done_at = now() "
                           "WHERE id = %s", ("round-1", "req-1"))


def test_a_refused_draw_is_rolled_back_and_its_reason_kept() -> None:
    def refuse(cur, **kwargs):  # noqa: ANN001, ANN202
        cur.execute("INSERT INTO public.review_rounds ...")  # half a round, to be undone
        raise ValueError("nothing to draw for website · gap · None")

    cur = Cursor()
    assert draw_request(cur, REQUEST, refuse) == "failed"
    statements = [sql for sql, _ in cur.sql]
    assert statements.index("ROLLBACK TO SAVEPOINT draw_request") > statements.index("INSERT INTO public.review_rounds ...")
    assert cur.sql[-1][1] == ("nothing to draw for website · gap · None", "req-1")


def test_no_role_is_named_admin_because_hasura_never_checks_its_permissions() -> None:
    """Hasura treats the role `admin` as its superuser: a Keycloak realm role of that name would bypass every
    permission (measured locally, 2026-11-11: it could set a request's status). The rounds use `manager`."""
    import json
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    tables = [yaml.safe_load(path.read_text()) for path in (root / "hasura/metadata/databases/default/tables").glob("*.yaml")]
    roles = {permission["role"] for table in tables if isinstance(table, dict)  # tables.yaml is the list of includes
             for kind, permissions in table.items() if kind.endswith("_permissions") for permission in permissions}
    realm = json.loads((root / "keycloak/datahub-realm.json").read_text())
    assert "manager" in roles and "admin" not in roles
    assert "admin" not in {role["name"] for role in realm["roles"]["realm"]}
