from __future__ import annotations

import argparse
import json
import mimetypes
import sqlite3
import traceback
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

import find_reference_drug as ref_finder
import synopsis_service
from test_router_adapter import call_test_router_openrouter


DF_CACHE_LOCK = Lock()
DF_CACHE: dict[str, pd.DataFrame] = {}

SESSIONS_LOCK = Lock()
SESSIONS: dict[str, "SearchSession"] = {}

FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"
DB_PATH = Path(__file__).resolve().parent / "pharma_runs.db"
DB_LOCK = Lock()
DOWNLOADS_DIR = Path(__file__).resolve().parent / "downloads"


@dataclass
class SearchSession:
    request_id: str
    created_at: str
    xls_path: str
    query: dict[str, str]
    matched_indices: list[int]
    reference_options: list[dict[str, Any]]


def _init_db() -> None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    mode TEXT NOT NULL,
                    session_id TEXT,
                    query_json TEXT,
                    matches_count INTEGER,
                    reference_options_count INTEGER,
                    selected_reference_drug TEXT,
                    selection_rows_count INTEGER,
                    selection_json TEXT,
                    selection_file_path TEXT,
                    router_output_text TEXT,
                    router_output_path TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS synopsis_runs (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_run_id TEXT NOT NULL,
                    template_path TEXT,
                    prompt_path TEXT,
                    attributes_json TEXT,
                    output_markdown TEXT,
                    output_docx_path TEXT,
                    error_text TEXT
                )
                """
            )
            _ensure_run_columns(conn)
            conn.commit()
        finally:
            conn.close()


def _ensure_run_columns(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(runs)")
    existing = {row[1] for row in cur.fetchall()}
    required = {
        "status": "TEXT NOT NULL DEFAULT 'done'",
        "started_at": "TEXT",
        "finished_at": "TEXT",
        "matches_count": "INTEGER",
        "reference_options_count": "INTEGER",
    }
    for name, ddl in required.items():
        if name in existing:
            continue
        conn.execute(f"ALTER TABLE runs ADD COLUMN {name} {ddl}")


def _insert_run(
    *,
    status: str,
    started_at: str | None,
    finished_at: str | None,
    mode: str,
    session_id: str | None,
    query: dict[str, Any] | None,
    matches_count: int | None,
    reference_options_count: int | None,
    selected_reference_drug: str | None,
    selection_rows_count: int | None,
    selection_payload: dict[str, Any] | None,
    selection_file_path: str | None,
    router_output_text: str | None,
    router_output_path: str | None,
) -> str:
    run_id = uuid.uuid4().hex
    created_at = datetime.now().isoformat(timespec="seconds")
    payload_json = json.dumps(selection_payload, ensure_ascii=False) if selection_payload is not None else None
    query_json = json.dumps(query, ensure_ascii=False) if query is not None else None
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute(
                """
                INSERT INTO runs (
                    id, created_at, status, started_at, finished_at, mode, session_id, query_json,
                    matches_count, reference_options_count, selected_reference_drug, selection_rows_count, selection_json,
                    selection_file_path, router_output_text, router_output_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    created_at,
                    status,
                    started_at,
                    finished_at,
                    mode,
                    session_id,
                    query_json,
                    matches_count,
                    reference_options_count,
                    selected_reference_drug,
                    selection_rows_count,
                    payload_json,
                    selection_file_path,
                    router_output_text,
                    router_output_path,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    return run_id


def _update_run(run_id: str, **fields: Any) -> None:
    if not fields:
        return
    allowed = {
        "status",
        "started_at",
        "finished_at",
        "matches_count",
        "reference_options_count",
        "selected_reference_drug",
        "selection_rows_count",
        "selection_json",
        "selection_file_path",
        "router_output_text",
        "router_output_path",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
    values = list(updates.values()) + [run_id]
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute(f"UPDATE runs SET {set_clause} WHERE id = ?", values)
            conn.commit()
        finally:
            conn.close()


def _row_to_run(row: tuple[Any, ...]) -> dict[str, Any]:
    (
        run_id,
        created_at,
        status,
        started_at,
        finished_at,
        mode,
        session_id,
        query_json,
        matches_count,
        reference_options_count,
        selected_reference_drug,
        selection_rows_count,
        selection_json,
        selection_file_path,
        router_output_text,
        router_output_path,
    ) = row
    query = json.loads(query_json) if query_json else None
    selection_payload = json.loads(selection_json) if selection_json else None
    if selection_payload and (matches_count is None or reference_options_count is None):
        ref_options = selection_payload.get("reference_options") or []
        if matches_count is None:
            try:
                matches_count = sum(int(opt.get("rows_count") or 0) for opt in ref_options)
            except (TypeError, ValueError):
                matches_count = None
        if reference_options_count is None:
            reference_options_count = len(ref_options)
    return {
        "id": run_id,
        "created_at": created_at,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "mode": mode,
        "session_id": session_id,
        "query": query,
        "matches_count": matches_count,
        "reference_options_count": reference_options_count,
        "selected_reference_drug": selected_reference_drug,
        "selection_rows_count": selection_rows_count,
        "selection_payload": selection_payload,
        "selection_file_path": selection_file_path,
        "router_output_text": router_output_text,
        "router_output_path": router_output_path,
    }


def _get_run(run_id: str) -> dict[str, Any] | None:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.execute(
                """
                SELECT id, created_at, status, started_at, finished_at, mode, session_id, query_json,
                       matches_count, reference_options_count,
                       selected_reference_drug, selection_rows_count, selection_json, selection_file_path,
                       router_output_text, router_output_path
                FROM runs WHERE id = ?
                """,
                (run_id,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
    return _row_to_run(row) if row else None


def _list_runs(limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit), 100))
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            if status:
                cur = conn.execute(
                    """
                    SELECT id, created_at, status, started_at, finished_at, mode, session_id, query_json,
                           matches_count, reference_options_count,
                           selected_reference_drug, selection_rows_count, selection_json, selection_file_path,
                           router_output_text, router_output_path
                    FROM runs WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (status, limit),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT id, created_at, status, started_at, finished_at, mode, session_id, query_json,
                           matches_count, reference_options_count,
                           selected_reference_drug, selection_rows_count, selection_json, selection_file_path,
                           router_output_text, router_output_path
                    FROM runs
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [_row_to_run(row) for row in rows]


def _get_latest_synopsis(run_id: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            """
            SELECT id, created_at, status, output_docx_path, error_text
            FROM synopsis_runs
            WHERE source_run_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    synopsis_id, created_at, status, output_docx_path, error_text = row
    download_url = None
    if output_docx_path:
        try:
            p = Path(output_docx_path).resolve()
            p.relative_to(DOWNLOADS_DIR.resolve())
            download_url = f"/downloads/{p.name}"
        except Exception:
            download_url = None
    return {
        "id": synopsis_id,
        "created_at": created_at,
        "status": status,
        "output_docx_path": output_docx_path,
        "download_url": download_url,
        "error_text": error_text,
    }


def _delete_run(run_id: str) -> bool:
    with DB_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            cur = conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _error(handler: BaseHTTPRequestHandler, status: int, message: str, *, details: Any = None) -> None:
    payload = {"ok": False, "error": message}
    if details is not None:
        payload["details"] = details
    _json_response(handler, status, payload)


def _serve_static_file(handler: BaseHTTPRequestHandler, file_path: Path) -> None:
    if not file_path.exists() or not file_path.is_file():
        _error(handler, HTTPStatus.NOT_FOUND, f"РЎС‚Р°С‚РёС‡РµСЃРєРёР№ С„Р°Р№Р» РЅРµ РЅР°Р№РґРµРЅ: {file_path.name}")
        return
    body = file_path.read_bytes()
    content_type, _ = mimetypes.guess_type(str(file_path))
    if file_path.suffix.lower() == ".js":
        content_type = "application/javascript"
    elif file_path.suffix.lower() == ".css":
        content_type = "text/css"
    elif file_path.suffix.lower() == ".docx":
        content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if not content_type:
        content_type = "application/octet-stream"
    if (content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}) and "charset=" not in content_type:
        content_type = f"{content_type}; charset=utf-8"
    handler.send_response(HTTPStatus.OK)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _try_serve_frontend(handler: BaseHTTPRequestHandler, path: str) -> bool:
    if path in {"/", "/index.html"}:
        _serve_static_file(handler, FRONTEND_DIR / "index.html")
        return True

    if not path.startswith("/ui/"):
        return False

    relative = path.removeprefix("/ui/").strip("/")
    if not relative:
        _serve_static_file(handler, FRONTEND_DIR / "index.html")
        return True

    file_path = (FRONTEND_DIR / relative).resolve()
    try:
        file_path.relative_to(FRONTEND_DIR.resolve())
    except ValueError:
        _error(handler, HTTPStatus.BAD_REQUEST, "РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ РїСѓС‚СЊ Рє СЃС‚Р°С‚РёС‡РµСЃРєРѕРјСѓ С„Р°Р№Р»Сѓ")
        return True

    _serve_static_file(handler, file_path)
    return True


def _try_serve_download(handler: BaseHTTPRequestHandler, path: str) -> bool:
    if not path.startswith("/downloads/"):
        return False
    rel = path.removeprefix("/downloads/").strip("/")
    if not rel:
        _error(handler, HTTPStatus.BAD_REQUEST, "РџСѓС‚СЊ Рє С„Р°Р№Р»Сѓ РЅРµ СѓРєР°Р·Р°РЅ")
        return True
    file_path = (DOWNLOADS_DIR / rel).resolve()
    try:
        file_path.relative_to(DOWNLOADS_DIR.resolve())
    except ValueError:
        _error(handler, HTTPStatus.BAD_REQUEST, "РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ РїСѓС‚СЊ Рє С„Р°Р№Р»Сѓ")
        return True
    _serve_static_file(handler, file_path)
    return True


def _resolve_xls_path(raw: str | None) -> Path:
    if raw:
        path = Path(raw)
    else:
        path = ref_finder.first_xls_in_dir(Path.cwd())
    if not path.exists():
        raise FileNotFoundError(f"Р¤Р°Р№Р» .xls РЅРµ РЅР°Р№РґРµРЅ: {path}")
    return path


def _get_dataframe(xls_path: Path) -> pd.DataFrame:
    key = str(xls_path.resolve())
    with DF_CACHE_LOCK:
        cached = DF_CACHE.get(key)
    if cached is not None:
        return cached

    df = ref_finder.load_sheet(xls_path)
    with DF_CACHE_LOCK:
        DF_CACHE[key] = df
    return df


def _validate_query_payload(payload: dict[str, Any]) -> dict[str, str]:
    required = {
        "mnn": "РњРќРќ",
        "routes": "РџСѓС‚Рё РІРІРµРґРµРЅРёСЏ",
        "base_form": "Р‘Р°Р·РѕРІР°СЏ Р»РµРєР°СЂСЃС‚РІРµРЅРЅР°СЏ С„РѕСЂРјР°",
        "release_type": "РўРёРї РІС‹СЃРІРѕР±РѕР¶РґРµРЅРёСЏ",
        "dosage": "Р”РѕР·РёСЂРѕРІРєР°",
    }
    result: dict[str, str] = {}
    missing = []
    for key in required:
        value = payload.get(key)
        if value is None or not str(value).strip():
            missing.append(key)
        else:
            result[key] = str(value).strip()
    if missing:
        labels = [required[k] for k in missing]
        raise ValueError(f"РћС‚СЃСѓС‚СЃС‚РІСѓСЋС‚ РѕР±СЏР·Р°С‚РµР»СЊРЅС‹Рµ РїРѕР»СЏ: {', '.join(labels)}")
    return result


def _find_matches(df: pd.DataFrame, query: dict[str, str]) -> pd.DataFrame:
    user_routes = ref_finder.parse_user_routes(query["routes"])
    if not user_routes:
        user_routes = {ref_finder.normalize_text(query["routes"])}

    mask = df.apply(
        ref_finder.row_matches,
        axis=1,
        args=(
            query["mnn"],
            user_routes,
            query["base_form"],
            query["release_type"],
            query["dosage"],
        ),
    )
    return df[mask].copy()


def _build_reference_options(matches: pd.DataFrame) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for ref_name, group in matches.groupby(ref_finder.COL_REFERENCE, sort=True):
        sample_rows = []
        for _, row in group.head(3).iterrows():
            sample_rows.append(
                {
                    "trade_name": None if pd.isna(row[ref_finder.COL_TRADE]) else str(row[ref_finder.COL_TRADE]),
                    "drug_form": None if pd.isna(row[ref_finder.COL_FORM]) else str(row[ref_finder.COL_FORM]),
                    "dosage": None if pd.isna(row[ref_finder.COL_DOSAGE]) else str(row[ref_finder.COL_DOSAGE]),
                }
            )
        options.append(
            {
                "reference_drug": str(ref_name),
                "rows_count": int(len(group)),
                "sample_rows": sample_rows,
            }
        )
    options.sort(key=lambda item: item["reference_drug"])
    return options


def _build_matches_preview(matches: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(matches.head(limit).iterrows(), start=1):
        preview.append(
            {
                "row_no": idx,
                "reference_drug": str(row[ref_finder.COL_REFERENCE]),
                "mnn": str(row[ref_finder.COL_MNN]),
                "trade_name": str(row[ref_finder.COL_TRADE]),
                "drug_form": None if pd.isna(row[ref_finder.COL_FORM]) else str(row[ref_finder.COL_FORM]),
                "dosage": None if pd.isna(row[ref_finder.COL_DOSAGE]) else str(row[ref_finder.COL_DOSAGE]),
                "parsed": {
                    "base_form": row["parsed_base_form"],
                    "release_type": row["parsed_release_type"],
                    "routes": list(row["parsed_routes"] or []),
                },
            }
        )
    return preview


def _store_search_session(
    xls_path: Path,
    query: dict[str, str],
    matches: pd.DataFrame,
    reference_options: list[dict[str, Any]],
) -> SearchSession:
    session = SearchSession(
        request_id=uuid.uuid4().hex,
        created_at=datetime.now().isoformat(timespec="seconds"),
        xls_path=str(xls_path),
        query=query,
        matched_indices=[int(i) for i in matches.index.tolist()],
        reference_options=reference_options,
    )
    with SESSIONS_LOCK:
        SESSIONS[session.request_id] = session
    return session


def _get_session_or_raise(request_id: str) -> SearchSession:
    with SESSIONS_LOCK:
        session = SESSIONS.get(request_id)
    if session is None:
        raise KeyError(f"РЎРµСЃСЃРёСЏ РЅРµ РЅР°Р№РґРµРЅР°: {request_id}")
    return session


def _get_matches_from_session(session: SearchSession) -> pd.DataFrame:
    df = _get_dataframe(Path(session.xls_path))
    return df.loc[session.matched_indices].copy()


def _resolve_choice(
    session: SearchSession,
    payload: dict[str, Any],
) -> str:
    if "reference_drug" in payload and str(payload["reference_drug"]).strip():
        ref_name = str(payload["reference_drug"]).strip()
        for option in session.reference_options:
            if option["reference_drug"] == ref_name:
                return ref_name
        raise ValueError("РЈРєР°Р·Р°РЅРЅС‹Р№ reference_drug РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ РІР°СЂРёР°РЅС‚Р°С… СЃРµСЃСЃРёРё")

    if "option_index" in payload:
        try:
            idx = int(payload["option_index"])
        except (TypeError, ValueError):
            raise ValueError("option_index РґРѕР»Р¶РµРЅ Р±С‹С‚СЊ С‡РёСЃР»РѕРј") from None
        if not (1 <= idx <= len(session.reference_options)):
            raise ValueError(f"option_index РІРЅРµ РґРёР°РїР°Р·РѕРЅР° 1..{len(session.reference_options)}")
        return session.reference_options[idx - 1]["reference_drug"]

    raise ValueError("РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ `option_index` РёР»Рё `reference_drug`")


def _choose_reference(
    session: SearchSession,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str, int, str | None]:
    chosen_reference = _resolve_choice(session, payload)
    matches = _get_matches_from_session(session)
    chosen_rows = matches[matches[ref_finder.COL_REFERENCE] == chosen_reference].copy()
    if chosen_rows.empty:
        raise RuntimeError("Р’С‹Р±СЂР°РЅРЅС‹Р№ СЂРµС„РµСЂРµРЅС‚ РЅРµ РЅР°Р№РґРµРЅ РІ СЃРѕРІРїР°РґРµРЅРёСЏС… СЃРµСЃСЃРёРё")

    output_payload = ref_finder.build_output_payload(
        session.query,
        chosen_reference,
        chosen_rows,
        session.reference_options,
        Path(session.xls_path),
    )

    save_path_raw = payload.get("save_json_path")
    save_path = None
    if save_path_raw:
        save_path = Path(str(save_path_raw))
        save_path.write_text(json.dumps(output_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return output_payload, chosen_reference, int(len(chosen_rows)), str(save_path) if save_path else None


class PharmaApiHandler(BaseHTTPRequestHandler):
    server_version = "PharmaLocalAPI/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if _try_serve_frontend(self, parsed.path):
            return
        if _try_serve_download(self, parsed.path):
            return

        if parsed.path == "/health":
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "ok": True,
                    "service": "pharma-local-api",
                    "time": datetime.now().isoformat(timespec="seconds"),
                    "sessions_count": len(SESSIONS),
                    "cached_workbooks": len(DF_CACHE),
                },
            )
            return

        if parsed.path == "/runs/list":
            qs = parse_qs(parsed.query or "")
            status = qs.get("status", [None])[0]
            limit_raw = qs.get("limit", [20])[0]
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                limit = 20
            runs = _list_runs(limit=limit, status=status)
            _json_response(self, HTTPStatus.OK, {"ok": True, "runs": runs})
            return

        if parsed.path == "/runs/get":
            qs = parse_qs(parsed.query or "")
            run_id = str(qs.get("run_id", [""])[0]).strip()
            if not run_id:
                _error(self, HTTPStatus.BAD_REQUEST, "РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ run_id")
                return
            run = _get_run(run_id)
            if not run:
                _error(self, HTTPStatus.NOT_FOUND, f"Р—Р°РїРёСЃСЊ РЅРµ РЅР°Р№РґРµРЅР°: {run_id}")
                return
            _json_response(self, HTTPStatus.OK, {"ok": True, "run": run})
            return

        if parsed.path == "/synopsis/get":
            qs = parse_qs(parsed.query or "")
            run_id = str(qs.get("run_id", [""])[0]).strip()
            if not run_id:
                _error(self, HTTPStatus.BAD_REQUEST, "РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ run_id")
                return
            synopsis = _get_latest_synopsis(run_id)
            if not synopsis:
                _json_response(self, HTTPStatus.OK, {"ok": True, "synopsis": None})
                return
            _json_response(self, HTTPStatus.OK, {"ok": True, "synopsis": synopsis})
            return

        if parsed.path == "/sessions":
            with SESSIONS_LOCK:
                sessions = [asdict(s) for s in SESSIONS.values()]
            _json_response(self, HTTPStatus.OK, {"ok": True, "sessions": sessions})
            return

        _error(self, HTTPStatus.NOT_FOUND, f"РњР°СЂС€СЂСѓС‚ РЅРµ РЅР°Р№РґРµРЅ: {parsed.path}")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            payload = _read_json(self)
            if parsed.path == "/reference/search":
                self._handle_reference_search(payload)
                return
            if parsed.path == "/reference/choose":
                self._handle_reference_choose(payload)
                return
            if parsed.path == "/router/analyze":
                self._handle_router_analyze(payload)
                return
            if parsed.path == "/pipeline/analyze":
                self._handle_pipeline_analyze(payload)
                return
            if parsed.path == "/synopsis/build":
                self._handle_synopsis_build(payload)
                return
            if parsed.path == "/runs/delete":
                self._handle_run_delete(payload)
                return
            _error(self, HTTPStatus.NOT_FOUND, f"РњР°СЂС€СЂСѓС‚ РЅРµ РЅР°Р№РґРµРЅ: {parsed.path}")
        except json.JSONDecodeError as e:
            _error(self, HTTPStatus.BAD_REQUEST, "РќРµРєРѕСЂСЂРµРєС‚РЅС‹Р№ JSON", details=str(e))
        except FileNotFoundError as e:
            _error(self, HTTPStatus.BAD_REQUEST, str(e))
        except KeyError as e:
            _error(self, HTTPStatus.NOT_FOUND, str(e))
        except ValueError as e:
            _error(self, HTTPStatus.BAD_REQUEST, str(e))
        except Exception as e:
            _error(
                self,
                HTTPStatus.INTERNAL_SERVER_ERROR,
                str(e),
                details=traceback.format_exc(),
            )

    def _handle_reference_search(self, payload: dict[str, Any]) -> None:
        xls_path = _resolve_xls_path(payload.get("xls_path"))
        query = _validate_query_payload(payload)
        df = _get_dataframe(xls_path)
        matches = _find_matches(df, query)

        if matches.empty:
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "ok": True,
                    "query": query,
                    "xls_path": str(xls_path),
                    "matches_count": 0,
                    "reference_options_count": 0,
                    "reference_options": [],
                    "match_rows_preview": [],
                    "message": "РЎРѕРІРїР°РґРµРЅРёСЏ РЅРµ РЅР°Р№РґРµРЅС‹",
                },
            )
            return

        reference_options = _build_reference_options(matches)
        session = _store_search_session(xls_path, query, matches, reference_options)

        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "session_id": session.request_id,
                "xls_path": str(xls_path),
                "query": query,
                "matches_count": int(len(matches)),
                "reference_options_count": len(reference_options),
                "reference_options": reference_options,
                "match_rows_preview": _build_matches_preview(matches, limit=20),
            },
        )

    def _handle_reference_choose(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("session_id") or "").strip()
        if not request_id:
            raise ValueError("РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ `session_id`")
        session = _get_session_or_raise(request_id)
        selection_payload, chosen_reference, rows_count, saved_json_path = _choose_reference(session, payload)
        timestamp = datetime.now().isoformat(timespec="seconds")
        run_id = _insert_run(
            status="done",
            started_at=timestamp,
            finished_at=timestamp,
            mode="choose",
            session_id=session.request_id,
            query=session.query,
            matches_count=len(session.matched_indices),
            reference_options_count=len(session.reference_options),
            selected_reference_drug=chosen_reference,
            selection_rows_count=rows_count,
            selection_payload=selection_payload,
            selection_file_path=saved_json_path,
            router_output_text=None,
            router_output_path=None,
        )
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "run_id": run_id,
                "session_id": session.request_id,
                "saved_json_path": saved_json_path,
                "selected_reference_drug": chosen_reference,
                "selected_reference_rows_count": rows_count,
                "selection_payload": selection_payload,
            },
        )

    def _handle_router_analyze(self, payload: dict[str, Any]) -> None:
        reference_drug = str(payload.get("reference_drug") or "").strip()
        if not reference_drug:
            raise ValueError("РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ `reference_drug`")

        test_router_path = str(payload.get("test_router_path") or "test_router.py")
        timestamp = datetime.now().isoformat(timespec="seconds")
        run_id = _insert_run(
            status="running",
            started_at=timestamp,
            finished_at=None,
            mode="router",
            session_id=None,
            query=None,
            matches_count=None,
            reference_options_count=None,
            selected_reference_drug=reference_drug,
            selection_rows_count=None,
            selection_payload=None,
            selection_file_path=None,
            router_output_text=None,
            router_output_path=None,
        )
        print(f"[router] send -> test_router.py | drug='{reference_drug}' | path='{test_router_path}'")
        analysis_text = call_test_router_openrouter(reference_drug, test_router_path=test_router_path)
        print(f"[router] recv <- test_router.py | chars={len(analysis_text or '')}")

        save_path_raw = payload.get("save_response_path")
        save_path = None
        if save_path_raw:
            save_path = Path(str(save_path_raw))
            save_path.write_text(analysis_text, encoding="utf-8")

        finished_at = datetime.now().isoformat(timespec="seconds")
        _update_run(
            run_id,
            status="done",
            finished_at=finished_at,
            router_output_text=analysis_text,
            router_output_path=str(save_path) if save_path else None,
        )

        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "run_id": run_id,
                "reference_drug": reference_drug,
                "test_router_path": test_router_path,
                "saved_response_path": str(save_path) if save_path else None,
                "analysis_text": analysis_text,
            },
        )

    def _handle_pipeline_analyze(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("session_id") or "").strip()
        if not request_id:
            raise ValueError("РќСѓР¶РЅРѕ РїРµСЂРµРґР°С‚СЊ `session_id`")
        session = _get_session_or_raise(request_id)

        selection_payload, chosen_reference, rows_count, saved_json_path = _choose_reference(session, payload)
        selected_reference = chosen_reference
        test_router_path = str(payload.get("test_router_path") or "test_router.py")
        timestamp = datetime.now().isoformat(timespec="seconds")
        run_id = _insert_run(
            status="running",
            started_at=timestamp,
            finished_at=None,
            mode="pipeline",
            session_id=session.request_id,
            query=session.query,
            matches_count=len(session.matched_indices),
            reference_options_count=len(session.reference_options),
            selected_reference_drug=selected_reference,
            selection_rows_count=rows_count,
            selection_payload=selection_payload,
            selection_file_path=saved_json_path,
            router_output_text=None,
            router_output_path=None,
        )
        print(f"[pipeline] send -> test_router.py | drug='{selected_reference}' | path='{test_router_path}'")
        analysis_text = call_test_router_openrouter(selected_reference, test_router_path=test_router_path)
        print(f"[pipeline] recv <- test_router.py | chars={len(analysis_text or '')}")

        save_router_output_raw = payload.get("save_router_output_path")
        router_output_path = None
        if save_router_output_raw:
            router_output_path = Path(str(save_router_output_raw))
            router_output_path.write_text(analysis_text, encoding="utf-8")

        finished_at = datetime.now().isoformat(timespec="seconds")
        _update_run(
            run_id,
            status="done",
            finished_at=finished_at,
            router_output_text=analysis_text,
            router_output_path=str(router_output_path) if router_output_path else None,
        )

        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "selection": {
                    "run_id": run_id,
                    "session_id": session.request_id,
                    "saved_json_path": saved_json_path,
                    "selected_reference_drug": selected_reference,
                    "selected_reference_rows_count": rows_count,
                    "selection_payload": selection_payload,
                },
                "router": {
                    "reference_drug": selected_reference,
                    "test_router_path": test_router_path,
                    "saved_response_path": str(router_output_path) if router_output_path else None,
                    "analysis_text": analysis_text,
                },
            },
        )

    def _handle_synopsis_build(self, payload: dict[str, Any]) -> None:
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            raise ValueError("Нужно передать `run_id`")

        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        prompt_path = payload.get("prompt_path")
        template_path = payload.get("template_path") or str(synopsis_service.TEMPLATE_PATH_DEFAULT)
        output_docx = payload.get("output_docx")
        output_name = output_docx or f"synopsis_{run_id}.docx"
        output_path = (DOWNLOADS_DIR / output_name).resolve()

        result = synopsis_service.build_synopsis_for_run(
            run_id=run_id,
            prompt_path=Path(prompt_path) if prompt_path else None,
            template_path=Path(template_path) if template_path else None,
            output_docx_path=output_path,
        )

        _json_response(
            self,
            HTTPStatus.OK,
            {
                "ok": True,
                "synopsis_run_id": result.get("synopsis_run_id"),
                "run_id": run_id,
                "output_docx_path": result["output_docx_path"],
                "download_url": f"/downloads/{output_path.name}",
            },
        )

    def _handle_run_delete(self, payload: dict[str, Any]) -> None:
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            raise ValueError("Нужно передать `run_id`")
        deleted = _delete_run(run_id)
        if not deleted:
            raise KeyError(f"Запись не найдена: {run_id}")
        _json_response(self, HTTPStatus.OK, {"ok": True, "run_id": run_id, "deleted": True})

    def log_message(self, format: str, *args: Any) -> None:
        # РљРѕСЂРѕС‚РєРёР№ Р»РѕРі РІ РєРѕРЅСЃРѕР»СЊ, Р±РµР· Р»РёС€РЅРµРіРѕ С€СѓРјР°.
        print(f"[{self.log_date_time_string()}] {self.address_string()} - {format % args}")


def run_server(host: str = "127.0.0.1", port: int = 8000) -> None:
    _init_db()
    server = ThreadingHTTPServer((host, port), PharmaApiHandler)
    print(f"Pharma local API started at http://{host}:{port}")
    print("UI: GET /")
    print("Routes: GET /health, GET /runs/list, GET /runs/get, GET /synopsis/get, POST /reference/search, POST /reference/choose, POST /router/analyze, POST /pipeline/analyze, POST /synopsis/build, POST /runs/delete")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Local API for reference drug search + test_router integration")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()

