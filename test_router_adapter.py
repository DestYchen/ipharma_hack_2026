from __future__ import annotations

import ast
from pathlib import Path
from threading import Lock
from typing import Any


_CACHE_LOCK = Lock()
_CACHE: dict[str, dict[str, Any]] = {}


def _read_source(path: Path) -> str:
    for encoding in ("utf-8", "cp1251"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def _is_skippable_top_level(node: ast.stmt) -> bool:
    # Убираем только пример запуска внизу файла, не затрагивая функции/константы.
    if isinstance(node, ast.Assign):
        target_names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if target_names & {"drug_query", "result"}:
            return True

    if isinstance(node, ast.Expr):
        call = node.value
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "print":
            if call.args and isinstance(call.args[0], ast.Name) and call.args[0].id == "result":
                return True
    return False


def _load_namespace(test_router_path: Path) -> dict[str, Any]:
    cache_key = str(test_router_path.resolve())
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached:
            return cached

    source = _read_source(test_router_path)
    module_ast = ast.parse(source, filename=str(test_router_path))
    filtered_body = [node for node in module_ast.body if not _is_skippable_top_level(node)]
    module_ast = ast.Module(body=filtered_body, type_ignores=[])

    namespace: dict[str, Any] = {
        "__name__": "test_router_runtime",
        "__file__": str(test_router_path),
        "__package__": None,
    }
    compiled = compile(module_ast, str(test_router_path), "exec")
    exec(compiled, namespace, namespace)

    if "openrouter_chat" not in namespace:
        raise RuntimeError("В test_router.py не найдена функция openrouter_chat")

    with _CACHE_LOCK:
        _CACHE[cache_key] = namespace
    return namespace


def call_test_router_openrouter(drug_query: str, test_router_path: str | Path = "test_router.py") -> str:
    path = Path(test_router_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    namespace = _load_namespace(path)
    func = namespace["openrouter_chat"]
    return func(drug_query)


def build_test_router_messages(drug_query: str, test_router_path: str | Path = "test_router.py") -> list[dict[str, str]]:
    path = Path(test_router_path)
    namespace = _load_namespace(path)
    func = namespace.get("build_messages")
    if func is None:
        raise RuntimeError("В test_router.py не найдена функция build_messages")
    return func(drug_query)
