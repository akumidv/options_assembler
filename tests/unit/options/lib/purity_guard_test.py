"""Architecture guard (R1, T41): ``options/lib`` stays a pure computation layer.

Walks every module under ``alphavar.options.lib`` and fails if one imports the I/O layer or a
facade, or calls a file/network I/O API. AST-based on purpose: it inspects real imports and calls,
so a docstring that merely *mentions* ``provider`` / ``storage`` / ``to_parquet`` is not a hit.
"""
import ast
import pathlib

import pytest

import alphavar.options.lib as _lib

LIB_ROOT = pathlib.Path(_lib.__file__).parent

# Forbidden import targets: the I/O infra layer and the class facades (lib must not depend outward).
_FORBIDDEN_IMPORT_PREFIXES = ("alphavar.io",)
_FORBIDDEN_IMPORT_MODULES = frozenset({"httpx", "requests", "aiohttp", "apscheduler", "urllib.request"})
# Forbidden call targets: file/network I/O the storage layer owns, not lib.
_FORBIDDEN_ATTR_CALLS = frozenset(
    {"read_parquet", "to_parquet", "read_csv", "to_csv", "to_json", "read_json", "makedirs"}
)
_FORBIDDEN_NAME_CALLS = frozenset({"open"})


def _is_facade_import(module: str) -> bool:
    # Facade modules are `alphavar.options.<x>_class` (Option and its component classes).
    return module.startswith("alphavar.options.") and module.rsplit(".", 1)[-1].endswith("_class")


def _violations(tree: ast.AST) -> list[str]:
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            mod = node.module
            if mod.startswith(_FORBIDDEN_IMPORT_PREFIXES) or mod in _FORBIDDEN_IMPORT_MODULES or _is_facade_import(mod):
                found.append(f"import from {mod!r} (line {node.lineno})")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(_FORBIDDEN_IMPORT_PREFIXES) or alias.name in _FORBIDDEN_IMPORT_MODULES:
                    found.append(f"import {alias.name!r} (line {node.lineno})")
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr in _FORBIDDEN_ATTR_CALLS:
                found.append(f"call .{func.attr}() (line {node.lineno})")
            elif isinstance(func, ast.Name) and func.id in _FORBIDDEN_NAME_CALLS:
                found.append(f"call {func.id}() (line {node.lineno})")
    return found


@pytest.mark.parametrize(
    "module_path",
    sorted(LIB_ROOT.rglob("*.py")),
    ids=lambda p: str(p.relative_to(LIB_ROOT)),
)
def test_lib_module_is_pure(module_path):
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    violations = _violations(tree)
    assert not violations, f"{module_path.relative_to(LIB_ROOT)} breaks the pure-lib boundary: {violations}"
