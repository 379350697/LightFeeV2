#!/usr/bin/env python3
"""Report LiveRuntime split architecture drift.

This script is intentionally read-only. It reports the runtime split shape and
flags patterns that would make the delegate boundary harder to audit.
"""

from __future__ import annotations

import ast
import subprocess
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "lightfee" / "engine"
RUNTIME = ENGINE / "runtime.py"
DELEGATES = [
    ENGINE / "residual_repair_runtime.py",
    ENGINE / "passive_maker_runtime.py",
    ENGINE / "market_data_runtime.py",
    ENGINE / "close_runtime.py",
    ENGINE / "entry_gate_runtime.py",
    ENGINE / "entry_dispatch_runtime.py",
    ENGINE / "pending_entry_runtime.py",
    ENGINE / "recovery_startup_runtime.py",
]
DELEGATE_ATTR_SUFFIX = "_runtime"
DYNAMIC_PROXY_PATTERNS = ("__getattr__", "get_delegate", "register_delegate")
TRUTH_ENDPOINT_GUARD_FILES = [
    ENGINE / "recovery_startup_runtime.py",
    ROOT / "scripts" / "diagnose_live.py",
    ENGINE / "passive_close.py",
    ENGINE / "residual_repair_runtime.py",
]
TRUTH_ENDPOINT_ALLOWED_FILES = {
    ROOT / "scripts" / "check_runtime_split_architecture.py",
    ROOT / "lightfee" / "rate_limit" / "config.py",
    ROOT / "lightfee" / "venues" / "specs.py",
}
BITGET_ADAPTER_ENDPOINT_ALLOWED_SCOPES = {"detect_position_hedge_mode"}
PRIVATE_TRUTH_ENDPOINT_LITERALS = (
    "/fapi/v1/openOrders",
    "/fapi/v3/openOrders",
    "/api/v5/trade/orders-pending",
    "/api/v2/mix/order/orders-pending",
    "/v5/order/realtime",
    "/info openOrders",
    '"type": "clearinghouseState"',
    '"type": "userAbstraction"',
    '"type": "spotClearinghouseState"',
)
HYPERLIQUID_INFO_TYPE_VALUES = (
    "clearinghouseState",
    "userAbstraction",
    "spotClearinghouseState",
)
BITGET_PRIVATE_TRUTH_ENDPOINT_LITERALS = (
    "/api/v2/mix/order/place-order",
    "/api/v2/mix/order/cancel-order",
    "/api/v2/mix/order/detail",
    "/api/v2/mix/order/orders-pending",
    "/api/v2/mix/position/single-position",
    "/api/v2/mix/position/all-position",
    "/api/v2/mix/account/account",
    "/api/v2/mix/account/accounts",
    "/api/v3/trade/place-order",
    "/api/v3/trade/cancel-order",
    "/api/v3/trade/order-info",
    "/api/v3/trade/unfilled-orders",
    "/api/v3/position/current-position",
    "/api/v3/account/assets",
    "/api/v3/account/settings",
)


@dataclass(frozen=True)
class RuntimeMethod:
    name: str
    line: int
    forwarding: bool


def status(label: str, state: str, detail: str) -> str:
    return f"{state:<4} {label}: {detail}"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def line_count(path: Path) -> int:
    return len(read(path).splitlines())


def class_methods(path: Path) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    tree = ast.parse(read(path))
    methods: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            methods.extend(
                item
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            )
    return methods


def is_forwarding_return(stmt: ast.stmt) -> bool:
    value: ast.AST | None = None
    if isinstance(stmt, ast.Return):
        value = stmt.value
    elif isinstance(stmt, ast.Expr):
        value = stmt.value
    if isinstance(value, ast.Await):
        value = value.value
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    if not isinstance(func, ast.Attribute):
        return False
    receiver = func.value
    if not isinstance(receiver, ast.Attribute):
        return False
    if not isinstance(receiver.value, ast.Name) or receiver.value.id != "self":
        return False
    return receiver.attr.endswith(DELEGATE_ATTR_SUFFIX)


def runtime_methods() -> list[RuntimeMethod]:
    methods: list[RuntimeMethod] = []
    for node in class_methods(RUNTIME):
        body = [
            stmt
            for stmt in node.body
            if not (
                isinstance(stmt, ast.Expr)
                and isinstance(stmt.value, ast.Constant)
                and isinstance(stmt.value.value, str)
            )
        ]
        methods.append(
            RuntimeMethod(
                name=node.name,
                line=node.lineno,
                forwarding=len(body) == 1 and is_forwarding_return(body[0]),
            )
        )
    return methods


def tracked_python_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    # A guard is also run against in-progress worktrees.  `git ls-files`
    # intentionally still includes paths staged or deleted only in the
    # worktree; attempting to parse one makes the guard itself fail before it
    # can report an architectural violation.  Ignore only paths absent from
    # disk, while preserving every currently readable tracked Python file.
    return [
        path
        for line in result.stdout.splitlines()
        if line and (path := ROOT / line).is_file()
    ]


def _function_line_ranges(path: Path) -> list[tuple[str, int, int]]:
    tree = ast.parse(read(path))
    ranges: list[tuple[str, int, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end_lineno = getattr(node, "end_lineno", node.lineno)
            ranges.append((node.name, node.lineno, end_lineno))
    return ranges


def _is_allowed_bitget_adapter_endpoint_literal(path: Path, lineno: int) -> bool:
    if path != ROOT / "lightfee" / "venues" / "bitget.py":
        return False
    for name, start, end in _function_line_ranges(path):
        if name in BITGET_ADAPTER_ENDPOINT_ALLOWED_SCOPES and start <= lineno <= end:
            return True
    return False


def direct_reference_report(methods: list[RuntimeMethod]) -> list[str]:
    files = tracked_python_files()
    output: list[str] = []
    for method in [item for item in methods if item.forwarding]:
        refs: list[tuple[str, str]] = []
        monkeypatch_refs = 0
        token = f".{method.name}"
        for path in files:
            if path == RUNTIME:
                continue
            try:
                text = read(path)
            except UnicodeDecodeError:
                continue
            if token not in text and f'"{method.name}"' not in text and f"'{method.name}'" not in text:
                continue
            rel = path.relative_to(ROOT).as_posix()
            kind = "test" if rel.startswith("tests/") else "production"
            for line in text.splitlines():
                if token in line or f'"{method.name}"' in line or f"'{method.name}'" in line:
                    refs.append((kind, rel))
                    if "monkeypatch" in line or "patch.object" in line or "setattr" in line:
                        monkeypatch_refs += 1
                    break
        prod = sum(1 for kind, _ in refs if kind == "production")
        tests = sum(1 for kind, _ in refs if kind == "test")
        candidate = prod == 0 and tests == 0 and monkeypatch_refs == 0
        output.append(
            f"{method.name}: prod={prod} test={tests} monkeypatch={monkeypatch_refs} "
            f"gitnexus_upstream=not_queried third_stage_candidate={candidate}"
        )
    return output


def delegate_dependency_hits(path: Path) -> tuple[list[str], list[str]]:
    tree = ast.parse(read(path))
    live_runtime_hits: list[str] = []
    runtime_module_hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == "lightfee.engine.runtime":
                for alias in node.names:
                    if alias.name == "LiveRuntime":
                        live_runtime_hits.append(f"{path.name}:{node.lineno}:import LiveRuntime")
                    else:
                        runtime_module_hits.append(f"{path.name}:{node.lineno}:from runtime import {alias.name}")
            if node.module == "lightfee.engine":
                for alias in node.names:
                    if alias.name == "runtime":
                        runtime_module_hits.append(f"{path.name}:{node.lineno}:from lightfee.engine import runtime")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "lightfee.engine.runtime":
                    runtime_module_hits.append(f"{path.name}:{node.lineno}:import lightfee.engine.runtime")
        elif isinstance(node, ast.Name) and node.id == "LiveRuntime":
            live_runtime_hits.append(f"{path.name}:{node.lineno}:LiveRuntime")
    return live_runtime_hits, runtime_module_hits


def contract_truth_endpoint_bypass_hits() -> list[str]:
    hits: list[str] = []
    for path in TRUTH_ENDPOINT_GUARD_FILES:
        text = read(path)
        rel = path.relative_to(ROOT).as_posix()
        for lineno, line in enumerate(text.splitlines(), start=1):
            for literal in PRIVATE_TRUTH_ENDPOINT_LITERALS:
                if literal in line:
                    hits.append(f"{rel}:{lineno}:{literal}")
        hits.extend(_hyperliquid_info_type_body_hits(path, text))

    bitget_guard_files = [
        path
        for path in tracked_python_files()
        if not path.relative_to(ROOT).as_posix().startswith("tests/")
    ]
    for path in sorted(bitget_guard_files):
        if path in TRUTH_ENDPOINT_ALLOWED_FILES:
            continue
        text = read(path)
        rel = path.relative_to(ROOT).as_posix()
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _is_allowed_bitget_adapter_endpoint_literal(path, lineno):
                continue
            for literal in BITGET_PRIVATE_TRUTH_ENDPOINT_LITERALS:
                if literal in line:
                    hits.append(f"{rel}:{lineno}:{literal}")
    return hits


def _hyperliquid_info_type_body_hits(path: Path, text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []

    rel = path.relative_to(ROOT).as_posix()
    hits: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        values: list[str] = []
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "type"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and value.value in HYPERLIQUID_INFO_TYPE_VALUES
                ):
                    values.append(value.value)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "dict"
        ):
            for keyword in node.keywords:
                value = keyword.value
                if (
                    keyword.arg == "type"
                    and isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and value.value in HYPERLIQUID_INFO_TYPE_VALUES
                ):
                    values.append(value.value)
        for value in values:
            key = (node.lineno, value)
            if key in seen:
                continue
            seen.add(key)
            hits.append((node.lineno, f"{rel}:{node.lineno}:type={value}"))
    return [hit for _lineno, hit in sorted(hits, key=lambda item: item[0])]


def main() -> int:
    methods = runtime_methods()
    forwarding = [item for item in methods if item.forwarding]
    non_forwarding_private = [
        item for item in methods if item.name.startswith("_") and not item.forwarding
    ]
    runtime_text = read(RUNTIME)
    delegate_text = "\n".join(read(path) for path in DELEGATES)
    dynamic_hits = [
        pattern for pattern in DYNAMIC_PROXY_PATTERNS if pattern in runtime_text or pattern in delegate_text
    ]
    truth_endpoint_hits = contract_truth_endpoint_bypass_hits()
    reverse_live_runtime_hits: list[str] = []
    runtime_module_hits: list[str] = []
    for path in DELEGATES:
        live_hits, module_hits = delegate_dependency_hits(path)
        reverse_live_runtime_hits.extend(live_hits)
        runtime_module_hits.extend(module_hits)

    print("# Runtime Split Architecture Report")
    print()
    print(status("runtime_lines", "PASS" if line_count(RUNTIME) <= 11_000 else "WARN", str(line_count(RUNTIME))))
    print(status("runtime_methods", "INFO", str(len(methods))))
    print(status("runtime_forwarding_stubs", "INFO", str(len(forwarding))))
    print(status("runtime_non_forwarding_private_methods", "INFO", str(len(non_forwarding_private))))
    print()
    print("## Delegates")
    for path in DELEGATES:
        rel = path.relative_to(ROOT).as_posix()
        print(f"INFO {rel}: lines={line_count(path)} methods={len(class_methods(path))}")
    print()
    print(
        status(
            "delegate_reverse_live_runtime_dependency",
            "FAIL" if reverse_live_runtime_hits else "PASS",
            ", ".join(reverse_live_runtime_hits) if reverse_live_runtime_hits else "none",
        )
    )
    print(
        status(
            "delegate_runtime_module_dependency",
            "FAIL" if runtime_module_hits else "PASS",
            ", ".join(runtime_module_hits) if runtime_module_hits else "none",
        )
    )
    print(
        status(
            "dynamic_delegate_proxy",
            "FAIL" if dynamic_hits else "PASS",
            ", ".join(dynamic_hits) if dynamic_hits else "none",
        )
    )
    print(
        status(
            "contract_truth_endpoint_registry_bypass",
            "FAIL" if truth_endpoint_hits else "PASS",
            ", ".join(truth_endpoint_hits) if truth_endpoint_hits else "none",
        )
    )
    print()
    print("## Forwarding Stub Reference Report")
    for line in direct_reference_report(methods):
        print(line)
    return 1 if (
        reverse_live_runtime_hits
        or runtime_module_hits
        or dynamic_hits
        or truth_endpoint_hits
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
