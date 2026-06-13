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
    return [ROOT / line for line in result.stdout.splitlines() if line]


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
            "WARN" if runtime_module_hits else "PASS",
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
    print()
    print("## Forwarding Stub Reference Report")
    for line in direct_reference_report(methods):
        print(line)
    return 1 if reverse_live_runtime_hits or dynamic_hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
