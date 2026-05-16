#!/usr/bin/env python3
"""
python-extractor.py — AST-based Python source extractor for codesearch.

Usage:
    python3 python-extractor.py --root <dir>        # extract all .py files under dir
    python3 python-extractor.py --file <file.py>    # extract a single file

Output: JSON array to stdout, one element per source file:
[
  {
    "file": "/abs/path/to/foo.py",
    "package": "package.subpackage",   // derived from path relative to root
    "classes": [
      { "name": "MyClass", "qualified": "pkg.MyClass",
        "bases": ["Base"], "decorators": ["dataclass"],
        "docstring": "...", "start": 10, "end": 50 }
    ],
    "functions": [
      { "name": "my_func", "class_name": "MyClass",  // null for top-level
        "params": ["self", "x"],
        "decorators": ["staticmethod"],
        "docstring": "...", "body": "def my_func...",
        "calls": ["helper", "util"],
        "start": 15, "end": 20, "is_constructor": false }
    ]
  }
]
"""

import ast
import json
import os
import sys
import argparse
import textwrap


def decorator_name(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):
        return decorator_name(node.func)
    return None


def extract_calls(func_node):
    """Collect all function/method call names inside a function body."""
    calls = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                calls.append(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                calls.append(node.func.attr)
    return list(dict.fromkeys(calls))  # deduplicate, preserve order


def get_docstring(node):
    return ast.get_docstring(node)


def type_annotation_str(node):
    """Convert an annotation AST node to a string (best-effort)."""
    try:
        return ast.unparse(node)
    except AttributeError:
        # Python < 3.8 has no ast.unparse
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None


def extract_class_fields(class_node):
    """Extract field declarations from a class body.

    Two sources:
    - Class-level annotated attributes: ``name: Type [= value]``  (ast.AnnAssign)
    - Instance attributes set in __init__: ``self.name = ...``    (ast.Assign)
    """
    fields = []
    seen = set()

    # 1. Class-level annotated attributes (PEP 526 / dataclass style)
    for item in ast.iter_child_nodes(class_node):
        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            name = item.target.id
            if name not in seen:
                seen.add(name)
                fields.append({
                    "name": name,
                    "type": type_annotation_str(item.annotation),
                    "line": item.lineno,
                })

    # 2. Instance attributes assigned in __init__ (self.x = ...)
    for item in ast.iter_child_nodes(class_node):
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == "__init__":
            for stmt in ast.walk(item):
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if (isinstance(target, ast.Attribute) and
                                isinstance(target.value, ast.Name) and
                                target.value.id == "self" and
                                target.attr not in seen):
                            seen.add(target.attr)
                            fields.append({
                                "name": target.attr,
                                "type": None,
                                "line": stmt.lineno,
                            })

    return fields


def source_segment(source_lines, start_line, end_line):
    """Extract source lines (1-based, inclusive)."""
    lines = source_lines[start_line - 1:end_line]
    if not lines:
        return ""
    return textwrap.dedent("".join(lines))


def extract_file(filepath, root_dir, source_lines):
    """Extract classes and functions from a single Python file."""
    try:
        source = "".join(source_lines)
        tree = ast.parse(source, filename=filepath)
    except SyntaxError as e:
        sys.stderr.write(f"SyntaxError in {filepath}: {e}\n")
        return None

    # Derive package from path relative to root_dir
    rel = os.path.relpath(filepath, root_dir)
    parts = rel.replace(os.sep, "/").split("/")
    # Remove __init__.py or the file stem from the last part
    stem = os.path.splitext(parts[-1])[0]
    package_parts = parts[:-1] + ([stem] if stem != "__init__" else [])
    # Filter out leading "src" component — treat src/ as the root
    if package_parts and package_parts[0] == "src":
        package_parts = package_parts[1:]
    package = ".".join(p for p in package_parts if p)

    classes = []
    functions = []

    # Walk top-level nodes
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            qualified = (package + "." + node.name) if package else node.name
            bases = []
            for b in node.bases:
                if isinstance(b, ast.Name):
                    bases.append(b.id)
                elif isinstance(b, ast.Attribute):
                    bases.append(b.attr)

            decorators = [d for d in (decorator_name(d) for d in node.decorator_list) if d]
            end_line = getattr(node, "end_lineno", node.lineno)

            classes.append({
                "name": node.name,
                "qualified": qualified,
                "bases": bases,
                "decorators": decorators,
                "docstring": get_docstring(node),
                "start": node.lineno,
                "end": end_line,
                "fields": extract_class_fields(node),
            })

            # Methods inside the class
            for item in ast.iter_child_nodes(node):
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    params = [a.arg for a in item.args.args]
                    item_end = getattr(item, "end_lineno", item.lineno)
                    body_text = source_segment(source_lines, item.lineno, item_end)
                    item_decorators = [d for d in (decorator_name(d) for d in item.decorator_list) if d]
                    functions.append({
                        "name": item.name,
                        "class_name": node.name,
                        "params": params,
                        "decorators": item_decorators,
                        "docstring": get_docstring(item),
                        "body": body_text,
                        "calls": extract_calls(item),
                        "start": item.lineno,
                        "end": item_end,
                        "is_constructor": item.name == "__init__",
                    })

        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            # Top-level function — class_name is null
            params = [a.arg for a in node.args.args]
            end_line = getattr(node, "end_lineno", node.lineno)
            body_text = source_segment(source_lines, node.lineno, end_line)
            decorators = [d for d in (decorator_name(d) for d in node.decorator_list) if d]
            functions.append({
                "name": node.name,
                "class_name": None,
                "params": params,
                "decorators": decorators,
                "docstring": get_docstring(node),
                "body": body_text,
                "calls": extract_calls(node),
                "start": node.lineno,
                "end": end_line,
                "is_constructor": False,
            })

    return {
        "file": os.path.abspath(filepath),
        "package": package,
        "classes": classes,
        "functions": functions,
    }


def extract_dir(root_dir):
    results = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip hidden directories and common non-source dirs
        dirnames[:] = [d for d in dirnames
                       if not d.startswith(".") and d not in ("__pycache__", ".git", "venv", ".venv", "node_modules")]
        for filename in sorted(filenames):
            if filename.endswith(".py"):
                filepath = os.path.join(dirpath, filename)
                try:
                    with open(filepath, encoding="utf-8", errors="replace") as f:
                        source_lines = f.readlines()
                    result = extract_file(filepath, root_dir, source_lines)
                    if result is not None:
                        results.append(result)
                except OSError as e:
                    sys.stderr.write(f"Cannot read {filepath}: {e}\n")
    return results


def extract_single(filepath):
    root_dir = os.path.dirname(os.path.abspath(filepath))
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            source_lines = f.readlines()
        result = extract_file(os.path.abspath(filepath), root_dir, source_lines)
        return [result] if result is not None else []
    except OSError as e:
        sys.stderr.write(f"Cannot read {filepath}: {e}\n")
        return []


def main():
    parser = argparse.ArgumentParser(description="Extract Python AST info for codesearch")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--root", metavar="DIR", help="Root directory to walk")
    group.add_argument("--file", metavar="FILE", help="Single file to extract")
    args = parser.parse_args()

    if args.root:
        results = extract_dir(os.path.abspath(args.root))
    else:
        results = extract_single(args.file)

    json.dump(results, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
