#!/usr/bin/env python3
"""
terraform-extractor.py — HCL2-based Terraform (.tf) extractor for pharos.

Usage:
    python3 terraform-extractor.py --root <dir>       # extract all .tf files under dir
    python3 terraform-extractor.py --file <file.tf>   # extract a single file

Terraform has no OOP structure, so every HCL block (resource, data, module,
variable, output, provider, and each key inside a locals block) is emitted
as a top-level "function" entry with class_name=null. ScriptBasedCodeParser
attaches these to a synthetic per-file module pseudo-class.

Output: JSON array to stdout, one element per source file:
[
  {
    "file": "/abs/path/to/main.tf",
    "package": "modules.vpc.main",
    "functions": [
      { "name": "aws_instance.web", "class_name": null,
        "params": ["ami", "instance_type"],
        "decorators": ["resource"],
        "docstring": null, "body": "resource \\"aws_instance\\" ...",
        "calls": ["var.region", "data.aws_ami.foo"],
        "start": 10, "end": 20, "is_constructor": false }
    ]
  }
]

Requires the third-party "python-hcl2" package (pip install python-hcl2).
"""

import json
import os
import re
import sys
import argparse

try:
    import hcl2
    from hcl2.utils import SerializationOptions
    HCL2_AVAILABLE = True
except ImportError:
    HCL2_AVAILABLE = False

SKIP_DIRS = {
    ".git", ".terraform", ".terragrunt-cache", "node_modules",
}

BLOCK_KINDS = ("resource", "data", "module", "variable", "output", "provider", "locals")

BLOCK_HEADER_RE = re.compile(
    r'^\s*(resource|data|module|variable|output|provider|locals)\b'
    r'(?:\s*"([^"]*)")?(?:\s*"([^"]*)")?\s*\{'
)

STRING_RE = re.compile(r'"(?:[^"\\]|\\.)*"')

RESERVED_FIRST_WORDS = {
    "count", "each", "self", "path", "terraform",
    "var", "local", "module", "data",
}


def strip_strings(line):
    """Blank out quoted-string contents so brace counting ignores interpolations."""
    return STRING_RE.sub(lambda m: '"' + (" " * (len(m.group(0)) - 2)) + '"', line)


def scan_top_level_blocks(lines):
    """Brace-depth scan over raw source lines to find each top-level HCL block.

    Returns a list of dicts: {kind, labels, start, end, body}, in file order.
    """
    blocks = []
    depth = 0
    current = None

    for idx, raw_line in enumerate(lines):
        line_no = idx + 1
        sanitized = strip_strings(raw_line)

        if depth == 0 and current is None:
            m = BLOCK_HEADER_RE.match(raw_line)
            if m:
                kind = m.group(1)
                labels = [g for g in (m.group(2), m.group(3)) if g is not None]
                current = {"kind": kind, "labels": labels, "start": line_no}

        depth += sanitized.count("{") - sanitized.count("}")

        if current is not None and depth <= 0:
            current["end"] = line_no
            current["body"] = "".join(lines[current["start"] - 1:current["end"]])
            blocks.append(current)
            current = None
            depth = 0

    return blocks


def parse_attributes(filepath):
    """Parse the file with python-hcl2, returning {kind: [block_attrs, ...]} in file order."""
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            data = hcl2.load(f, serialization_options=SerializationOptions(strip_string_quotes=True))
    except Exception as e:
        sys.stderr.write(f"hcl2 parse error in {filepath}: {e}\n")
        return {}

    result = {}
    for kind in BLOCK_KINDS:
        entries = data.get(kind, [])
        parsed = []
        for entry in entries:
            if kind == "locals":
                attrs = {k: v for k, v in entry.items() if k != "__is_block__"}
                parsed.append(attrs)
            elif kind in ("resource", "data"):
                # entry = {type: {name: {attrs...}}}
                for type_name, named in entry.items():
                    for label_name, attrs in named.items():
                        parsed.append({k: v for k, v in attrs.items() if k != "__is_block__"})
            else:
                # module/variable/output/provider: entry = {name: {attrs...}}
                for label_name, attrs in entry.items():
                    if isinstance(attrs, dict):
                        parsed.append({k: v for k, v in attrs.items() if k != "__is_block__"})
        result[kind] = parsed
    return result


REF_DATA_RE = re.compile(r'\bdata\.([\w-]+)\.(\w+)')
REF_VAR_RE = re.compile(r'\bvar\.(\w+)')
REF_LOCAL_RE = re.compile(r'\blocal\.(\w+)')
REF_MODULE_RE = re.compile(r'\bmodule\.(\w+)')
REF_BARE_RE = re.compile(r'\b([a-zA-Z_][\w-]*)\.([a-zA-Z_]\w*)\b')


def extract_calls(body_text):
    """Best-effort regex scan of a block's raw text for cross-block references."""
    calls = []

    def blank(match):
        return " " * len(match.group(0))

    remaining = body_text

    for m in REF_DATA_RE.finditer(remaining):
        calls.append(f"data.{m.group(1)}.{m.group(2)}")
    remaining = REF_DATA_RE.sub(blank, remaining)

    for m in REF_VAR_RE.finditer(remaining):
        calls.append(f"var.{m.group(1)}")
    remaining = REF_VAR_RE.sub(blank, remaining)

    for m in REF_LOCAL_RE.finditer(remaining):
        calls.append(f"local.{m.group(1)}")
    remaining = REF_LOCAL_RE.sub(blank, remaining)

    for m in REF_MODULE_RE.finditer(remaining):
        calls.append(f"module.{m.group(1)}")
    remaining = REF_MODULE_RE.sub(blank, remaining)

    # Plain string literals (no interpolation) are attribute values, not references —
    # blank them before the bare-pattern pass to avoid noise like "t2.micro".
    bare_scan_text = re.sub(r'"(?:[^"${\\]|\\.)*"', blank, remaining)

    for m in REF_BARE_RE.finditer(bare_scan_text):
        first, second = m.group(1), m.group(2)
        if first in RESERVED_FIRST_WORDS:
            continue
        calls.append(f"{first}.{second}")

    return list(dict.fromkeys(calls))  # dedupe, preserve order


def block_name(kind, labels):
    if kind == "resource":
        return f"{labels[0]}.{labels[1]}" if len(labels) >= 2 else None
    if kind == "data":
        return f"data.{labels[0]}.{labels[1]}" if len(labels) >= 2 else None
    if kind == "module":
        return f"module.{labels[0]}" if len(labels) >= 1 else None
    if kind == "variable":
        return f"var.{labels[0]}" if len(labels) >= 1 else None
    if kind == "output":
        return f"output.{labels[0]}" if len(labels) >= 1 else None
    if kind == "provider":
        return f"provider.{labels[0]}" if len(labels) >= 1 else None
    return None


def build_functions(blocks, attrs_by_kind):
    """Correlate brace-scanned blocks with hcl2-parsed attributes (same kind, file order)."""
    functions = []
    cursors = {kind: 0 for kind in BLOCK_KINDS}

    for block in blocks:
        kind = block["kind"]
        if kind == "locals":
            idx = cursors["locals"]
            cursors["locals"] += 1
            attrs = attrs_by_kind.get("locals", [])
            local_attrs = attrs[idx] if idx < len(attrs) else {}
            for key in local_attrs:
                functions.append({
                    "name": f"local.{key}",
                    "class_name": None,
                    "params": [],
                    "decorators": ["locals"],
                    "docstring": None,
                    "body": block["body"],
                    "calls": extract_calls(block["body"]),
                    "start": block["start"],
                    "end": block["end"],
                    "is_constructor": False,
                })
            continue

        name = block_name(kind, block["labels"])
        if name is None:
            continue

        idx = cursors[kind]
        cursors[kind] += 1
        attrs_list = attrs_by_kind.get(kind, [])
        attrs = attrs_list[idx] if idx < len(attrs_list) else {}

        params = [k for k in attrs.keys()]
        docstring = attrs.get("description")
        if not isinstance(docstring, str):
            docstring = None

        functions.append({
            "name": name,
            "class_name": None,
            "params": params,
            "decorators": [kind],
            "docstring": docstring,
            "body": block["body"],
            "calls": extract_calls(block["body"]),
            "start": block["start"],
            "end": block["end"],
            "is_constructor": False,
        })

    return functions


def derive_package(filepath, root_dir):
    rel = os.path.relpath(filepath, root_dir)
    parts = rel.replace(os.sep, "/").split("/")
    stem = os.path.splitext(parts[-1])[0]
    package_parts = parts[:-1] + [stem]
    return ".".join(p for p in package_parts if p)


def extract_file(filepath, root_dir):
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError as e:
        sys.stderr.write(f"Cannot read {filepath}: {e}\n")
        return None

    blocks = scan_top_level_blocks(lines)
    attrs_by_kind = parse_attributes(filepath)
    functions = build_functions(blocks, attrs_by_kind)

    return {
        "file": os.path.abspath(filepath),
        "package": derive_package(filepath, root_dir),
        "functions": functions,
    }


def extract_dir(root_dir):
    results = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d not in SKIP_DIRS]
        for filename in sorted(filenames):
            if filename.endswith(".tf"):
                filepath = os.path.join(dirpath, filename)
                result = extract_file(filepath, root_dir)
                if result is not None:
                    results.append(result)
    return results


def extract_single(filepath):
    root_dir = os.path.dirname(os.path.abspath(filepath))
    result = extract_file(os.path.abspath(filepath), root_dir)
    return [result] if result is not None else []


def main():
    parser = argparse.ArgumentParser(description="Extract Terraform HCL info for pharos")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--root", metavar="DIR", help="Root directory to walk")
    group.add_argument("--file", metavar="FILE", help="Single file to extract")
    args = parser.parse_args()

    if not HCL2_AVAILABLE:
        sys.stderr.write("python-hcl2 not installed — run: pip install python-hcl2\n")
        json.dump([], sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    if args.root:
        results = extract_dir(os.path.abspath(args.root))
    else:
        results = extract_single(args.file)

    json.dump(results, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
