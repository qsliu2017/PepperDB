#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Phase 0 scaffolder: create one .rs per header (skipping the catalog derive-driven
group) plus module-root files (lib.rs / mod.rs / header-as-root) that `pub mod` every
child, guaranteeing module-path resolution before any translation happens."""
import sys
from pathlib import Path

# This file lives at <repo>/ai/harness/scaffold.py.
ROOT = Path(__file__).resolve().parents[2]
HLIST = ROOT / "ai/plans/001-header-file/h-file-list"
SRC = ROOT / "src"

KEYWORDS = {
    "as","break","const","continue","crate","else","enum","extern","false","fn","for",
    "if","impl","in","let","loop","match","mod","move","mut","pub","ref","return","self",
    "Self","static","struct","super","trait","true","type","unsafe","use","where","while",
    "async","await","dyn","abstract","become","box","do","final","macro","override","priv",
    "typeof","unsized","virtual","yield","try","gen",
}


def collect():
    headers, skip = set(), set()
    for f in sorted(HLIST.glob("*.txt")):
        for line in f.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or not line.endswith(".h"):
                continue
            (skip if f.name == "0101.txt" else headers).add(line)
    return headers - skip, skip


def mod_ident(stem):  # filename stem -> (ident, needs_path)
    ident = stem.replace("-", "_")
    needs = "-" in stem
    if ident in KEYWORDS:
        ident = "r#" + ident
        needs = True
    return ident, needs


def decl(name, base, subdir_only):
    """Emit a `pub mod` line. `base` is the path prefix (relative to the declaring
    file's own directory) needed when the directory root is a `<lastseg>.rs` file,
    which lives one level up from where its children resolve."""
    ident, needs = mod_ident(name)
    if subdir_only:  # always explicit -> disambiguates the crate-root `lib` clash
        return f'#[path = "{base}{name}/mod.rs"]\npub mod {ident};'
    if base or needs:
        return f'#[path = "{base}{name}.rs"]\npub mod {ident};'
    return f"pub mod {ident};"


def main():
    headers, _ = collect()
    header_paths = {h[:-2] for h in headers}  # relpaths without .h

    # children[dir] = dict(name -> ("header", filestem) | ("dir", None))
    from collections import defaultdict
    children = defaultdict(dict)
    dirs = set()
    for rel in sorted(header_paths):
        parts = rel.split("/")
        # register every ancestor dir
        for i in range(1, len(parts)):
            d = "/".join(parts[:i])
            dirs.add(d)
            parent = "/".join(parts[:i-1])
            children[parent][parts[i-1]] = ("dir", None)
        parent = "/".join(parts[:-1])
        name = parts[-1]
        children[parent][name] = ("header", name)  # filestem == name (last segment)
    dirs.add("")

    def root_file(d):
        if d == "":
            return SRC / "lib.rs"
        if d in header_paths:               # header D.h doubles as the dir root
            return SRC / (d + ".rs")
        return SRC / d / "mod.rs"

    written = 0
    # 1. create placeholder .rs for every header that is NOT a dir root
    for rel in sorted(header_paths):
        if rel in dirs:
            continue  # handled as a root file below
        p = SRC / (rel + ".rs")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"//! Translated from PostgreSQL src/include/{rel}.h\n")
        written += 1

    # 2. write each directory root file with its child decls
    for d in sorted(dirs):
        rf = root_file(d)
        rf.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        if d == "":
            lines.append('#![doc = "PepperDB: a single-process, async Rust port of PostgreSQL."]')
            lines.append("#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]")
            lines.append("#![allow(dead_code, unused_imports, unused_variables)]")
        elif d in header_paths:
            lines.append(f"//! Translated from PostgreSQL src/include/{d}.h")
        else:
            lines.append(f"//! Directory module: src/include/{d}")
        lines.append("")
        lines.append("// === scaffold: child modules (Phase 0) ===")
        base = (d.split("/")[-1] + "/") if d in header_paths else ""
        for name in sorted(children[d]):
            full = ("/".join([d, name]) if d else name)
            has_header = full in header_paths
            if has_header:                       # header (may also be a subdir root)
                lines.append(decl(name, base, subdir_only=False))
            elif full in dirs:                   # header-less subdir
                lines.append(decl(name, base, subdir_only=True))
        lines.append("// === end scaffold ===")
        rf.write_text("\n".join(lines) + "\n")
        written += 1

    print(f"wrote {written} files; headers={len(header_paths)} dirs={len(dirs)}")


if __name__ == "__main__":
    main()
