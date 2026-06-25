#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Roll out #[derive(pepperdb_derive::Catalog)] across catalog modules.

For each src/catalog/pg_*.rs carrying a `// TODO(catalog-derive)` block:
  1. add the derive to the `FormData_pg_<name>` struct, and
  2. delete the hand-emitted `// TODO(catalog-derive) .. Natts_*` const block
     (the derive now produces those Anum_*/Natts_* consts from field order).

Reports any file where the struct's field count differs from the former hand
`Natts_*` value (so the change can be spot-checked against the C source).
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CAT = ROOT / "src/catalog"

DERIVE = "#[derive(pepperdb_derive::Catalog)]"

mismatches = []
changed = 0
for f in sorted(CAT.glob("pg_*.rs")):
    lines = f.read_text().splitlines()
    if not any("TODO(catalog-derive)" in l for l in lines):
        continue

    # locate the FormData struct line
    struct_idx = next(
        (i for i, l in enumerate(lines) if l.lstrip().startswith("pub struct FormData_pg_")),
        None,
    )
    if struct_idx is None:
        print(f"SKIP {f.name}: no FormData struct")
        continue

    # count struct fields (lines `pub <name>:` until the closing brace)
    n_fields = 0
    j = struct_idx + 1
    depth = lines[struct_idx].count("{")
    while j < len(lines) and depth > 0:
        depth += lines[j].count("{") - lines[j].count("}")
        m = re.match(r"\s*pub\s+([A-Za-z_][A-Za-z0-9_#]*)\s*:", lines[j])
        if m and depth >= 1:
            n_fields += 1
        j += 1

    # find the TODO block: from the TODO line to the last contiguous Natts_ line
    todo_idx = next(i for i, l in enumerate(lines) if "TODO(catalog-derive)" in l)
    end = todo_idx
    hand_natts = None
    k = todo_idx + 1
    while k < len(lines):
        s = lines[k].strip()
        if s.startswith("pub const Anum_"):
            end = k
        elif s.startswith("pub const Natts_"):
            end = k
            mm = re.search(r"=\s*(\d+)", s)
            hand_natts = int(mm.group(1)) if mm else None
            break
        elif s == "":
            k += 1
            continue
        else:
            break
        k += 1

    if hand_natts is not None and hand_natts != n_fields:
        mismatches.append((f.name, hand_natts, n_fields))

    # delete [todo_idx, end]; also drop a single trailing blank if present
    del lines[todo_idx : end + 1]
    if todo_idx < len(lines) and lines[todo_idx].strip() == "" and \
       todo_idx > 0 and lines[todo_idx - 1].strip() == "":
        del lines[todo_idx]

    # insert the derive just before the struct line (after any #[repr(C)])
    lines.insert(struct_idx, DERIVE)

    f.write_text("\n".join(lines) + "\n")
    changed += 1

print(f"applied derive to {changed} catalog files")
if mismatches:
    print("FIELD-COUNT vs hand-Natts MISMATCHES (spot-check these):")
    for name, hn, nf in mismatches:
        print(f"  {name}: hand Natts={hn}, struct fields={nf}")
else:
    print("no field-count mismatches")
