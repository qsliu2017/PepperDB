#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""One-time codegen: translate two PG Perl generators into committed Rust.

- errcodes: src/backend/utils/errcodes.txt -> ERRCODE_* consts (computed at
  compile time via make_sqlstate, so the encoding stays single-source).
- wait events: src/backend/utils/activity/wait_event_names.txt -> one #[repr(u32)]
  enum per wait class (discriminant = classid<<24 | index) + name() display string.
  Descriptions are doc-only in PG and omitted (as in the generated C header).

Writes src/utils/errcodes.rs and src/utils/wait_event_types.rs. Run once; the
output is the committed source of truth.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PG = ROOT / "ref/postgres"


def gen_errcodes() -> str:
    src = (PG / "src/backend/utils/errcodes.txt").read_text().splitlines()
    rows = []
    for line in src:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("Section:"):
            continue
        parts = line.split()
        # format: <sqlstate> <E|W|S> <ERRCODE_NAME> [spec_name]
        if len(parts) < 3:
            continue
        sqlstate, _sev, name = parts[0], parts[1], parts[2]
        if not name.startswith("ERRCODE_") or len(sqlstate) != 5:
            continue
        rows.append((name, sqlstate))
    body = "\n".join(f'    ({n}, "{s}"),' for n, s in rows)
    return f'''//! Translated from PostgreSQL src/include/utils/errcodes.h
//!
//! SQLSTATE error codes. In C these `ERRCODE_*` macros are emitted by
//! `generate-errcodes.pl` from `errcodes.txt`. Here the same list drives a
//! `macro_rules!` so each code is computed at compile time via `make_sqlstate`
//! (single source of truth for the 5-char SQLSTATE encoding). Regenerate with
//! ai/harness/gen_flat_tables.py if upstream `errcodes.txt` changes.

/// PGSIXBIT: map a SQLSTATE character to its 6-bit code.
pub const fn pg_six_bit(ch: u8) -> u32 {{
    ((ch.wrapping_sub(b'0')) & 0x3F) as u32
}}

/// MAKE_SQLSTATE: pack five SQLSTATE characters into an int.
pub const fn make_sqlstate(ch1: u8, ch2: u8, ch3: u8, ch4: u8, ch5: u8) -> i32 {{
    (pg_six_bit(ch1)
        + (pg_six_bit(ch2) << 6)
        + (pg_six_bit(ch3) << 12)
        + (pg_six_bit(ch4) << 18)
        + (pg_six_bit(ch5) << 24)) as i32
}}

/// make_sqlstate over a 5-byte SQLSTATE string.
pub const fn make_sqlstate_str(s: &[u8]) -> i32 {{
    make_sqlstate(s[0], s[1], s[2], s[3], s[4])
}}

macro_rules! define_errcodes {{
    ($(($name:ident, $code:literal)),* $(,)?) => {{
        $( pub const $name: i32 = make_sqlstate_str($code.as_bytes()); )*
    }};
}}

define_errcodes! {{
{body}
}}
'''


CLASS_ID = {
    "WaitEventActivity": 0x05,
    "WaitEventClient": 0x06,
    "WaitEventIPC": 0x08,
    "WaitEventTimeout": 0x09,
    "WaitEventIO": 0x0A,
    "WaitEventBufferPin": 0x04,
    "WaitEventExtension": 0x07,
    "WaitEventLWLock": 0x01,
    "WaitEventLock": 0x03,
}


def camel(sym: str) -> str:
    # ARCHIVER_MAIN -> ArchiverMain ; already-CamelCase names (LWLock/Lock) pass through.
    if "_" in sym or sym.isupper():
        return "".join(w.capitalize() for w in sym.split("_"))
    return sym[:1].upper() + sym[1:]


def gen_wait_events() -> str:
    src = (PG / "src/backend/utils/activity/wait_event_names.txt").read_text().splitlines()
    classes: dict[str, list[str]] = {}
    cur = None
    for line in src:
        s = line.strip()
        if s.startswith("Section: ClassName -"):
            cur = s.split("-", 1)[1].strip()
            classes.setdefault(cur, [])
            continue
        if not s or s.startswith("#") or cur is None:
            continue
        if s.startswith("ABI_compatibility"):
            continue
        sym = s.split("\t", 1)[0].strip()
        if sym:
            classes[cur].append(sym)

    out = ['''//! Translated from PostgreSQL src/include/utils/wait_event_types.h
//!
//! Wait-event enums. In C `generate-wait_event_types.pl` emits these from
//! `wait_event_names.txt`; here one `#[repr(u32)]` enum per wait class, each
//! discriminant = `(class_id << 24) | index` (matching PG's wait_event_info
//! encoding). `name()` returns the display string pg_stat shows. Descriptions are
//! documentation-only in PG and omitted. Regenerate with ai/harness/gen_flat_tables.py.
''']
    for cls, syms in classes.items():
        cid = CLASS_ID.get(cls)
        if cid is None or not syms:
            continue
        variants, names = [], []
        for i, sym in enumerate(syms):
            v = camel(sym)
            val = (cid << 24) | i
            variants.append(f"    {v} = 0x{val:08X},")
            names.append(f"            {cls}::{v} => \"{v}\",")
        out.append(f"""
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum {cls} {{
{chr(10).join(variants)}
}}

impl {cls} {{
    pub fn name(self) -> &'static str {{
        match self {{
{chr(10).join(names)}
        }}
    }}
}}""")
    return "\n".join(out) + "\n"


(ROOT / "src/utils/errcodes.rs").write_text(gen_errcodes())
(ROOT / "src/utils/wait_event_types.rs").write_text(gen_wait_events())
print("wrote src/utils/errcodes.rs and src/utils/wait_event_types.rs")
