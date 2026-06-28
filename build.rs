#![allow(clippy::format_push_string, reason = "build-time codegen string assembly")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "build-time tooling: panicking on a broken build env (missing submodule / malformed .dat) is the correct fail-fast; the error model governs the runtime backend, not build scripts"
)]
//! Build-time generators for data-driven PostgreSQL tables kept as upstream `.dat`
//! files (we read them from the `ref/postgres` submodule, the single source of
//! truth). Per the project's mechanism rule, build.rs is used only for these
//! `.dat`-backed families; the flat-list families are Rust macros instead.
//!
//! Families: the fmgr tables (Gen_fmgrtab.pl) from `catalog/pg_proc.dat`, and the
//! catalog symbolic OID constants (genbki.pl) from the `catalog/pg_*.dat` rows.

use std::path::PathBuf;

fn main() {
    let pg = PathBuf::from("ref/postgres/src/include/catalog");
    let proc_dat = pg.join("pg_proc.dat");
    println!("cargo:rerun-if-changed={}", proc_dat.display());
    println!("cargo:rerun-if-changed=build.rs");

    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let text = std::fs::read_to_string(&proc_dat)
        .unwrap_or_else(|e| panic!("read {}: {e}", proc_dat.display()));
    let procs = parse_dat(&text);

    std::fs::write(out.join("fmgroids_generated.rs"), gen_fmgroids(&procs)).unwrap();
    std::fs::write(out.join("fmgrtab_generated.rs"), gen_fmgrtab(&procs)).unwrap();

    // Catalog symbolic OID consts from every catalog/*.dat (genbki OID symbols).
    let mut dats: Vec<PathBuf> = std::fs::read_dir(&pg)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "dat"))
        .collect();
    dats.sort();
    for d in &dats {
        println!("cargo:rerun-if-changed={}", d.display());
    }
    std::fs::write(out.join("catalog_oids_generated.rs"), gen_catalog_oids(&dats)).unwrap();

    // Grammar (gram.y analog): process every .lalrpop under src/ into OUT_DIR
    // (mirroring the source path), so gram.rs can include it via lalrpop_mod!.
    // Isolated from the .dat codegen above. lalrpop emits no rerun directives, so
    // declare the grammar as an input ourselves.
    println!("cargo:rerun-if-changed=src/backend/parser/gram.lalrpop");
    lalrpop::process_root().unwrap();
}

struct Proc {
    oid: u32,
    proname: String,
    proargtypes: String,
    strict: bool,
    retset: bool,
    lang: String,
    prosrc: String,
    nargs: i16,
}

/// Minimal parser for pg_proc.dat: `{ key => 'value', ... },` records (flat, no
/// nested braces), with `#` comment lines. Good enough for the fields fmgr needs.
fn parse_dat(text: &str) -> Vec<Proc> {
    // Drop comment lines (first non-space char is '#').
    let mut buf = String::with_capacity(text.len());
    for line in text.lines() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        buf.push_str(line);
        buf.push('\n');
    }

    let bytes = buf.as_bytes();
    let mut procs = Vec::new();
    let mut i = 0;
    while let Some(open) = buf[i..].find('{') {
        let start = i + open + 1;
        let close = buf[start..].find('}').map(|p| start + p);
        let Some(close) = close else { break };
        procs.push(parse_record(&buf[start..close]));
        i = close + 1;
        let _ = bytes;
    }
    procs.into_iter().flatten().collect()
}

fn parse_record(rec: &str) -> Option<Proc> {
    let mut kv = std::collections::HashMap::new();
    let b = rec.as_bytes();
    let mut i = 0;
    while i < b.len() {
        // find an identifier key
        if !(b[i] as char).is_ascii_alphabetic() {
            i += 1;
            continue;
        }
        let ks = i;
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        let key = &rec[ks..i];
        // expect '=>'
        while i < b.len() && (b[i] == b' ' || b[i] == b'\t' || b[i] == b'\n') {
            i += 1;
        }
        if !(i + 1 < b.len() && b[i] == b'=' && b[i + 1] == b'>') {
            continue;
        }
        i += 2;
        while i < b.len() && (b[i] == b' ' || b[i] == b'\t' || b[i] == b'\n') {
            i += 1;
        }
        if i < b.len() && b[i] == b'\'' {
            i += 1;
            let vs = i;
            while i < b.len() && b[i] != b'\'' {
                if b[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            kv.insert(key.to_string(), rec[vs..i].to_string());
            i += 1; // closing quote
        }
    }

    let oid: u32 = kv.get("oid")?.parse().ok()?;
    let proname = kv.get("proname")?.clone();
    let proargtypes = kv.get("proargtypes").cloned().unwrap_or_default();
    let nargs = match kv.get("pronargs") {
        Some(n) => n.parse().unwrap_or(0),
        None if proargtypes.trim().is_empty() => 0,
        None => proargtypes.split_whitespace().count() as i16,
    };
    Some(Proc {
        oid,
        proname,
        proargtypes,
        strict: kv.get("proisstrict").is_some_and(|s| s == "t"),
        retset: kv.get("proretset").is_some_and(|s| s == "t"),
        lang: kv.get("prolang").cloned().unwrap_or_else(|| "internal".into()),
        prosrc: kv.get("prosrc").cloned().unwrap_or_default(),
        nargs,
    })
}

/// Sanitize a string into an upper-case Rust identifier fragment.
fn ident(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .to_uppercase()
}

fn gen_fmgroids(procs: &[Proc]) -> String {
    use std::collections::HashMap;
    let mut counts: HashMap<&str, u32> = HashMap::new();
    for p in procs {
        *counts.entry(p.proname.as_str()).or_default() += 1;
    }
    let mut sorted: Vec<&Proc> = procs.iter().collect();
    sorted.sort_by_key(|p| p.oid);

    let mut seen: HashMap<String, u32> = HashMap::new();
    let mut out = String::from(
        "// Generated by build.rs from pg_proc.dat (Gen_fmgrtab.pl). F_<proname>;\n\
         // overloaded pronames get _<proargtypes> appended.\n",
    );
    for p in &sorted {
        let mut name = p.proname.clone();
        if counts[p.proname.as_str()] > 1 {
            name.push('_');
            name.push_str(&p.proargtypes);
        }
        let mut sym = format!("F_{}", ident(&name));
        let n = seen.entry(sym.clone()).or_insert(0);
        if *n > 0 {
            sym = format!("{sym}_{}", p.oid); // disambiguate residual collisions
        }
        *n += 1;
        out.push_str(&format!("pub const {sym}: Oid = Oid({});\n", p.oid));
    }
    out
}

/// Curated map from a builtin's `prosrc` (its internal C-function name in
/// pg_proc.dat, also the `func_name` column of `fmgr_builtins`) to the Rust path
/// of the translated implementation. This is the Rust analog of Gen_fmgrtab.pl
/// emitting `func: int4out` from fmgrprotos.h: there the prototype header binds
/// every builtin; here we bind them one milestone at a time as each
/// `utils/adt/*` leaf is translated.
///
/// A present entry makes the table row `func: Some(<path> as PGFunction)`; an
/// absent name stays `func: None` (reached -> ERROR "function returned NULL" /
/// the not-yet-bound path, exactly as an unimplemented builtin should).
///
/// Step 01 implements NO builtins, so the map is empty. Step 02 adds entries as
/// the type I/O functions land, e.g.:
///
///     ("int4out", "crate::backend::utils::adt::int::int4out"),
///     ("int4in",  "crate::backend::utils::adt::int::int4in"),
///
/// Keep this sorted by prosrc name. It scales to hundreds of builtins as a flat
/// compile-time table of fn-path references (the faithful analog) -- do NOT swap
/// it for a runtime inventory/linkme registration scheme.
const BUILTIN_FN_BINDINGS: &[(&str, &str)] = &[
    // utils/adt/int.c (step 02). Functions reaching not-yet-translated
    // subsystems (recv/send, int2vector, generate_series SRF, support node)
    // bind to their real Rust fn, which `unimplemented!()`s when invoked --
    // exactly the not-yet-runnable behavior, while still wiring the table.
    ("bool_int4", "crate::backend::utils::adt::int::bool_int4"),
    ("generate_series_int4", "crate::backend::utils::adt::int::generate_series_int4"),
    ("generate_series_int4_support", "crate::backend::utils::adt::int::generate_series_int4_support"),
    ("i2toi4", "crate::backend::utils::adt::int::i2toi4"),
    ("i4toi2", "crate::backend::utils::adt::int::i4toi2"),
    ("in_range_int2_int2", "crate::backend::utils::adt::int::in_range_int2_int2"),
    ("in_range_int2_int4", "crate::backend::utils::adt::int::in_range_int2_int4"),
    ("in_range_int2_int8", "crate::backend::utils::adt::int::in_range_int2_int8"),
    ("in_range_int4_int2", "crate::backend::utils::adt::int::in_range_int4_int2"),
    ("in_range_int4_int4", "crate::backend::utils::adt::int::in_range_int4_int4"),
    ("in_range_int4_int8", "crate::backend::utils::adt::int::in_range_int4_int8"),
    ("int24div", "crate::backend::utils::adt::int::int24div"),
    ("int24eq", "crate::backend::utils::adt::int::int24eq"),
    ("int24ge", "crate::backend::utils::adt::int::int24ge"),
    ("int24gt", "crate::backend::utils::adt::int::int24gt"),
    ("int24le", "crate::backend::utils::adt::int::int24le"),
    ("int24lt", "crate::backend::utils::adt::int::int24lt"),
    ("int24mi", "crate::backend::utils::adt::int::int24mi"),
    ("int24mul", "crate::backend::utils::adt::int::int24mul"),
    ("int24ne", "crate::backend::utils::adt::int::int24ne"),
    ("int24pl", "crate::backend::utils::adt::int::int24pl"),
    ("int2abs", "crate::backend::utils::adt::int::int2abs"),
    ("int2and", "crate::backend::utils::adt::int::int2and"),
    ("int2div", "crate::backend::utils::adt::int::int2div"),
    ("int2eq", "crate::backend::utils::adt::int::int2eq"),
    ("int2ge", "crate::backend::utils::adt::int::int2ge"),
    ("int2gt", "crate::backend::utils::adt::int::int2gt"),
    ("int2in", "crate::backend::utils::adt::int::int2in"),
    ("int2larger", "crate::backend::utils::adt::int::int2larger"),
    ("int2le", "crate::backend::utils::adt::int::int2le"),
    ("int2lt", "crate::backend::utils::adt::int::int2lt"),
    ("int2mi", "crate::backend::utils::adt::int::int2mi"),
    ("int2mod", "crate::backend::utils::adt::int::int2mod"),
    ("int2mul", "crate::backend::utils::adt::int::int2mul"),
    ("int2ne", "crate::backend::utils::adt::int::int2ne"),
    ("int2not", "crate::backend::utils::adt::int::int2not"),
    ("int2or", "crate::backend::utils::adt::int::int2or"),
    ("int2out", "crate::backend::utils::adt::int::int2out"),
    ("int2pl", "crate::backend::utils::adt::int::int2pl"),
    ("int2recv", "crate::backend::utils::adt::int::int2recv"),
    ("int2send", "crate::backend::utils::adt::int::int2send"),
    ("int2shl", "crate::backend::utils::adt::int::int2shl"),
    ("int2shr", "crate::backend::utils::adt::int::int2shr"),
    ("int2smaller", "crate::backend::utils::adt::int::int2smaller"),
    ("int2um", "crate::backend::utils::adt::int::int2um"),
    ("int2up", "crate::backend::utils::adt::int::int2up"),
    ("int2vectorin", "crate::backend::utils::adt::int::int2vectorin"),
    ("int2vectorout", "crate::backend::utils::adt::int::int2vectorout"),
    ("int2vectorrecv", "crate::backend::utils::adt::int::int2vectorrecv"),
    ("int2vectorsend", "crate::backend::utils::adt::int::int2vectorsend"),
    ("int2xor", "crate::backend::utils::adt::int::int2xor"),
    ("int42div", "crate::backend::utils::adt::int::int42div"),
    ("int42eq", "crate::backend::utils::adt::int::int42eq"),
    ("int42ge", "crate::backend::utils::adt::int::int42ge"),
    ("int42gt", "crate::backend::utils::adt::int::int42gt"),
    ("int42le", "crate::backend::utils::adt::int::int42le"),
    ("int42lt", "crate::backend::utils::adt::int::int42lt"),
    ("int42mi", "crate::backend::utils::adt::int::int42mi"),
    ("int42mul", "crate::backend::utils::adt::int::int42mul"),
    ("int42ne", "crate::backend::utils::adt::int::int42ne"),
    ("int42pl", "crate::backend::utils::adt::int::int42pl"),
    ("int4_bool", "crate::backend::utils::adt::int::int4_bool"),
    ("int4abs", "crate::backend::utils::adt::int::int4abs"),
    ("int4and", "crate::backend::utils::adt::int::int4and"),
    ("int4div", "crate::backend::utils::adt::int::int4div"),
    ("int4eq", "crate::backend::utils::adt::int::int4eq"),
    ("int4gcd", "crate::backend::utils::adt::int::int4gcd"),
    ("int4ge", "crate::backend::utils::adt::int::int4ge"),
    ("int4gt", "crate::backend::utils::adt::int::int4gt"),
    ("int4in", "crate::backend::utils::adt::int::int4in"),
    ("int4inc", "crate::backend::utils::adt::int::int4inc"),
    ("int4larger", "crate::backend::utils::adt::int::int4larger"),
    ("int4lcm", "crate::backend::utils::adt::int::int4lcm"),
    ("int4le", "crate::backend::utils::adt::int::int4le"),
    ("int4lt", "crate::backend::utils::adt::int::int4lt"),
    ("int4mi", "crate::backend::utils::adt::int::int4mi"),
    ("int4mod", "crate::backend::utils::adt::int::int4mod"),
    ("int4mul", "crate::backend::utils::adt::int::int4mul"),
    ("int4ne", "crate::backend::utils::adt::int::int4ne"),
    ("int4not", "crate::backend::utils::adt::int::int4not"),
    ("int4or", "crate::backend::utils::adt::int::int4or"),
    ("int4out", "crate::backend::utils::adt::int::int4out"),
    ("int4pl", "crate::backend::utils::adt::int::int4pl"),
    ("int4recv", "crate::backend::utils::adt::int::int4recv"),
    ("int4send", "crate::backend::utils::adt::int::int4send"),
    ("int4shl", "crate::backend::utils::adt::int::int4shl"),
    ("int4shr", "crate::backend::utils::adt::int::int4shr"),
    ("int4smaller", "crate::backend::utils::adt::int::int4smaller"),
    ("int4um", "crate::backend::utils::adt::int::int4um"),
    ("int4up", "crate::backend::utils::adt::int::int4up"),
    ("int4xor", "crate::backend::utils::adt::int::int4xor"),
];

fn builtin_fn_path(prosrc: &str) -> Option<&'static str> {
    BUILTIN_FN_BINDINGS
        .iter()
        .find(|(name, _)| *name == prosrc)
        .map(|(_, path)| *path)
}

fn gen_fmgrtab(procs: &[Proc]) -> String {
    let mut builtins: Vec<&Proc> = procs.iter().filter(|p| p.lang == "internal").collect();
    builtins.sort_by_key(|p| p.oid);
    let last_oid = builtins.last().map_or(0, |p| p.oid);

    let mut out = String::from("// Generated by build.rs from pg_proc.dat (internal-language builtins).\n");
    out.push_str("pub static fmgr_builtins: &[FmgrBuiltin] = &[\n");
    for p in &builtins {
        // Bind the Rust implementation if curated; else None (Gen_fmgrtab.pl
        // emits the symbol from fmgrprotos.h -- we use the BUILTIN_FN_BINDINGS map).
        let func = builtin_fn_path(&p.prosrc)
            .map_or_else(|| "None".to_string(), |path| format!("Some({path} as PGFunction)"));
        out.push_str(&format!(
            "    FmgrBuiltin {{ foid: Oid({}), nargs: {}, strict: {}, retset: {}, func_name: \"{}\", func: {} }},\n",
            p.oid, p.nargs, p.strict, p.retset, p.prosrc, func
        ));
    }
    out.push_str("];\n");
    out.push_str(&format!("pub const fmgr_nbuiltins: usize = {};\n", builtins.len()));
    out.push_str(&format!("pub const fmgr_last_builtin_oid: Oid = Oid({last_oid});\n"));

    // oid -> index map (InvalidOidBuiltinMapping for unused oids), like C.
    let mut index = vec![u16::MAX; (last_oid as usize) + 1];
    for (i, p) in builtins.iter().enumerate() {
        index[p.oid as usize] = i as u16;
    }
    out.push_str("pub static fmgr_builtin_oid_index: &[u16] = &[\n");
    for chunk in index.chunks(20) {
        out.push_str("    ");
        for v in chunk {
            out.push_str(&format!("{v},"));
        }
        out.push('\n');
    }
    out.push_str("];\n");
    out
}

/// Generic `.dat` record reader: returns each `{ k => 'v', ... }` as a key->value
/// map (flat records, no nested braces - true for all catalog .dat files).
fn parse_records(text: &str) -> Vec<std::collections::HashMap<String, String>> {
    let mut buf = String::with_capacity(text.len());
    for line in text.lines() {
        if line.trim_start().starts_with('#') {
            continue;
        }
        buf.push_str(line);
        buf.push('\n');
    }
    let mut recs = Vec::new();
    let mut i = 0;
    while let Some(open) = buf[i..].find('{') {
        let start = i + open + 1;
        let Some(close) = buf[start..].find('}').map(|p| start + p) else { break };
        let mut kv = std::collections::HashMap::new();
        let rec = &buf[start..close];
        let b = rec.as_bytes();
        let mut j = 0;
        while j < b.len() {
            if !(b[j] as char).is_ascii_alphabetic() {
                j += 1;
                continue;
            }
            let ks = j;
            while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                j += 1;
            }
            let key = rec[ks..j].to_string();
            while j < b.len() && matches!(b[j], b' ' | b'\t' | b'\n') {
                j += 1;
            }
            if !(j + 1 < b.len() && b[j] == b'=' && b[j + 1] == b'>') {
                continue;
            }
            j += 2;
            while j < b.len() && matches!(b[j], b' ' | b'\t' | b'\n') {
                j += 1;
            }
            if j < b.len() && b[j] == b'\'' {
                j += 1;
                let vs = j;
                while j < b.len() && b[j] != b'\'' {
                    if b[j] == b'\\' {
                        j += 1;
                    }
                    j += 1;
                }
                kv.insert(key, rec[vs..j].to_string());
                j += 1;
            }
        }
        recs.push(kv);
        i = close + 1;
    }
    recs
}

/// Emit the catalog symbolic OID constants genbki writes into the `pg_*_d.h`
/// headers: every row's explicit `oid_symbol`, plus pg_type's typname-derived
/// `<TYPNAME>OID` (and `<TYPNAME>ARRAYOID` for rows with `array_type_oid`).
fn gen_catalog_oids(dats: &[PathBuf]) -> String {
    let mut out = String::from(
        "// Generated by build.rs from catalog/*.dat (genbki OID symbols).\n",
    );
    let mut seen = std::collections::HashSet::new();
    let mut emit = |out: &mut String, name: &str, oid: &str| {
        if name.is_empty() || !seen.insert(name.to_string()) {
            return;
        }
        out.push_str(&format!("pub const {name}: Oid = Oid({oid});\n"));
    };

    for d in dats {
        let text = std::fs::read_to_string(d).unwrap();
        let is_type = d.file_name().is_some_and(|n| n == "pg_type.dat");
        for r in parse_records(&text) {
            let Some(oid) = r.get("oid") else { continue };
            if let Some(sym) = r.get("oid_symbol") {
                emit(&mut out, sym, oid);
            }
            if is_type
                && let Some(typname) = r.get("typname") {
                    let up = typname.to_uppercase();
                    emit(&mut out, &format!("{up}OID"), oid);
                    if let Some(arr) = r.get("array_type_oid") {
                        emit(&mut out, &format!("{up}ARRAYOID"), arr);
                    }
                }
        }
    }
    out
}
