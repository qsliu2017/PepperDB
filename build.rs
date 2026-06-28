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

use std::path::{Path, PathBuf};

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

    // Catalog bootstrap codegen (genbki-equivalent), gating decision 2:
    //  - Schema_pg_* attribute descriptor arrays for the formrdesc catalogs
    //    (pg_class/pg_attribute/pg_proc/pg_type), derived from each catalog's
    //    `CATALOG(...)` column list in its .h plus per-column physical type props
    //    resolved from pg_type.dat (this is genbki's `morph_row_for_pgattr`).
    //  - generic .dat seed rows for the M2 bootstrap catalogs (pg_am, pg_namespace,
    //    pg_collation, pg_opclass, pg_amop, pg_amproc).
    let inc = PathBuf::from("ref/postgres/src/include/catalog");
    let types = read_pg_type_props(&inc.join("pg_type.dat"));
    std::fs::write(
        out.join("bootstrap_schema_generated.rs"),
        gen_bootstrap_schemas(&inc, &types),
    )
    .unwrap();
    std::fs::write(
        out.join("bootstrap_seed_generated.rs"),
        gen_bootstrap_seeds(&inc),
    )
    .unwrap();

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
    // utils/adt/int.c (step 02) + bool.c/name.c/varlena.c (step 11):
    // the builtin prosrc -> Rust impl map, kept globally sorted by prosrc.
    ("bool_accum", "crate::backend::utils::adt::bool::bool_accum"),
    ("bool_accum_inv", "crate::backend::utils::adt::bool::bool_accum_inv"),
    ("bool_alltrue", "crate::backend::utils::adt::bool::bool_alltrue"),
    ("bool_anytrue", "crate::backend::utils::adt::bool::bool_anytrue"),
    ("bool_int4", "crate::backend::utils::adt::int::bool_int4"),
    ("booland_statefunc", "crate::backend::utils::adt::bool::booland_statefunc"),
    ("booleq", "crate::backend::utils::adt::bool::booleq"),
    ("boolge", "crate::backend::utils::adt::bool::boolge"),
    ("boolgt", "crate::backend::utils::adt::bool::boolgt"),
    ("boolin", "crate::backend::utils::adt::bool::boolin"),
    ("boolle", "crate::backend::utils::adt::bool::boolle"),
    ("boollt", "crate::backend::utils::adt::bool::boollt"),
    ("boolne", "crate::backend::utils::adt::bool::boolne"),
    ("boolor_statefunc", "crate::backend::utils::adt::bool::boolor_statefunc"),
    ("boolout", "crate::backend::utils::adt::bool::boolout"),
    ("boolrecv", "crate::backend::utils::adt::bool::boolrecv"),
    ("boolsend", "crate::backend::utils::adt::bool::boolsend"),
    ("booltext", "crate::backend::utils::adt::bool::booltext"),
    // access/nbtree/nbtcompare.c (step 13): btree comparison support fns.
    ("btboolcmp", "crate::backend::access::nbtree::nbtcompare::btboolcmp"),
    ("btcharcmp", "crate::backend::access::nbtree::nbtcompare::btcharcmp"),
    ("btcharskipsupport", "crate::backend::access::nbtree::nbtcompare::btcharskipsupport"),
    ("btint24cmp", "crate::backend::access::nbtree::nbtcompare::btint24cmp"),
    ("btint28cmp", "crate::backend::access::nbtree::nbtcompare::btint28cmp"),
    ("btint2cmp", "crate::backend::access::nbtree::nbtcompare::btint2cmp"),
    ("btint2skipsupport", "crate::backend::access::nbtree::nbtcompare::btint2skipsupport"),
    ("btint2sortsupport", "crate::backend::access::nbtree::nbtcompare::btint2sortsupport"),
    ("btint42cmp", "crate::backend::access::nbtree::nbtcompare::btint42cmp"),
    ("btint48cmp", "crate::backend::access::nbtree::nbtcompare::btint48cmp"),
    ("btint4cmp", "crate::backend::access::nbtree::nbtcompare::btint4cmp"),
    ("btint4skipsupport", "crate::backend::access::nbtree::nbtcompare::btint4skipsupport"),
    ("btint4sortsupport", "crate::backend::access::nbtree::nbtcompare::btint4sortsupport"),
    ("btint82cmp", "crate::backend::access::nbtree::nbtcompare::btint82cmp"),
    ("btint84cmp", "crate::backend::access::nbtree::nbtcompare::btint84cmp"),
    ("btint8cmp", "crate::backend::access::nbtree::nbtcompare::btint8cmp"),
    ("btint8skipsupport", "crate::backend::access::nbtree::nbtcompare::btint8skipsupport"),
    ("btint8sortsupport", "crate::backend::access::nbtree::nbtcompare::btint8sortsupport"),
    ("btnamecmp", "crate::backend::utils::adt::name::btnamecmp"),
    ("btoidcmp", "crate::backend::access::nbtree::nbtcompare::btoidcmp"),
    ("btoidskipsupport", "crate::backend::access::nbtree::nbtcompare::btoidskipsupport"),
    ("btoidsortsupport", "crate::backend::access::nbtree::nbtcompare::btoidsortsupport"),
    ("btoidvectorcmp", "crate::backend::access::nbtree::nbtcompare::btoidvectorcmp"),
    ("btnamesortsupport", "crate::backend::utils::adt::name::btnamesortsupport"),
    ("bttextcmp", "crate::backend::utils::adt::varlena::bttextcmp"),
    ("byteacat", "crate::backend::utils::adt::varlena::byteacat"),
    ("byteacmp", "crate::backend::utils::adt::varlena::byteacmp"),
    ("byteaeq", "crate::backend::utils::adt::varlena::byteaeq"),
    ("byteage", "crate::backend::utils::adt::varlena::byteage"),
    ("byteagt", "crate::backend::utils::adt::varlena::byteagt"),
    ("byteain", "crate::backend::utils::adt::varlena::byteain"),
    ("byteale", "crate::backend::utils::adt::varlena::byteale"),
    ("bytealt", "crate::backend::utils::adt::varlena::bytealt"),
    ("byteane", "crate::backend::utils::adt::varlena::byteane"),
    ("byteaoctetlen", "crate::backend::utils::adt::varlena::byteaoctetlen"),
    ("byteaout", "crate::backend::utils::adt::varlena::byteaout"),
    ("bytearecv", "crate::backend::utils::adt::varlena::bytearecv"),
    ("byteasend", "crate::backend::utils::adt::varlena::byteasend"),
    ("generate_series_int4", "crate::backend::utils::adt::int::generate_series_int4"),
    ("generate_series_int4_support", "crate::backend::utils::adt::int::generate_series_int4_support"),
    ("hashbool", "crate::backend::utils::adt::bool::hashbool"),
    ("hashboolextended", "crate::backend::utils::adt::bool::hashboolextended"),
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
    ("nameconcatoid", "crate::backend::utils::adt::name::nameconcatoid"),
    ("nameeq", "crate::backend::utils::adt::name::nameeq"),
    ("namege", "crate::backend::utils::adt::name::namege"),
    ("namegt", "crate::backend::utils::adt::name::namegt"),
    ("namein", "crate::backend::utils::adt::name::namein"),
    ("namele", "crate::backend::utils::adt::name::namele"),
    ("namelt", "crate::backend::utils::adt::name::namelt"),
    ("namene", "crate::backend::utils::adt::name::namene"),
    ("nameout", "crate::backend::utils::adt::name::nameout"),
    ("namerecv", "crate::backend::utils::adt::name::namerecv"),
    ("namesend", "crate::backend::utils::adt::name::namesend"),
    ("text_ge", "crate::backend::utils::adt::varlena::text_ge"),
    ("text_gt", "crate::backend::utils::adt::varlena::text_gt"),
    ("text_le", "crate::backend::utils::adt::varlena::text_le"),
    ("text_lt", "crate::backend::utils::adt::varlena::text_lt"),
    ("text_starts_with", "crate::backend::utils::adt::varlena::text_starts_with"),
    ("text_substr", "crate::backend::utils::adt::varlena::text_substr"),
    ("text_substr_no_len", "crate::backend::utils::adt::varlena::text_substr_no_len"),
    ("textcat", "crate::backend::utils::adt::varlena::textcat"),
    ("texteq", "crate::backend::utils::adt::varlena::texteq"),
    ("textin", "crate::backend::utils::adt::varlena::textin"),
    ("textlen", "crate::backend::utils::adt::varlena::textlen"),
    ("textne", "crate::backend::utils::adt::varlena::textne"),
    ("textoctetlen", "crate::backend::utils::adt::varlena::textoctetlen"),
    ("textout", "crate::backend::utils::adt::varlena::textout"),
    ("textrecv", "crate::backend::utils::adt::varlena::textrecv"),
    ("textsend", "crate::backend::utils::adt::varlena::textsend"),
    ("unknownin", "crate::backend::utils::adt::varlena::unknownin"),
    ("unknownout", "crate::backend::utils::adt::varlena::unknownout"),
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

// ===========================================================================
// Catalog bootstrap codegen (genbki-equivalent) -- gating decision 2.
// ===========================================================================

/// pg_collation.dat C_COLLATION_OID -- the collation morph_row_for_pgattr stamps
/// onto every collatable catalog column (collation-aware catalog columns use C).
const C_COLLATION_OID: u32 = 950;
/// pg_collation.dat DEFAULT_COLLATION_OID -- the `default` symbolic collation.
const DEFAULT_COLLATION_OID: u32 = 100;

/// Physical properties of a pg_type row that genbki copies onto a pg_attribute
/// row (`morph_row_for_pgattr`): the OID + the on-disk layout fields.
struct TypeProps {
    oid: u32,
    typlen: i16,
    typbyval: bool,
    typalign: i8,           // the char value: 'c'/'s'/'i'/'d'
    typstorage: i8,         // 'p'/'e'/'x'/'m'
    typcategory: u8,        // 'A' marks an array type -> attndims = 1
    typcollation: u32,      // 0, or non-0 for collatable types
    array_type_oid: u32,    // element types: OID of the auto-generated array type (0 if none)
}

/// Derive the auto-generated array type's physical props from its element type,
/// matching Catalog.pm `GenerateArrayTypes` + pg_type.h `BKI_ARRAY_DEFAULT`:
///   typlen = -1, typbyval = f, typstorage = 'x' (BKI_ARRAY_DEFAULT),
///   typcategory = 'A', oid = elem.array_type_oid,
///   typalign = 'd' if the element requires double alignment, else 'i',
///   typcollation = copied from the element (no BKI_ARRAY_DEFAULT for it), so the
///                  array is collatable iff its element is.
fn array_type_props(elem_name: &str, elem: &TypeProps) -> TypeProps {
    assert!(
        elem.array_type_oid != 0,
        "pg_type {elem_name} has no array_type_oid but is used as a `[]` column element",
    );
    TypeProps {
        oid: elem.array_type_oid,
        typlen: -1,
        typbyval: false,
        typalign: if elem.typalign == b'd' as i8 { b'd' as i8 } else { b'i' as i8 },
        typstorage: b'x' as i8,
        typcategory: b'A',
        typcollation: elem.typcollation,
        array_type_oid: 0,
    }
}

/// Read pg_type.dat into a typname -> TypeProps map, applying the .dat defaults
/// genbki applies (typbyval=f, typalign='i', typstorage='p', typcollation=0).
fn read_pg_type_props(path: &Path) -> std::collections::HashMap<String, TypeProps> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut map = std::collections::HashMap::new();
    for r in parse_records(&text) {
        let (Some(name), Some(oid)) = (r.get("typname"), r.get("oid")) else { continue };
        let oid: u32 = oid.parse().unwrap_or(0);
        let typlen = parse_typlen(r.get("typlen").map_or("0", String::as_str));
        let typbyval = r.get("typbyval").is_some_and(|s| s == "t");
        let typalign = r.get("typalign").and_then(|s| s.bytes().next()).unwrap_or(b'i') as i8;
        let typstorage = r.get("typstorage").and_then(|s| s.bytes().next()).unwrap_or(b'p') as i8;
        let typcategory = r.get("typcategory").and_then(|s| s.bytes().next()).unwrap_or(0);
        let typcollation =
            r.get("typcollation").map_or(0, |s| parse_collation(s));
        let array_type_oid = r.get("array_type_oid").and_then(|s| s.parse().ok()).unwrap_or(0);
        map.insert(
            name.clone(),
            TypeProps {
                oid,
                typlen,
                typbyval,
                typalign,
                typstorage,
                typcategory,
                typcollation,
                array_type_oid,
            },
        );
    }
    map
}

/// pg_type.dat's typlen is a number or the symbolic `NAMEDATALEN`.
fn parse_typlen(s: &str) -> i16 {
    match s.trim() {
        "NAMEDATALEN" => 64, // matches src/c.rs NAMEDATALEN
        n => n.parse().unwrap_or(0),
    }
}

/// pg_type.dat's typcollation is a numeric OID or one of the symbolic collation
/// names genbki resolves via FindDefinedSymbolFromData (`default` ->
/// DEFAULT_COLLATION_OID, `C` -> C_COLLATION_OID). Only the non-zero/zero
/// distinction is load-bearing for morph_row_for_pgattr's attcollation.
fn parse_collation(s: &str) -> u32 {
    match s.trim() {
        "default" => DEFAULT_COLLATION_OID,
        "C" => C_COLLATION_OID,
        n => n.parse().unwrap_or(0),
    }
}

/// The C type names genbki maps to a differently-named pg_type (Catalog.pm
/// %RENAME_ATTTYPE); every other C type name IS the pg_type name.
fn c_type_to_pg_type(c: &str) -> &str {
    match c {
        "int16" => "int2",
        "int32" => "int4",
        "int64" => "int8",
        "Oid" => "oid",
        "NameData" => "name",
        "TransactionId" => "xid",
        "XLogRecPtr" => "pg_lsn",
        other => other,
    }
}

/// One parsed catalog column.
struct CatColumn {
    name: String,
    pgtype: String,
    is_array: bool,
    force_null: bool,
    force_not_null: bool,
}

/// Parse a catalog header's `CATALOG(<name>,...) { ... }` body into its ordered
/// column list (the genbki "schema").
fn parse_catalog_columns(header: &Path, catalog: &str) -> Vec<CatColumn> {
    let text = std::fs::read_to_string(header)
        .unwrap_or_else(|e| panic!("read {}: {e}", header.display()));
    let marker = format!("CATALOG({catalog},");
    let start = text
        .find(&marker)
        .unwrap_or_else(|| panic!("no {marker} in {}", header.display()));
    let brace = text[start..].find('{').map(|p| start + p + 1).unwrap();
    let end = text[brace..].find("\n}").map(|p| brace + p).unwrap();
    let body = clean_catalog_body(&text[brace..end]);

    let mut cols = Vec::new();
    for raw in body.split(';') {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let mut toks = line.split_whitespace();
        let Some(ctype) = toks.next() else { continue };
        let Some(rawname) = toks.next() else { continue };
        if !ctype.bytes().next().is_some_and(|b| b.is_ascii_alphabetic()) {
            continue;
        }
        let is_array = rawname.contains('[');
        let name: String = rawname.chars().take_while(|&c| c != '[' && c != ';').collect();
        if name.is_empty() {
            continue;
        }
        cols.push(CatColumn {
            name,
            pgtype: c_type_to_pg_type(ctype).to_string(),
            is_array,
            force_null: line.contains("BKI_FORCE_NULL"),
            force_not_null: line.contains("BKI_FORCE_NOT_NULL"),
        });
    }
    cols
}

/// Clean a `CATALOG(...) { ... }` body for column parsing: drop preprocessor
/// directive lines (`#ifdef CATALOG_VARLEN` / `#endif` guarding the varlen tail)
/// and line comments, then strip `/* ... */` block comments (which may span
/// lines). The result is split on `;` into one fragment per column declaration.
fn clean_catalog_body(body: &str) -> String {
    let mut filtered = String::with_capacity(body.len());
    for line in body.lines() {
        let t = line.trim_start();
        if t.starts_with('#') {
            continue; // #ifdef CATALOG_VARLEN / #endif / #ifndef ...
        }
        // Drop a `// ...` line comment tail (none in current headers, but safe).
        let line = line.split("//").next().unwrap_or(line);
        filtered.push_str(line);
        filtered.push('\n');
    }
    // Strip /* ... */ block comments.
    let s = &filtered;
    let mut out = String::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
            if let Some(end) = s[i + 2..].find("*/") {
                i = i + 2 + end + 2;
                continue;
            }
            break;
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

/// Emit the `Schema_pg_*` attribute descriptor arrays for the formrdesc catalogs
/// (the genbki schemapg.h analog), as `BootstrapAttr` literals consumed by
/// `formrdesc`.
fn gen_bootstrap_schemas(
    inc: &Path,
    types: &std::collections::HashMap<String, TypeProps>,
) -> String {
    let mut out = String::from(
        "// Generated by build.rs (genbki schemapg.h analog). Schema_pg_* attribute\n\
         // descriptor arrays for the formrdesc bootstrap catalogs.\n",
    );

    let catalogs: &[(&str, &str)] = &[
        ("pg_type", "pg_type.h"),
        ("pg_attribute", "pg_attribute.h"),
        ("pg_proc", "pg_proc.h"),
        ("pg_class", "pg_class.h"),
    ];

    for (cat, hdr) in catalogs {
        let cols = parse_catalog_columns(&inc.join(hdr), cat);
        let arr = format!("SCHEMA_{}", cat.to_uppercase());
        out.push_str(&format!("pub static {arr}: &[BootstrapAttr] = &[\n"));
        // genbki tracks `priorfixedwidth` left-to-right: it starts true and is
        // `&=`-ed by (attnotnull && attlen > 0) after each column (genbki.pl).
        let mut prior_fixed_width = true;
        for (i, col) in cols.iter().enumerate() {
            let attnum = i + 1;
            // Resolve the column's pg_type exactly as genbki/Catalog.pm does: a
            // `foo[]` column has type `_<elem>` (the auto-generated array type),
            // whose physical fields come from GenerateArrayTypes. Both array and
            // scalar columns then flow through the same morph_row_for_pgattr copy.
            let tp = if col.is_array {
                let elem = types.get(&col.pgtype).unwrap_or_else(|| {
                    panic!(
                        "catalog {cat} array column {} references unknown element pg_type {}",
                        col.name, col.pgtype
                    )
                });
                array_type_props(&col.pgtype, elem)
            } else {
                let tp = types.get(&col.pgtype).unwrap_or_else(|| {
                    panic!(
                        "catalog {cat} column {} references unknown pg_type {}",
                        col.name, col.pgtype
                    )
                });
                TypeProps { array_type_oid: 0, ..*tp }
            };

            let atttypid = tp.oid;
            let attlen = tp.typlen;
            let attbyval = tp.typbyval;
            let attalign = tp.typalign;
            let attstorage = tp.typstorage;
            // morph_row_for_pgattr: attndims = 1 for an array type (typcategory 'A'),
            // else 0.
            let attndims = i16::from(tp.typcategory == b'A');
            // morph_row_for_pgattr: collation-aware columns use C collation; a
            // column is collatable iff its (array or scalar) type's typcollation
            // is non-zero (the array type copies typcollation from its element).
            let attcollation = if tp.typcollation != 0 { C_COLLATION_OID } else { 0 };

            // attnotnull, per genbki morph_row_for_pgattr: FORCE_NOT_NULL -> true,
            // FORCE_NULL -> false, else `priorfixedwidth` (this column and all
            // prior columns are fixed-width AND not-null).
            let fixed = attlen > 0;
            let attnotnull = if col.force_not_null {
                true
            } else if col.force_null {
                false
            } else {
                prior_fixed_width && fixed
            };
            // Update priorfixedwidth for the NEXT column (genbki: `&= attnotnull
            // && attlen > 0`).
            prior_fixed_width &= attnotnull && fixed;

            out.push_str(&format!(
                "    BootstrapAttr {{ attname: \"{}\", atttypid: {atttypid}, attlen: {attlen}, \
                 attbyval: {attbyval}, attalign: {attalign}, attstorage: {attstorage}, \
                 attnotnull: {attnotnull}, attndims: {attndims}, attcollation: {attcollation}, \
                 attnum: {attnum} }},\n",
                col.name,
            ));
        }
        out.push_str("];\n");
        out.push_str(&format!(
            "pub const NATTS_{}: usize = {};\n",
            cat.to_uppercase(),
            cols.len()
        ));
    }
    out
}

/// Emit generic seed rows for the M2 bootstrap catalogs that have a `.dat` file.
/// A seed row is an ordered `&[(column, value)]` exactly as written in the `.dat`.
fn gen_bootstrap_seeds(inc: &Path) -> String {
    let mut out = String::from(
        "// Generated by build.rs (genbki BKI `insert` analog). Generic seed rows\n\
         // for the M2 bootstrap catalogs: each row is an ordered &[(col, value)].\n\
         // value `_null_` denotes SQL NULL; `-` is the regproc/unknown sentinel.\n",
    );

    // The M2-needed catalogs whose initial contents come from a .dat. pg_proc has
    // its own fmgr codegen; pg_class/pg_attribute/pg_type rows that DESCRIBE the
    // catalogs are produced by formrdesc + the schema codegen above, not seeded
    // from a .dat. opclass/amop/amproc seed the int4/oid/name/text btree operator
    // classes so the M2 catalog indexes resolve.
    let m2_catalogs =
        ["pg_am", "pg_namespace", "pg_collation", "pg_opclass", "pg_amop", "pg_amproc"];

    for cat in m2_catalogs {
        let dat = inc.join(format!("{cat}.dat"));
        let const_name = format!("SEED_{}", cat.to_uppercase());
        let Ok(text) = std::fs::read_to_string(&dat) else {
            out.push_str(&format!(
                "// TODO(seed): {cat}.dat not found; no seed rows emitted.\n\
                 pub static {const_name}: &[&[(&str, &str)]] = &[];\n"
            ));
            continue;
        };
        let recs = parse_ordered_records(&text);
        out.push_str(&format!("pub static {const_name}: &[&[(&str, &str)]] = &[\n"));
        for rec in &recs {
            out.push_str("    &[");
            for (k, v) in rec {
                let v = v.replace('\\', "\\\\").replace('"', "\\\"");
                out.push_str(&format!("(\"{k}\", \"{v}\"), "));
            }
            out.push_str("],\n");
        }
        out.push_str("];\n");
    }

    out.push_str(
        "// TODO(seed,M3+): pg_cast, pg_operator, pg_aggregate, pg_range,\n\
         // pg_conversion, pg_language, pg_ts_* -- emit seed rows at their milestone.\n",
    );
    out
}

/// Like `parse_records` but preserves column ORDER within each record (the
/// seed-row codegen needs declaration order, which a HashMap loses).
fn parse_ordered_records(text: &str) -> Vec<Vec<(String, String)>> {
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
        let rec = &buf[start..close];
        let b = rec.as_bytes();
        let mut kv = Vec::new();
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
                kv.push((key, rec[vs..j].to_string()));
                j += 1;
            }
        }
        recs.push(kv);
        i = close + 1;
    }
    recs
}
