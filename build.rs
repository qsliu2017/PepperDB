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

    // M3: resolved seed rows for pg_proc + pg_operator (the int4 arithmetic /
    // comparison operators and their support functions) -- the operator/function
    // resolution path's catalogs. Symbolic references (type names, proc names) are
    // resolved to OIDs here in build.rs from the .dat files (genbki BKI_LOOKUP).
    std::fs::write(
        out.join("bootstrap_m3_seed_generated.rs"),
        gen_m3_operator_proc_seeds(&inc, &types),
    )
    .unwrap();

    // M4: resolved pg_cast + cast-support pg_proc seed rows (step 23) -- the
    // numeric/int/float cast tower. Reuses the M3Proc struct shape; relies on the
    // M3 seed being included first (it defines M3Proc).
    std::fs::write(
        out.join("bootstrap_m4_cast_seed_generated.rs"),
        gen_m4_cast_seeds(&inc, &types),
    )
    .unwrap();

    // M5 (step 25B): resolved pg_aggregate + agg/transfn pg_proc seed rows for the
    // common aggregates (count/sum/min/max/avg). Reuses the M3Proc struct shape for
    // the proc rows. Symbolic aggfnoid/transfn/finalfn names resolved to OIDs here.
    std::fs::write(
        out.join("bootstrap_m5_agg_seed_generated.rs"),
        gen_m5_aggregate_seeds(&inc, &types),
    )
    .unwrap();

    // M12 (step 42): resolved pg_proc seed rows for the built-in window functions
    // (row_number/rank/dense_rank/.../lag/lead/first_value/...), prokind 'w'. Reuses
    // the M3Proc struct shape; relies on the M3 seed being included first.
    std::fs::write(
        out.join("bootstrap_m12_window_seed_generated.rs"),
        gen_m12_window_seeds(&inc, &types),
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
        // pg_proc.h: proisstrict BKI_DEFAULT(t) -- absent means strict.
        strict: kv.get("proisstrict").is_none_or(|s| s == "t"),
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
         // overloaded pronames get _<proargtypes> appended.\n\
         // Emitted as free consts AND associated consts on Oid (matchable as Oid::F_*).\n",
    );
    let mut assoc = String::from("impl Oid {\n");
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
        out.push_str(&format!("pub const {sym}: Oid = Oid::new({});\n", p.oid));
        assoc.push_str(&format!("    pub const {sym}: Self = Self::new({});\n", p.oid));
    }
    assoc.push_str("}\n");
    out.push_str(&assoc);
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
    ("btfloat48cmp", "crate::backend::utils::adt::float::btfloat48cmp"),
    ("btfloat4cmp", "crate::backend::utils::adt::float::btfloat4cmp"),
    ("btfloat4sortsupport", "crate::backend::utils::adt::float::btfloat4sortsupport"),
    ("btfloat84cmp", "crate::backend::utils::adt::float::btfloat84cmp"),
    ("btfloat8cmp", "crate::backend::utils::adt::float::btfloat8cmp"),
    ("btfloat8sortsupport", "crate::backend::utils::adt::float::btfloat8sortsupport"),
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
    ("char_text", "crate::backend::utils::adt::char::char_text"),
    ("chareq", "crate::backend::utils::adt::char::chareq"),
    ("charge", "crate::backend::utils::adt::char::charge"),
    ("chargt", "crate::backend::utils::adt::char::chargt"),
    ("charin", "crate::backend::utils::adt::char::charin"),
    ("charle", "crate::backend::utils::adt::char::charle"),
    ("charlt", "crate::backend::utils::adt::char::charlt"),
    ("charne", "crate::backend::utils::adt::char::charne"),
    ("charout", "crate::backend::utils::adt::char::charout"),
    ("charrecv", "crate::backend::utils::adt::char::charrecv"),
    ("charsend", "crate::backend::utils::adt::char::charsend"),
    ("chartoi4", "crate::backend::utils::adt::char::chartoi4"),
    ("dacos", "crate::backend::utils::adt::float::dacos"),
    ("dacosd", "crate::backend::utils::adt::float::dacosd"),
    ("dacosh", "crate::backend::utils::adt::float::dacosh"),
    ("dasin", "crate::backend::utils::adt::float::dasin"),
    ("dasind", "crate::backend::utils::adt::float::dasind"),
    ("dasinh", "crate::backend::utils::adt::float::dasinh"),
    ("datan", "crate::backend::utils::adt::float::datan"),
    ("datan2", "crate::backend::utils::adt::float::datan2"),
    ("datan2d", "crate::backend::utils::adt::float::datan2d"),
    ("datand", "crate::backend::utils::adt::float::datand"),
    ("datanh", "crate::backend::utils::adt::float::datanh"),
    ("dcbrt", "crate::backend::utils::adt::float::dcbrt"),
    ("dceil", "crate::backend::utils::adt::float::dceil"),
    ("dcos", "crate::backend::utils::adt::float::dcos"),
    ("dcosd", "crate::backend::utils::adt::float::dcosd"),
    ("dcosh", "crate::backend::utils::adt::float::dcosh"),
    ("dcot", "crate::backend::utils::adt::float::dcot"),
    ("dcotd", "crate::backend::utils::adt::float::dcotd"),
    ("degrees", "crate::backend::utils::adt::float::degrees"),
    ("derf", "crate::backend::utils::adt::float::derf"),
    ("derfc", "crate::backend::utils::adt::float::derfc"),
    ("dexp", "crate::backend::utils::adt::float::dexp"),
    ("dfloor", "crate::backend::utils::adt::float::dfloor"),
    ("dgamma", "crate::backend::utils::adt::float::dgamma"),
    ("dlgamma", "crate::backend::utils::adt::float::dlgamma"),
    ("dlog1", "crate::backend::utils::adt::float::dlog1"),
    ("dlog10", "crate::backend::utils::adt::float::dlog10"),
    ("dpi", "crate::backend::utils::adt::float::dpi"),
    ("dpow", "crate::backend::utils::adt::float::dpow"),
    ("dround", "crate::backend::utils::adt::float::dround"),
    ("dsign", "crate::backend::utils::adt::float::dsign"),
    ("dsin", "crate::backend::utils::adt::float::dsin"),
    ("dsind", "crate::backend::utils::adt::float::dsind"),
    ("dsinh", "crate::backend::utils::adt::float::dsinh"),
    ("dsqrt", "crate::backend::utils::adt::float::dsqrt"),
    ("dtan", "crate::backend::utils::adt::float::dtan"),
    ("dtand", "crate::backend::utils::adt::float::dtand"),
    ("dtanh", "crate::backend::utils::adt::float::dtanh"),
    ("dtof", "crate::backend::utils::adt::float::dtof"),
    ("dtoi2", "crate::backend::utils::adt::float::dtoi2"),
    ("dtoi4", "crate::backend::utils::adt::float::dtoi4"),
    ("dtrunc", "crate::backend::utils::adt::float::dtrunc"),
    ("float48div", "crate::backend::utils::adt::float::float48div"),
    ("float48eq", "crate::backend::utils::adt::float::float48eq"),
    ("float48ge", "crate::backend::utils::adt::float::float48ge"),
    ("float48gt", "crate::backend::utils::adt::float::float48gt"),
    ("float48le", "crate::backend::utils::adt::float::float48le"),
    ("float48lt", "crate::backend::utils::adt::float::float48lt"),
    ("float48mi", "crate::backend::utils::adt::float::float48mi"),
    ("float48mul", "crate::backend::utils::adt::float::float48mul"),
    ("float48ne", "crate::backend::utils::adt::float::float48ne"),
    ("float48pl", "crate::backend::utils::adt::float::float48pl"),
    ("float4abs", "crate::backend::utils::adt::float::float4abs"),
    ("float4div", "crate::backend::utils::adt::float::float4div"),
    ("float4eq", "crate::backend::utils::adt::float::float4eq"),
    ("float4ge", "crate::backend::utils::adt::float::float4ge"),
    ("float4gt", "crate::backend::utils::adt::float::float4gt"),
    ("float4in", "crate::backend::utils::adt::float::float4in"),
    ("float4larger", "crate::backend::utils::adt::float::float4larger"),
    ("float4le", "crate::backend::utils::adt::float::float4le"),
    ("float4lt", "crate::backend::utils::adt::float::float4lt"),
    ("float4mi", "crate::backend::utils::adt::float::float4mi"),
    ("float4mul", "crate::backend::utils::adt::float::float4mul"),
    ("float4ne", "crate::backend::utils::adt::float::float4ne"),
    ("float4out", "crate::backend::utils::adt::float::float4out"),
    ("float4pl", "crate::backend::utils::adt::float::float4pl"),
    ("float4recv", "crate::backend::utils::adt::float::float4recv"),
    ("float4send", "crate::backend::utils::adt::float::float4send"),
    ("float4smaller", "crate::backend::utils::adt::float::float4smaller"),
    ("float4um", "crate::backend::utils::adt::float::float4um"),
    ("float4up", "crate::backend::utils::adt::float::float4up"),
    ("float84div", "crate::backend::utils::adt::float::float84div"),
    ("float84eq", "crate::backend::utils::adt::float::float84eq"),
    ("float84ge", "crate::backend::utils::adt::float::float84ge"),
    ("float84gt", "crate::backend::utils::adt::float::float84gt"),
    ("float84le", "crate::backend::utils::adt::float::float84le"),
    ("float84lt", "crate::backend::utils::adt::float::float84lt"),
    ("float84mi", "crate::backend::utils::adt::float::float84mi"),
    ("float84mul", "crate::backend::utils::adt::float::float84mul"),
    ("float84ne", "crate::backend::utils::adt::float::float84ne"),
    ("float84pl", "crate::backend::utils::adt::float::float84pl"),
    ("float8abs", "crate::backend::utils::adt::float::float8abs"),
    ("float8div", "crate::backend::utils::adt::float::float8div"),
    ("float8eq", "crate::backend::utils::adt::float::float8eq"),
    ("float8ge", "crate::backend::utils::adt::float::float8ge"),
    ("float8gt", "crate::backend::utils::adt::float::float8gt"),
    ("float8in", "crate::backend::utils::adt::float::float8in"),
    ("float8larger", "crate::backend::utils::adt::float::float8larger"),
    ("float8le", "crate::backend::utils::adt::float::float8le"),
    ("float8lt", "crate::backend::utils::adt::float::float8lt"),
    ("float8mi", "crate::backend::utils::adt::float::float8mi"),
    ("float8mul", "crate::backend::utils::adt::float::float8mul"),
    ("float8ne", "crate::backend::utils::adt::float::float8ne"),
    ("float8out", "crate::backend::utils::adt::float::float8out"),
    ("float8pl", "crate::backend::utils::adt::float::float8pl"),
    ("float8recv", "crate::backend::utils::adt::float::float8recv"),
    ("float8send", "crate::backend::utils::adt::float::float8send"),
    ("float8smaller", "crate::backend::utils::adt::float::float8smaller"),
    ("float8um", "crate::backend::utils::adt::float::float8um"),
    ("float8up", "crate::backend::utils::adt::float::float8up"),
    // float.c aggregate transitions/finals: bound to the bodies, which stub-call
    // the not-yet-built agg array machinery (rules.md s4). Grouped with float here
    // (the table is a flat lookup; prosrc order is cosmetic).
    ("float4_accum", "crate::backend::utils::adt::float::float4_accum"),
    ("float8_accum", "crate::backend::utils::adt::float::float8_accum"),
    ("float8_avg", "crate::backend::utils::adt::float::float8_avg"),
    ("float8_combine", "crate::backend::utils::adt::float::float8_combine"),
    ("float8_corr", "crate::backend::utils::adt::float::float8_corr"),
    ("float8_covar_pop", "crate::backend::utils::adt::float::float8_covar_pop"),
    ("float8_covar_samp", "crate::backend::utils::adt::float::float8_covar_samp"),
    ("float8_regr_accum", "crate::backend::utils::adt::float::float8_regr_accum"),
    ("float8_regr_avgx", "crate::backend::utils::adt::float::float8_regr_avgx"),
    ("float8_regr_avgy", "crate::backend::utils::adt::float::float8_regr_avgy"),
    ("float8_regr_combine", "crate::backend::utils::adt::float::float8_regr_combine"),
    ("float8_regr_intercept", "crate::backend::utils::adt::float::float8_regr_intercept"),
    ("float8_regr_r2", "crate::backend::utils::adt::float::float8_regr_r2"),
    ("float8_regr_slope", "crate::backend::utils::adt::float::float8_regr_slope"),
    ("float8_regr_sxx", "crate::backend::utils::adt::float::float8_regr_sxx"),
    ("float8_regr_sxy", "crate::backend::utils::adt::float::float8_regr_sxy"),
    ("float8_regr_syy", "crate::backend::utils::adt::float::float8_regr_syy"),
    ("float8_stddev_pop", "crate::backend::utils::adt::float::float8_stddev_pop"),
    ("float8_stddev_samp", "crate::backend::utils::adt::float::float8_stddev_samp"),
    ("float8_var_pop", "crate::backend::utils::adt::float::float8_var_pop"),
    ("float8_var_samp", "crate::backend::utils::adt::float::float8_var_samp"),
    ("ftod", "crate::backend::utils::adt::float::ftod"),
    ("ftoi2", "crate::backend::utils::adt::float::ftoi2"),
    ("ftoi4", "crate::backend::utils::adt::float::ftoi4"),
    ("generate_series_int4", "crate::backend::utils::adt::int::generate_series_int4"),
    ("generate_series_int4_support", "crate::backend::utils::adt::int::generate_series_int4_support"),
    ("hashbool", "crate::backend::utils::adt::bool::hashbool"),
    ("hashboolextended", "crate::backend::utils::adt::bool::hashboolextended"),
    ("i2tod", "crate::backend::utils::adt::float::i2tod"),
    ("i2tof", "crate::backend::utils::adt::float::i2tof"),
    ("i2toi4", "crate::backend::utils::adt::int::i2toi4"),
    ("i4tochar", "crate::backend::utils::adt::char::i4tochar"),
    ("i4tod", "crate::backend::utils::adt::float::i4tod"),
    ("i4tof", "crate::backend::utils::adt::float::i4tof"),
    ("i4toi2", "crate::backend::utils::adt::int::i4toi2"),
    ("in_range_float4_float8", "crate::backend::utils::adt::float::in_range_float4_float8"),
    ("in_range_float8_float8", "crate::backend::utils::adt::float::in_range_float8_float8"),
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
    // int8.c (step 25B subset): count/sum/min/max support over bigint.
    ("int2_sum", "crate::backend::utils::adt::int::int2_sum"),
    ("int4_sum", "crate::backend::utils::adt::int::int4_sum"),
    ("int8in", "crate::backend::utils::adt::int::int8in"),
    ("int8inc", "crate::backend::utils::adt::int::int8inc"),
    ("int8inc_any", "crate::backend::utils::adt::int::int8inc_any"),
    ("int8larger", "crate::backend::utils::adt::int::int8larger"),
    ("int8out", "crate::backend::utils::adt::int::int8out"),
    ("int8pl", "crate::backend::utils::adt::int::int8pl"),
    ("int8smaller", "crate::backend::utils::adt::int::int8smaller"),
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
    ("float4_numeric", "crate::backend::utils::adt::numeric::float4_numeric"),
    ("float8_numeric", "crate::backend::utils::adt::numeric::float8_numeric"),
    ("int2_numeric", "crate::backend::utils::adt::numeric::int2_numeric"),
    ("int4_numeric", "crate::backend::utils::adt::numeric::int4_numeric"),
    ("int8_numeric", "crate::backend::utils::adt::numeric::int8_numeric"),
    ("numeric", "crate::backend::utils::adt::numeric::numeric"),
    ("numeric_abs", "crate::backend::utils::adt::numeric::numeric_abs"),
    ("numeric_accum", "crate::backend::utils::adt::numeric::numeric_accum"),
    ("numeric_accum_inv", "crate::backend::utils::adt::numeric::numeric_accum_inv"),
    ("numeric_add", "crate::backend::utils::adt::numeric::numeric_add"),
    ("numeric_avg", "crate::backend::utils::adt::numeric::numeric_avg"),
    ("numeric_avg_accum", "crate::backend::utils::adt::numeric::numeric_avg_accum"),
    ("numeric_avg_combine", "crate::backend::utils::adt::numeric::numeric_avg_combine"),
    ("numeric_avg_deserialize", "crate::backend::utils::adt::numeric::numeric_avg_deserialize"),
    ("numeric_avg_serialize", "crate::backend::utils::adt::numeric::numeric_avg_serialize"),
    ("numeric_ceil", "crate::backend::utils::adt::numeric::numeric_ceil"),
    ("numeric_cmp", "crate::backend::utils::adt::numeric::numeric_cmp"),
    ("numeric_combine", "crate::backend::utils::adt::numeric::numeric_combine"),
    ("numeric_deserialize", "crate::backend::utils::adt::numeric::numeric_deserialize"),
    ("numeric_div", "crate::backend::utils::adt::numeric::numeric_div"),
    ("numeric_div_trunc", "crate::backend::utils::adt::numeric::numeric_div_trunc"),
    ("numeric_eq", "crate::backend::utils::adt::numeric::numeric_eq"),
    ("numeric_exp", "crate::backend::utils::adt::numeric::numeric_exp"),
    ("numeric_fac", "crate::backend::utils::adt::numeric::numeric_fac"),
    ("numeric_float4", "crate::backend::utils::adt::numeric::numeric_float4"),
    ("numeric_float8", "crate::backend::utils::adt::numeric::numeric_float8"),
    ("numeric_float8_no_overflow", "crate::backend::utils::adt::numeric::numeric_float8_no_overflow"),
    ("numeric_floor", "crate::backend::utils::adt::numeric::numeric_floor"),
    ("numeric_gcd", "crate::backend::utils::adt::numeric::numeric_gcd"),
    ("numeric_ge", "crate::backend::utils::adt::numeric::numeric_ge"),
    ("numeric_gt", "crate::backend::utils::adt::numeric::numeric_gt"),
    ("numeric_in", "crate::backend::utils::adt::numeric::numeric_in"),
    ("numeric_inc", "crate::backend::utils::adt::numeric::numeric_inc"),
    ("numeric_int2", "crate::backend::utils::adt::numeric::numeric_int2"),
    ("numeric_int4", "crate::backend::utils::adt::numeric::numeric_int4"),
    ("numeric_int8", "crate::backend::utils::adt::numeric::numeric_int8"),
    ("numeric_larger", "crate::backend::utils::adt::numeric::numeric_larger"),
    ("numeric_lcm", "crate::backend::utils::adt::numeric::numeric_lcm"),
    ("numeric_le", "crate::backend::utils::adt::numeric::numeric_le"),
    ("numeric_ln", "crate::backend::utils::adt::numeric::numeric_ln"),
    ("numeric_log", "crate::backend::utils::adt::numeric::numeric_log"),
    ("numeric_lt", "crate::backend::utils::adt::numeric::numeric_lt"),
    ("numeric_min_scale", "crate::backend::utils::adt::numeric::numeric_min_scale"),
    ("numeric_mod", "crate::backend::utils::adt::numeric::numeric_mod"),
    ("numeric_mul", "crate::backend::utils::adt::numeric::numeric_mul"),
    ("numeric_ne", "crate::backend::utils::adt::numeric::numeric_ne"),
    ("numeric_out", "crate::backend::utils::adt::numeric::numeric_out"),
    ("numeric_pg_lsn", "crate::backend::utils::adt::numeric::numeric_pg_lsn"),
    ("numeric_poly_avg", "crate::backend::utils::adt::numeric::numeric_poly_avg"),
    ("numeric_poly_combine", "crate::backend::utils::adt::numeric::numeric_poly_combine"),
    ("numeric_poly_deserialize", "crate::backend::utils::adt::numeric::numeric_poly_deserialize"),
    ("numeric_poly_serialize", "crate::backend::utils::adt::numeric::numeric_poly_serialize"),
    ("numeric_poly_stddev_pop", "crate::backend::utils::adt::numeric::numeric_poly_stddev_pop"),
    ("numeric_poly_stddev_samp", "crate::backend::utils::adt::numeric::numeric_poly_stddev_samp"),
    ("numeric_poly_sum", "crate::backend::utils::adt::numeric::numeric_poly_sum"),
    ("numeric_poly_var_pop", "crate::backend::utils::adt::numeric::numeric_poly_var_pop"),
    ("numeric_poly_var_samp", "crate::backend::utils::adt::numeric::numeric_poly_var_samp"),
    ("numeric_power", "crate::backend::utils::adt::numeric::numeric_power"),
    ("numeric_random", "crate::backend::utils::adt::numeric::numeric_random"),
    ("numeric_recv", "crate::backend::utils::adt::numeric::numeric_recv"),
    ("numeric_round", "crate::backend::utils::adt::numeric::numeric_round"),
    ("numeric_scale", "crate::backend::utils::adt::numeric::numeric_scale"),
    ("numeric_send", "crate::backend::utils::adt::numeric::numeric_send"),
    ("numeric_serialize", "crate::backend::utils::adt::numeric::numeric_serialize"),
    ("numeric_sign", "crate::backend::utils::adt::numeric::numeric_sign"),
    ("numeric_smaller", "crate::backend::utils::adt::numeric::numeric_smaller"),
    ("numeric_sortsupport", "crate::backend::utils::adt::numeric::numeric_sortsupport"),
    ("numeric_sqrt", "crate::backend::utils::adt::numeric::numeric_sqrt"),
    ("numeric_stddev_pop", "crate::backend::utils::adt::numeric::numeric_stddev_pop"),
    ("numeric_stddev_samp", "crate::backend::utils::adt::numeric::numeric_stddev_samp"),
    ("numeric_sub", "crate::backend::utils::adt::numeric::numeric_sub"),
    ("numeric_sum", "crate::backend::utils::adt::numeric::numeric_sum"),
    ("numeric_support", "crate::backend::utils::adt::numeric::numeric_support"),
    ("numeric_trim_scale", "crate::backend::utils::adt::numeric::numeric_trim_scale"),
    ("numeric_trunc", "crate::backend::utils::adt::numeric::numeric_trunc"),
    ("numeric_uminus", "crate::backend::utils::adt::numeric::numeric_uminus"),
    ("numeric_uplus", "crate::backend::utils::adt::numeric::numeric_uplus"),
    ("numeric_var_pop", "crate::backend::utils::adt::numeric::numeric_var_pop"),
    ("numeric_var_samp", "crate::backend::utils::adt::numeric::numeric_var_samp"),
    ("numerictypmodin", "crate::backend::utils::adt::numeric::numerictypmodin"),
    ("numerictypmodout", "crate::backend::utils::adt::numeric::numerictypmodout"),
    ("oideq", "crate::backend::utils::adt::oid::oideq"),
    ("oidge", "crate::backend::utils::adt::oid::oidge"),
    ("oidgt", "crate::backend::utils::adt::oid::oidgt"),
    ("oidin", "crate::backend::utils::adt::oid::oidin"),
    ("oidlarger", "crate::backend::utils::adt::oid::oidlarger"),
    ("oidle", "crate::backend::utils::adt::oid::oidle"),
    ("oidlt", "crate::backend::utils::adt::oid::oidlt"),
    ("oidne", "crate::backend::utils::adt::oid::oidne"),
    ("oidout", "crate::backend::utils::adt::oid::oidout"),
    ("oidrecv", "crate::backend::utils::adt::oid::oidrecv"),
    ("oidsend", "crate::backend::utils::adt::oid::oidsend"),
    ("oidsmaller", "crate::backend::utils::adt::oid::oidsmaller"),
    ("oidvectoreq", "crate::backend::utils::adt::oid::oidvectoreq"),
    ("oidvectorge", "crate::backend::utils::adt::oid::oidvectorge"),
    ("oidvectorgt", "crate::backend::utils::adt::oid::oidvectorgt"),
    ("oidvectorin", "crate::backend::utils::adt::oid::oidvectorin"),
    ("oidvectorle", "crate::backend::utils::adt::oid::oidvectorle"),
    ("oidvectorlt", "crate::backend::utils::adt::oid::oidvectorlt"),
    ("oidvectorne", "crate::backend::utils::adt::oid::oidvectorne"),
    ("oidvectorout", "crate::backend::utils::adt::oid::oidvectorout"),
    ("oidvectorrecv", "crate::backend::utils::adt::oid::oidvectorrecv"),
    ("oidvectorsend", "crate::backend::utils::adt::oid::oidvectorsend"),
    ("radians", "crate::backend::utils::adt::float::radians"),
    ("text_char", "crate::backend::utils::adt::char::text_char"),
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
    ("width_bucket_float8", "crate::backend::utils::adt::float::width_bucket_float8"),
    // utils/adt/date.c + timestamp.c (step 22b): date/time/timetz +
    // timestamp/timestamptz/interval type fns, sorted by prosrc.
    ("clock_timestamp", "crate::backend::utils::adt::timestamp::clock_timestamp"),
    ("date_cmp", "crate::backend::utils::adt::date::date_cmp"),
    ("date_cmp_timestamp", "crate::backend::utils::adt::date::date_cmp_timestamp"),
    ("date_cmp_timestamptz", "crate::backend::utils::adt::date::date_cmp_timestamptz"),
    ("date_eq", "crate::backend::utils::adt::date::date_eq"),
    ("date_eq_timestamp", "crate::backend::utils::adt::date::date_eq_timestamp"),
    ("date_eq_timestamptz", "crate::backend::utils::adt::date::date_eq_timestamptz"),
    ("date_finite", "crate::backend::utils::adt::date::date_finite"),
    ("date_ge", "crate::backend::utils::adt::date::date_ge"),
    ("date_ge_timestamp", "crate::backend::utils::adt::date::date_ge_timestamp"),
    ("date_ge_timestamptz", "crate::backend::utils::adt::date::date_ge_timestamptz"),
    ("date_gt", "crate::backend::utils::adt::date::date_gt"),
    ("date_gt_timestamp", "crate::backend::utils::adt::date::date_gt_timestamp"),
    ("date_gt_timestamptz", "crate::backend::utils::adt::date::date_gt_timestamptz"),
    ("date_in", "crate::backend::utils::adt::date::date_in"),
    ("date_larger", "crate::backend::utils::adt::date::date_larger"),
    ("date_le", "crate::backend::utils::adt::date::date_le"),
    ("date_le_timestamp", "crate::backend::utils::adt::date::date_le_timestamp"),
    ("date_le_timestamptz", "crate::backend::utils::adt::date::date_le_timestamptz"),
    ("date_lt", "crate::backend::utils::adt::date::date_lt"),
    ("date_lt_timestamp", "crate::backend::utils::adt::date::date_lt_timestamp"),
    ("date_lt_timestamptz", "crate::backend::utils::adt::date::date_lt_timestamptz"),
    ("date_mi", "crate::backend::utils::adt::date::date_mi"),
    ("date_mi_interval", "crate::backend::utils::adt::date::date_mi_interval"),
    ("date_mii", "crate::backend::utils::adt::date::date_mii"),
    ("date_ne", "crate::backend::utils::adt::date::date_ne"),
    ("date_ne_timestamp", "crate::backend::utils::adt::date::date_ne_timestamp"),
    ("date_ne_timestamptz", "crate::backend::utils::adt::date::date_ne_timestamptz"),
    ("date_out", "crate::backend::utils::adt::date::date_out"),
    ("date_pl_interval", "crate::backend::utils::adt::date::date_pl_interval"),
    ("date_pli", "crate::backend::utils::adt::date::date_pli"),
    ("date_recv", "crate::backend::utils::adt::date::date_recv"),
    ("date_send", "crate::backend::utils::adt::date::date_send"),
    ("date_skipsupport", "crate::backend::utils::adt::date::date_skipsupport"),
    ("date_smaller", "crate::backend::utils::adt::date::date_smaller"),
    ("date_sortsupport", "crate::backend::utils::adt::date::date_sortsupport"),
    ("date_timestamp", "crate::backend::utils::adt::date::date_timestamp"),
    ("date_timestamptz", "crate::backend::utils::adt::date::date_timestamptz"),
    ("datetime_timestamp", "crate::backend::utils::adt::date::datetime_timestamp"),
    ("datetimetz_timestamptz", "crate::backend::utils::adt::date::datetimetz_timestamptz"),
    ("extract_date", "crate::backend::utils::adt::date::extract_date"),
    ("extract_interval", "crate::backend::utils::adt::timestamp::extract_interval"),
    ("extract_time", "crate::backend::utils::adt::date::extract_time"),
    ("extract_timestamp", "crate::backend::utils::adt::timestamp::extract_timestamp"),
    ("extract_timestamptz", "crate::backend::utils::adt::timestamp::extract_timestamptz"),
    ("extract_timetz", "crate::backend::utils::adt::date::extract_timetz"),
    ("float8_timestamptz", "crate::backend::utils::adt::timestamp::float8_timestamptz"),
    ("generate_series_timestamp", "crate::backend::utils::adt::timestamp::generate_series_timestamp"),
    ("generate_series_timestamp_support", "crate::backend::utils::adt::timestamp::generate_series_timestamp_support"),
    ("generate_series_timestamptz", "crate::backend::utils::adt::timestamp::generate_series_timestamptz"),
    ("generate_series_timestamptz_at_zone", "crate::backend::utils::adt::timestamp::generate_series_timestamptz_at_zone"),
    ("hashdate", "crate::backend::utils::adt::date::hashdate"),
    ("hashdateextended", "crate::backend::utils::adt::date::hashdateextended"),
    ("in_range_date_interval", "crate::backend::utils::adt::date::in_range_date_interval"),
    ("in_range_interval_interval", "crate::backend::utils::adt::timestamp::in_range_interval_interval"),
    ("in_range_time_interval", "crate::backend::utils::adt::date::in_range_time_interval"),
    ("in_range_timestamp_interval", "crate::backend::utils::adt::timestamp::in_range_timestamp_interval"),
    ("in_range_timestamptz_interval", "crate::backend::utils::adt::timestamp::in_range_timestamptz_interval"),
    ("in_range_timetz_interval", "crate::backend::utils::adt::date::in_range_timetz_interval"),
    ("interval_avg", "crate::backend::utils::adt::timestamp::interval_avg"),
    ("interval_avg_accum", "crate::backend::utils::adt::timestamp::interval_avg_accum"),
    ("interval_avg_accum_inv", "crate::backend::utils::adt::timestamp::interval_avg_accum_inv"),
    ("interval_avg_combine", "crate::backend::utils::adt::timestamp::interval_avg_combine"),
    ("interval_avg_deserialize", "crate::backend::utils::adt::timestamp::interval_avg_deserialize"),
    ("interval_avg_serialize", "crate::backend::utils::adt::timestamp::interval_avg_serialize"),
    ("interval_cmp", "crate::backend::utils::adt::timestamp::interval_cmp"),
    ("interval_div", "crate::backend::utils::adt::timestamp::interval_div"),
    ("interval_eq", "crate::backend::utils::adt::timestamp::interval_eq"),
    ("interval_finite", "crate::backend::utils::adt::timestamp::interval_finite"),
    ("interval_ge", "crate::backend::utils::adt::timestamp::interval_ge"),
    ("interval_gt", "crate::backend::utils::adt::timestamp::interval_gt"),
    ("interval_hash", "crate::backend::utils::adt::timestamp::interval_hash"),
    ("interval_hash_extended", "crate::backend::utils::adt::timestamp::interval_hash_extended"),
    ("interval_in", "crate::backend::utils::adt::timestamp::interval_in"),
    ("interval_justify_days", "crate::backend::utils::adt::timestamp::interval_justify_days"),
    ("interval_justify_hours", "crate::backend::utils::adt::timestamp::interval_justify_hours"),
    ("interval_justify_interval", "crate::backend::utils::adt::timestamp::interval_justify_interval"),
    ("interval_larger", "crate::backend::utils::adt::timestamp::interval_larger"),
    ("interval_le", "crate::backend::utils::adt::timestamp::interval_le"),
    ("interval_lt", "crate::backend::utils::adt::timestamp::interval_lt"),
    ("interval_mi", "crate::backend::utils::adt::timestamp::interval_mi"),
    ("interval_mul", "crate::backend::utils::adt::timestamp::interval_mul"),
    ("interval_ne", "crate::backend::utils::adt::timestamp::interval_ne"),
    ("interval_out", "crate::backend::utils::adt::timestamp::interval_out"),
    ("interval_part", "crate::backend::utils::adt::timestamp::interval_part"),
    ("interval_pl", "crate::backend::utils::adt::timestamp::interval_pl"),
    ("interval_recv", "crate::backend::utils::adt::timestamp::interval_recv"),
    ("interval_scale", "crate::backend::utils::adt::timestamp::interval_scale"),
    ("interval_send", "crate::backend::utils::adt::timestamp::interval_send"),
    ("interval_smaller", "crate::backend::utils::adt::timestamp::interval_smaller"),
    ("interval_sum", "crate::backend::utils::adt::timestamp::interval_sum"),
    ("interval_support", "crate::backend::utils::adt::timestamp::interval_support"),
    ("interval_time", "crate::backend::utils::adt::date::interval_time"),
    ("interval_trunc", "crate::backend::utils::adt::timestamp::interval_trunc"),
    ("interval_um", "crate::backend::utils::adt::timestamp::interval_um"),
    ("intervaltypmodin", "crate::backend::utils::adt::timestamp::intervaltypmodin"),
    ("intervaltypmodout", "crate::backend::utils::adt::timestamp::intervaltypmodout"),
    ("mul_d_interval", "crate::backend::utils::adt::timestamp::mul_d_interval"),
    ("now", "crate::backend::utils::adt::timestamp::now"),
    ("overlaps_time", "crate::backend::utils::adt::date::overlaps_time"),
    ("overlaps_timestamp", "crate::backend::utils::adt::timestamp::overlaps_timestamp"),
    ("overlaps_timetz", "crate::backend::utils::adt::date::overlaps_timetz"),
    ("pg_conf_load_time", "crate::backend::utils::adt::timestamp::pg_conf_load_time"),
    ("pg_postmaster_start_time", "crate::backend::utils::adt::timestamp::pg_postmaster_start_time"),
    ("statement_timestamp", "crate::backend::utils::adt::timestamp::statement_timestamp"),
    ("time_cmp", "crate::backend::utils::adt::date::time_cmp"),
    ("time_eq", "crate::backend::utils::adt::date::time_eq"),
    ("time_ge", "crate::backend::utils::adt::date::time_ge"),
    ("time_gt", "crate::backend::utils::adt::date::time_gt"),
    ("time_hash", "crate::backend::utils::adt::date::time_hash"),
    ("time_hash_extended", "crate::backend::utils::adt::date::time_hash_extended"),
    ("time_in", "crate::backend::utils::adt::date::time_in"),
    ("time_interval", "crate::backend::utils::adt::date::time_interval"),
    ("time_larger", "crate::backend::utils::adt::date::time_larger"),
    ("time_le", "crate::backend::utils::adt::date::time_le"),
    ("time_lt", "crate::backend::utils::adt::date::time_lt"),
    ("time_mi_interval", "crate::backend::utils::adt::date::time_mi_interval"),
    ("time_mi_time", "crate::backend::utils::adt::date::time_mi_time"),
    ("time_ne", "crate::backend::utils::adt::date::time_ne"),
    ("time_out", "crate::backend::utils::adt::date::time_out"),
    ("time_part", "crate::backend::utils::adt::date::time_part"),
    ("time_pl_interval", "crate::backend::utils::adt::date::time_pl_interval"),
    ("time_recv", "crate::backend::utils::adt::date::time_recv"),
    ("time_scale", "crate::backend::utils::adt::date::time_scale"),
    ("time_send", "crate::backend::utils::adt::date::time_send"),
    ("time_smaller", "crate::backend::utils::adt::date::time_smaller"),
    ("time_support", "crate::backend::utils::adt::date::time_support"),
    ("time_timetz", "crate::backend::utils::adt::date::time_timetz"),
    ("timeofday", "crate::backend::utils::adt::timestamp::timeofday"),
    ("timestamp_age", "crate::backend::utils::adt::timestamp::timestamp_age"),
    ("timestamp_at_local", "crate::backend::utils::adt::timestamp::timestamp_at_local"),
    ("timestamp_bin", "crate::backend::utils::adt::timestamp::timestamp_bin"),
    ("timestamp_cmp", "crate::backend::utils::adt::timestamp::timestamp_cmp"),
    ("timestamp_cmp_date", "crate::backend::utils::adt::date::timestamp_cmp_date"),
    ("timestamp_cmp_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_cmp_timestamptz"),
    ("timestamp_date", "crate::backend::utils::adt::date::timestamp_date"),
    ("timestamp_eq", "crate::backend::utils::adt::timestamp::timestamp_eq"),
    ("timestamp_eq_date", "crate::backend::utils::adt::date::timestamp_eq_date"),
    ("timestamp_eq_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_eq_timestamptz"),
    ("timestamp_finite", "crate::backend::utils::adt::timestamp::timestamp_finite"),
    ("timestamp_ge", "crate::backend::utils::adt::timestamp::timestamp_ge"),
    ("timestamp_ge_date", "crate::backend::utils::adt::date::timestamp_ge_date"),
    ("timestamp_ge_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_ge_timestamptz"),
    ("timestamp_gt", "crate::backend::utils::adt::timestamp::timestamp_gt"),
    ("timestamp_gt_date", "crate::backend::utils::adt::date::timestamp_gt_date"),
    ("timestamp_gt_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_gt_timestamptz"),
    ("timestamp_hash", "crate::backend::utils::adt::timestamp::timestamp_hash"),
    ("timestamp_hash_extended", "crate::backend::utils::adt::timestamp::timestamp_hash_extended"),
    ("timestamp_in", "crate::backend::utils::adt::timestamp::timestamp_in"),
    ("timestamp_izone", "crate::backend::utils::adt::timestamp::timestamp_izone"),
    ("timestamp_larger", "crate::backend::utils::adt::timestamp::timestamp_larger"),
    ("timestamp_le", "crate::backend::utils::adt::timestamp::timestamp_le"),
    ("timestamp_le_date", "crate::backend::utils::adt::date::timestamp_le_date"),
    ("timestamp_le_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_le_timestamptz"),
    ("timestamp_lt", "crate::backend::utils::adt::timestamp::timestamp_lt"),
    ("timestamp_lt_date", "crate::backend::utils::adt::date::timestamp_lt_date"),
    ("timestamp_lt_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_lt_timestamptz"),
    ("timestamp_mi", "crate::backend::utils::adt::timestamp::timestamp_mi"),
    ("timestamp_mi_interval", "crate::backend::utils::adt::timestamp::timestamp_mi_interval"),
    ("timestamp_ne", "crate::backend::utils::adt::timestamp::timestamp_ne"),
    ("timestamp_ne_date", "crate::backend::utils::adt::date::timestamp_ne_date"),
    ("timestamp_ne_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_ne_timestamptz"),
    ("timestamp_out", "crate::backend::utils::adt::timestamp::timestamp_out"),
    ("timestamp_part", "crate::backend::utils::adt::timestamp::timestamp_part"),
    ("timestamp_pl_interval", "crate::backend::utils::adt::timestamp::timestamp_pl_interval"),
    ("timestamp_recv", "crate::backend::utils::adt::timestamp::timestamp_recv"),
    ("timestamp_scale", "crate::backend::utils::adt::timestamp::timestamp_scale"),
    ("timestamp_send", "crate::backend::utils::adt::timestamp::timestamp_send"),
    ("timestamp_skipsupport", "crate::backend::utils::adt::timestamp::timestamp_skipsupport"),
    ("timestamp_smaller", "crate::backend::utils::adt::timestamp::timestamp_smaller"),
    ("timestamp_sortsupport", "crate::backend::utils::adt::timestamp::timestamp_sortsupport"),
    ("timestamp_support", "crate::backend::utils::adt::timestamp::timestamp_support"),
    ("timestamp_time", "crate::backend::utils::adt::date::timestamp_time"),
    ("timestamp_timestamptz", "crate::backend::utils::adt::timestamp::timestamp_timestamptz"),
    ("timestamp_trunc", "crate::backend::utils::adt::timestamp::timestamp_trunc"),
    ("timestamp_zone", "crate::backend::utils::adt::timestamp::timestamp_zone"),
    ("timestamptypmodin", "crate::backend::utils::adt::timestamp::timestamptypmodin"),
    ("timestamptypmodout", "crate::backend::utils::adt::timestamp::timestamptypmodout"),
    ("timestamptz_age", "crate::backend::utils::adt::timestamp::timestamptz_age"),
    ("timestamptz_at_local", "crate::backend::utils::adt::timestamp::timestamptz_at_local"),
    ("timestamptz_bin", "crate::backend::utils::adt::timestamp::timestamptz_bin"),
    ("timestamptz_cmp_date", "crate::backend::utils::adt::date::timestamptz_cmp_date"),
    ("timestamptz_cmp_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_cmp_timestamp"),
    ("timestamptz_date", "crate::backend::utils::adt::date::timestamptz_date"),
    ("timestamptz_eq_date", "crate::backend::utils::adt::date::timestamptz_eq_date"),
    ("timestamptz_eq_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_eq_timestamp"),
    ("timestamptz_ge_date", "crate::backend::utils::adt::date::timestamptz_ge_date"),
    ("timestamptz_ge_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_ge_timestamp"),
    ("timestamptz_gt_date", "crate::backend::utils::adt::date::timestamptz_gt_date"),
    ("timestamptz_gt_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_gt_timestamp"),
    ("timestamptz_hash", "crate::backend::utils::adt::timestamp::timestamptz_hash"),
    ("timestamptz_hash_extended", "crate::backend::utils::adt::timestamp::timestamptz_hash_extended"),
    ("timestamptz_in", "crate::backend::utils::adt::timestamp::timestamptz_in"),
    ("timestamptz_izone", "crate::backend::utils::adt::timestamp::timestamptz_izone"),
    ("timestamptz_le_date", "crate::backend::utils::adt::date::timestamptz_le_date"),
    ("timestamptz_le_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_le_timestamp"),
    ("timestamptz_lt_date", "crate::backend::utils::adt::date::timestamptz_lt_date"),
    ("timestamptz_lt_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_lt_timestamp"),
    ("timestamptz_mi_interval", "crate::backend::utils::adt::timestamp::timestamptz_mi_interval"),
    ("timestamptz_mi_interval_at_zone", "crate::backend::utils::adt::timestamp::timestamptz_mi_interval_at_zone"),
    ("timestamptz_ne_date", "crate::backend::utils::adt::date::timestamptz_ne_date"),
    ("timestamptz_ne_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_ne_timestamp"),
    ("timestamptz_out", "crate::backend::utils::adt::timestamp::timestamptz_out"),
    ("timestamptz_part", "crate::backend::utils::adt::timestamp::timestamptz_part"),
    ("timestamptz_pl_interval", "crate::backend::utils::adt::timestamp::timestamptz_pl_interval"),
    ("timestamptz_pl_interval_at_zone", "crate::backend::utils::adt::timestamp::timestamptz_pl_interval_at_zone"),
    ("timestamptz_recv", "crate::backend::utils::adt::timestamp::timestamptz_recv"),
    ("timestamptz_scale", "crate::backend::utils::adt::timestamp::timestamptz_scale"),
    ("timestamptz_send", "crate::backend::utils::adt::timestamp::timestamptz_send"),
    ("timestamptz_time", "crate::backend::utils::adt::date::timestamptz_time"),
    ("timestamptz_timestamp", "crate::backend::utils::adt::timestamp::timestamptz_timestamp"),
    ("timestamptz_timetz", "crate::backend::utils::adt::date::timestamptz_timetz"),
    ("timestamptz_trunc", "crate::backend::utils::adt::timestamp::timestamptz_trunc"),
    ("timestamptz_trunc_zone", "crate::backend::utils::adt::timestamp::timestamptz_trunc_zone"),
    ("timestamptz_zone", "crate::backend::utils::adt::timestamp::timestamptz_zone"),
    ("timestamptztypmodin", "crate::backend::utils::adt::timestamp::timestamptztypmodin"),
    ("timestamptztypmodout", "crate::backend::utils::adt::timestamp::timestamptztypmodout"),
    ("timetypmodin", "crate::backend::utils::adt::date::timetypmodin"),
    ("timetypmodout", "crate::backend::utils::adt::date::timetypmodout"),
    ("timetz_at_local", "crate::backend::utils::adt::date::timetz_at_local"),
    ("timetz_cmp", "crate::backend::utils::adt::date::timetz_cmp"),
    ("timetz_eq", "crate::backend::utils::adt::date::timetz_eq"),
    ("timetz_ge", "crate::backend::utils::adt::date::timetz_ge"),
    ("timetz_gt", "crate::backend::utils::adt::date::timetz_gt"),
    ("timetz_hash", "crate::backend::utils::adt::date::timetz_hash"),
    ("timetz_hash_extended", "crate::backend::utils::adt::date::timetz_hash_extended"),
    ("timetz_in", "crate::backend::utils::adt::date::timetz_in"),
    ("timetz_izone", "crate::backend::utils::adt::date::timetz_izone"),
    ("timetz_larger", "crate::backend::utils::adt::date::timetz_larger"),
    ("timetz_le", "crate::backend::utils::adt::date::timetz_le"),
    ("timetz_lt", "crate::backend::utils::adt::date::timetz_lt"),
    ("timetz_mi_interval", "crate::backend::utils::adt::date::timetz_mi_interval"),
    ("timetz_ne", "crate::backend::utils::adt::date::timetz_ne"),
    ("timetz_out", "crate::backend::utils::adt::date::timetz_out"),
    ("timetz_part", "crate::backend::utils::adt::date::timetz_part"),
    ("timetz_pl_interval", "crate::backend::utils::adt::date::timetz_pl_interval"),
    ("timetz_recv", "crate::backend::utils::adt::date::timetz_recv"),
    ("timetz_scale", "crate::backend::utils::adt::date::timetz_scale"),
    ("timetz_send", "crate::backend::utils::adt::date::timetz_send"),
    ("timetz_smaller", "crate::backend::utils::adt::date::timetz_smaller"),
    ("timetz_time", "crate::backend::utils::adt::date::timetz_time"),
    ("timetz_zone", "crate::backend::utils::adt::date::timetz_zone"),
    ("timetztypmodin", "crate::backend::utils::adt::date::timetztypmodin"),
    ("timetztypmodout", "crate::backend::utils::adt::date::timetztypmodout"),
    // utils/adt/formatting.c (step 22C): to_char/to_date/to_timestamp/to_number,
    // sorted by prosrc within this module block.
    ("float4_to_char", "crate::backend::utils::adt::formatting::float4_to_char"),
    ("float8_to_char", "crate::backend::utils::adt::formatting::float8_to_char"),
    ("int4_to_char", "crate::backend::utils::adt::formatting::int4_to_char"),
    ("int8_to_char", "crate::backend::utils::adt::formatting::int8_to_char"),
    ("interval_to_char", "crate::backend::utils::adt::formatting::interval_to_char"),
    ("numeric_to_char", "crate::backend::utils::adt::formatting::numeric_to_char"),
    ("numeric_to_number", "crate::backend::utils::adt::formatting::numeric_to_number"),
    ("timestamp_to_char", "crate::backend::utils::adt::formatting::timestamp_to_char"),
    ("timestamptz_to_char", "crate::backend::utils::adt::formatting::timestamptz_to_char"),
    ("to_date", "crate::backend::utils::adt::formatting::to_date"),
    ("to_timestamp", "crate::backend::utils::adt::formatting::to_timestamp"),
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
            "    FmgrBuiltin {{ foid: Oid::new({}), nargs: {}, strict: {}, retset: {}, func_name: \"{}\", func: {} }},\n",
            p.oid, p.nargs, p.strict, p.retset, p.prosrc, func
        ));
    }
    out.push_str("];\n");
    out.push_str(&format!("pub const fmgr_nbuiltins: usize = {};\n", builtins.len()));
    out.push_str(&format!("pub const fmgr_last_builtin_oid: Oid = Oid::new({last_oid});\n"));

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
        "// Generated by build.rs from catalog/*.dat (genbki OID symbols).\n\
         // Emitted as free consts AND associated consts on Oid (matchable as Oid::*).\n",
    );
    let mut assoc = String::from("impl Oid {\n");
    let mut seen = std::collections::HashSet::new();
    let mut emit = |out: &mut String, assoc: &mut String, name: &str, oid: &str| {
        if name.is_empty() || !seen.insert(name.to_string()) {
            return;
        }
        out.push_str(&format!("pub const {name}: Oid = Oid::new({oid});\n"));
        assoc.push_str(&format!("    pub const {name}: Self = Self::new({oid});\n"));
    };

    for d in dats {
        let text = std::fs::read_to_string(d).unwrap();
        let is_type = d.file_name().is_some_and(|n| n == "pg_type.dat");
        for r in parse_records(&text) {
            let Some(oid) = r.get("oid") else { continue };
            if let Some(sym) = r.get("oid_symbol") {
                emit(&mut out, &mut assoc, sym, oid);
            }
            if is_type
                && let Some(typname) = r.get("typname") {
                    let up = typname.to_uppercase();
                    emit(&mut out, &mut assoc, &format!("{up}OID"), oid);
                    if let Some(arr) = r.get("array_type_oid") {
                        emit(&mut out, &mut assoc, &format!("{up}ARRAYOID"), arr);
                    }
                }
        }
    }
    assoc.push_str("}\n");
    out.push_str(&assoc);
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
        // genbki's typbyval is 't'/'f' or the symbolic FLOAT8PASSBYVAL (int8/float8/
        // etc.), which resolves to true on 64-bit platforms (USE_FLOAT8_BYVAL).
        let typbyval = r
            .get("typbyval")
            .is_some_and(|s| s == "t" || s == "FLOAT8PASSBYVAL");
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
        // M3: pg_operator is nailed (formrdesc) so its descriptor exists before the
        // operator-resolution syscaches read it (it has no pg_class self-row yet).
        ("pg_operator", "pg_operator.h"),
        // M4 (step 23): pg_cast is nailed for the same reason -- the CASTSOURCETARGET
        // syscache reads its descriptor before pg_cast has a pg_class self-row.
        ("pg_cast", "pg_cast.h"),
        // M5 (step 25B): pg_aggregate is nailed so the AGGFNOID syscache can read its
        // descriptor before pg_aggregate has a pg_class self-row -- same rationale.
        ("pg_aggregate", "pg_aggregate.h"),
        // M10 (step 39): the object-DDL catalogs are nailed so their descriptors exist
        // for the runtime DDL inserts (CREATE SCHEMA/SEQUENCE, SET DEFAULT, ADD
        // CONSTRAINT, COMMENT) before they have pg_class self-rows.
        ("pg_namespace", "pg_namespace.h"),
        ("pg_sequence", "pg_sequence.h"),
        ("pg_attrdef", "pg_attrdef.h"),
        ("pg_constraint", "pg_constraint.h"),
        ("pg_description", "pg_description.h"),
        // M10 (step 39B): the minor object-DDL catalogs. pg_database/pg_tablespace
        // are seeded (the bootstrap db/tablespace rows); pg_collation/pg_conversion
        // start empty and are filled by CREATE COLLATION / CREATE CONVERSION. Nailing
        // gives their descriptors for those inserts before they have pg_class rows.
        ("pg_database", "pg_database.h"),
        ("pg_tablespace", "pg_tablespace.h"),
        ("pg_collation", "pg_collation.h"),
        ("pg_conversion", "pg_conversion.h"),
        // M11 (step 40): pg_rewrite is nailed so DefineQueryRewrite's InsertRule can
        // write the view _RETURN rule's metadata row before pg_rewrite has a pg_class
        // self-row. The action/qual trees live in the in-memory rule registry; the
        // ev_action/ev_qual varlena columns hold a deparsed placeholder.
        ("pg_rewrite", "pg_rewrite.h"),
        // M11 (step 41): pg_trigger is nailed so CREATE TRIGGER / ADD FOREIGN KEY can
        // write pg_trigger rows and RelationBuildTriggers can read them back before
        // pg_trigger has a pg_class self-row. Starts empty; filled by the DDL.
        ("pg_trigger", "pg_trigger.h"),
        // M13 (step 46): pg_statistic is nailed so ANALYZE can write its stats rows
        // and the planner (selfuncs) can read them back before pg_statistic has a
        // pg_class self-row. Starts empty; filled by ANALYZE.
        ("pg_statistic", "pg_statistic.h"),
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
    let m2_catalogs = [
        "pg_am", "pg_namespace", "pg_collation", "pg_opclass", "pg_amop", "pg_amproc",
        // M10 (step 39B): pg_tablespace's two bootstrap rows (pg_default/pg_global).
        "pg_tablespace",
    ];

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

// ===========================================================================
// M3: resolved pg_proc + pg_operator seed rows (operator/function resolution).
// ===========================================================================

/// The pg_proc functions M3 seeds: the int4 arithmetic/comparison operators'
/// support functions (oprcode) plus the unary-minus function. Each is read from
/// pg_proc.dat by name; its prorettype / proargtypes are resolved to OIDs.
const M3_PROC_NAMES: &[&str] = &[
    "int4pl", "int4mi", "int4mul", "int4div", "int4mod", "int4um",
    "int4lt", "int4gt", "int4le", "int4ge", "int4eq", "int4ne",
];

/// The pg_operator entries M3 seeds: the int4-on-int4 arithmetic and comparison
/// operators, keyed by (oprname, oprleft, oprright) to pick the int4/int4 row.
/// (oprname, left type name, right type name). A left of "" means a prefix
/// operator (oprleft = 0).
const M3_OPERATORS: &[(&str, &str, &str)] = &[
    ("+", "int4", "int4"),
    ("-", "int4", "int4"),
    ("*", "int4", "int4"),
    ("/", "int4", "int4"),
    ("%", "int4", "int4"),
    ("-", "", "int4"), // prefix unary minus
    ("<", "int4", "int4"),
    (">", "int4", "int4"),
    ("<=", "int4", "int4"),
    (">=", "int4", "int4"),
    ("=", "int4", "int4"),
    ("<>", "int4", "int4"),
];

/// The M4 casts pg_cast seeds (step 23): the numeric tower (int<->float<->numeric)
/// plus the explicit int4<->bool cast. Each is `(source_type, target_type,
/// castfunc_prosrc, context, method)` where `castfunc_prosrc` is the conversion
/// function's `prosrc` in pg_proc.dat (e.g. `int4_numeric`), used to resolve the
/// castfunc OID and to seed that pg_proc row (M4_CAST_PROCS). `context` is i/a/e
/// (COERCION_CODE_*) and `method` is f/b/i (COERCION_METHOD_*), verbatim from
/// pg_cast.dat. The any->text I/O coercion has NO catalog row (find_coercion_pathway
/// returns COERCEVIAIO directly), so it is not listed here.
const M4_CASTS: &[(&str, &str, &str, char, char)] = &[
    ("int2", "int4", "i2toi4", 'i', 'f'),
    ("int4", "int2", "i4toi2", 'a', 'f'),
    ("int2", "float8", "i2tod", 'i', 'f'),
    ("int4", "float8", "i4tod", 'i', 'f'),
    ("float8", "int4", "dtoi4", 'a', 'f'),
    ("int4", "float4", "i4tof", 'i', 'f'),
    ("float4", "float8", "ftod", 'i', 'f'),
    ("float8", "float4", "dtof", 'a', 'f'),
    ("int2", "numeric", "int2_numeric", 'i', 'f'),
    ("int4", "numeric", "int4_numeric", 'i', 'f'),
    ("float4", "numeric", "float4_numeric", 'a', 'f'),
    ("float8", "numeric", "float8_numeric", 'a', 'f'),
    ("numeric", "int4", "numeric_int4", 'a', 'f'),
    ("numeric", "float8", "numeric_float8", 'i', 'f'),
    ("numeric", "float4", "numeric_float4", 'i', 'f'),
    ("int4", "bool", "int4_bool", 'e', 'f'),
    ("bool", "int4", "bool_int4", 'e', 'f'),
];

/// Emit the M4 pg_cast + cast-support-proc seed tables (step 23). Like the M3
/// seeds, the source/target type names and the castfunc proc (resolved by its
/// `prosrc`, since the conversion functions share `proname`s like `numeric`/`int4`)
/// are resolved to OIDs here from the `.dat` files (genbki BKI_LOOKUP).
fn gen_m4_cast_seeds(
    inc: &Path,
    types: &std::collections::HashMap<String, TypeProps>,
) -> String {
    let type_oid = |name: &str| -> u32 {
        types
            .get(name)
            .unwrap_or_else(|| panic!("M4 cast seed: unknown type {name}"))
            .oid
    };

    let proc_text = std::fs::read_to_string(inc.join("pg_proc.dat")).unwrap();
    let proc_recs = parse_records(&proc_text);
    // Resolve a conversion proc by its prosrc (unique among the cast set).
    let by_prosrc = |src: &str| -> &std::collections::HashMap<String, String> {
        proc_recs
            .iter()
            .find(|r| r.get("prosrc").map(String::as_str) == Some(src))
            .unwrap_or_else(|| panic!("M4 cast seed: no pg_proc with prosrc {src}"))
    };

    let cast_text = std::fs::read_to_string(inc.join("pg_cast.dat")).unwrap();
    let cast_recs = parse_records(&cast_text);
    // Resolve the pg_cast row OID by (castsource, casttarget) so the seeded row
    // carries the upstream OID.
    let cast_oid = |src: &str, tgt: &str| -> u32 {
        cast_recs
            .iter()
            .find(|r| {
                r.get("castsource").map(String::as_str) == Some(src)
                    && r.get("casttarget").map(String::as_str) == Some(tgt)
            })
            .and_then(|r| r.get("oid"))
            .map_or(0, |s| s.parse().unwrap())
    };

    let mut out = String::from(
        "// Generated by build.rs (M4). Resolved pg_cast + cast-support pg_proc seed\n\
         // rows for the numeric/int/float cast tower. Names resolved to OIDs.\n",
    );

    // The cast-support pg_proc rows (reuses the M3Proc struct shape).
    out.push_str("pub static SEED_PG_PROC_M4_CAST: &[M3Proc] = &[\n");
    let mut emitted: Vec<u32> = Vec::new();
    for &(_, _, prosrc, _, _) in M4_CASTS {
        let rec = by_prosrc(prosrc);
        let oid: u32 = rec.get("oid").unwrap().parse().unwrap();
        if emitted.contains(&oid) {
            continue;
        }
        emitted.push(oid);
        let name = rec.get("proname").unwrap();
        let rettype = type_oid(rec.get("prorettype").map_or("", String::as_str));
        let argtypes: Vec<u32> = rec
            .get("proargtypes")
            .map(|s| s.split_whitespace().map(&type_oid).collect())
            .unwrap_or_default();
        let strict = rec.get("proisstrict").is_none_or(|s| s == "t");
        let retset = rec.get("proretset").is_some_and(|s| s == "t");
        let arglist = argtypes.iter().map(u32::to_string).collect::<Vec<_>>().join(", ");
        out.push_str(&format!(
            "    M3Proc {{ oid: {oid}, name: \"{name}\", rettype: {rettype}, \
             argtypes: &[{arglist}], strict: {strict}, retset: {retset}, prosrc: \"{prosrc}\" }},\n",
        ));
    }
    out.push_str("];\n");

    // The pg_cast rows: (oid, source, target, func, context, method).
    out.push_str(
        "pub struct M4Cast {\n\
        \x20   pub oid: u32, pub source: u32, pub target: u32, pub func: u32,\n\
        \x20   pub context: u8, pub method: u8,\n\
        }\n",
    );
    out.push_str("pub static SEED_PG_CAST_M4: &[M4Cast] = &[\n");
    for &(src, tgt, prosrc, ctx, meth) in M4_CASTS {
        let source = type_oid(src);
        let target = type_oid(tgt);
        let func: u32 = by_prosrc(prosrc).get("oid").unwrap().parse().unwrap();
        let oid = cast_oid(src, tgt);
        out.push_str(&format!(
            "    M4Cast {{ oid: {oid}, source: {source}, target: {target}, func: {func}, \
             context: {}, method: {} }},\n",
            ctx as u32, meth as u32,
        ));
    }
    out.push_str("];\n");
    out
}

// ===========================================================================
// M5 (step 25B): pg_aggregate + agg/transfn pg_proc seed rows.
// ===========================================================================

/// The aggregates M5 seeds, by their pg_aggregate `aggfnoid` signature string.
/// Each resolves the agg pg_proc OID, transfn/finalfn OIDs, transtype, and
/// initval from pg_aggregate.dat + pg_proc.dat. count/sum(int*)/min/max(int*) are
/// the executor-implemented set; the numeric/avg ones are seeded for catalog
/// completeness (their transfns stage until numeric accumulators land).
const M5_AGGREGATES: &[&str] = &[
    "count(*)", // alias resolved to count() below
    "count(any)",
    "sum(int2)",
    "sum(int4)",
    "sum(int8)",
    "sum(numeric)",
    "max(int2)",
    "max(int4)",
    "max(int8)",
    "max(numeric)",
    "max(text)",
    "min(int2)",
    "min(int4)",
    "min(int8)",
    "min(numeric)",
    "min(text)",
];

/// Resolve a pg_proc record by an `aggfnoid`-style signature `name(arg,arg)` (or
/// `name(*)`/`name()` for the zero-arg case). Matches proname + the argument
/// type-name list. `count(*)` is the zero-arg `count` proc.
fn m5_find_proc_by_sig<'a>(
    proc_recs: &'a [std::collections::HashMap<String, String>],
    sig: &str,
) -> &'a std::collections::HashMap<String, String> {
    let (name, argstr) = sig.split_once('(').unwrap_or((sig, ")"));
    let argstr = argstr.trim_end_matches(')');
    let want_args: Vec<&str> = if argstr.is_empty() || argstr == "*" {
        Vec::new()
    } else {
        argstr.split(',').map(str::trim).collect()
    };
    proc_recs
        .iter()
        .find(|r| {
            r.get("proname").map(String::as_str) == Some(name)
                && r.get("proargtypes").map_or(Vec::new(), |s| s.split_whitespace().collect::<Vec<_>>())
                    == want_args
        })
        .unwrap_or_else(|| panic!("M5 agg seed: no pg_proc for signature {sig}"))
}

/// Resolve a proc by bare name (transfn/finalfn names are unique among the
/// aggregate support set we seed). `None` for the empty / `-` sentinel.
fn m5_find_proc_by_name<'a>(
    proc_recs: &'a [std::collections::HashMap<String, String>],
    name: &str,
) -> Option<&'a std::collections::HashMap<String, String>> {
    if name.is_empty() || name == "-" {
        return None;
    }
    proc_recs.iter().find(|r| r.get("proname").map(String::as_str) == Some(name))
}

/// Emit the M5 resolved pg_aggregate rows + their supporting pg_proc rows (the
/// aggregate procs themselves, with prokind 'a', plus the transition/final fns).
/// Symbolic names are resolved to OIDs here (genbki BKI_LOOKUP).
fn gen_m5_aggregate_seeds(
    inc: &Path,
    types: &std::collections::HashMap<String, TypeProps>,
) -> String {
    // type_oid resolves names, numeric literals, and the empty string (-> 0). A
    // leading `_` denotes the element type's auto-generated array type (avg's
    // `_int8` transtype), resolved via the element's array_type_oid.
    let type_oid = |name: &str| -> u32 {
        if name.is_empty() {
            return 0;
        }
        if let Ok(n) = name.parse::<u32>() {
            return n;
        }
        if let Some(elem) = name.strip_prefix('_') {
            return types
                .get(elem)
                .map(|t| t.array_type_oid)
                .filter(|&o| o != 0)
                .unwrap_or_else(|| panic!("M5 agg seed: no array type for element {elem}"));
        }
        types
            .get(name)
            .unwrap_or_else(|| panic!("M5 agg seed: unknown type {name}"))
            .oid
    };

    let proc_text = std::fs::read_to_string(inc.join("pg_proc.dat")).unwrap();
    // Leak proc_recs to 'static so the resolved refs outlive helper scope (build
    // script; one-shot codegen, leak is fine).
    let proc_recs: &'static [std::collections::HashMap<String, String>] =
        Box::leak(parse_records(&proc_text).into_boxed_slice());

    let agg_text = std::fs::read_to_string(inc.join("pg_aggregate.dat")).unwrap();
    let agg_recs = parse_records(&agg_text);
    let find_agg = |sig: &str| -> &std::collections::HashMap<String, String> {
        // count(*) is stored as count() in pg_aggregate.dat.
        let key = if sig == "count(*)" { "count()" } else { sig };
        agg_recs
            .iter()
            .find(|r| r.get("aggfnoid").map(String::as_str) == Some(key))
            .unwrap_or_else(|| panic!("M5 agg seed: no pg_aggregate row for {sig}"))
    };

    let mut out = String::from(
        "// Generated by build.rs (M5). Resolved pg_aggregate + agg/transfn pg_proc\n\
         // seed rows for the common aggregates. Names resolved to OIDs.\n",
    );

    // Collect the pg_proc rows to seed: the aggregate procs + their transfn /
    // finalfn support procs (deduped by OID). Reuses the M3Proc struct.
    let mut proc_oids: Vec<u32> = Vec::new();
    let mut proc_rows: Vec<&std::collections::HashMap<String, String>> = Vec::new();
    let push_proc = |rec: &'static std::collections::HashMap<String, String>,
                     oids: &mut Vec<u32>,
                     rows: &mut Vec<&'static std::collections::HashMap<String, String>>| {
        let oid: u32 = rec.get("oid").unwrap().parse().unwrap();
        if !oids.contains(&oid) {
            oids.push(oid);
            rows.push(rec);
        }
    };
    // The pg_aggregate rows.
    out.push_str(
        "pub struct M5Aggregate {\n\
        \x20   pub aggfnoid: u32, pub aggkind: u8, pub aggtransfn: u32, pub aggfinalfn: u32,\n\
        \x20   pub aggtranstype: u32, pub initval: Option<&'static str>,\n\
        }\n",
    );
    out.push_str("pub static SEED_PG_AGGREGATE_M5: &[M5Aggregate] = &[\n");
    for &sig in M5_AGGREGATES {
        let agg = find_agg(sig);
        let aggproc = m5_find_proc_by_sig(proc_recs, sig);
        let aggfnoid: u32 = aggproc.get("oid").unwrap().parse().unwrap();
        push_proc(aggproc, &mut proc_oids, &mut proc_rows);

        let transfn_name = agg.get("aggtransfn").map_or("", String::as_str);
        let finalfn_name = agg.get("aggfinalfn").map_or("", String::as_str);
        let transfn = m5_find_proc_by_name(proc_recs, transfn_name)
            .unwrap_or_else(|| panic!("M5 agg seed: transfn {transfn_name} not found"));
        let transfn_oid: u32 = transfn.get("oid").unwrap().parse().unwrap();
        push_proc(transfn, &mut proc_oids, &mut proc_rows);
        let finalfn_oid = m5_find_proc_by_name(proc_recs, finalfn_name).map_or(0u32, |f| {
            push_proc(f, &mut proc_oids, &mut proc_rows);
            f.get("oid").unwrap().parse().unwrap()
        });

        let transtype = type_oid(agg.get("aggtranstype").map_or("", String::as_str));
        let initval =
            agg.get("agginitval").map_or_else(|| "None".to_string(), |s| format!("Some(\"{s}\")"));
        // aggkind defaults to 'n' (normal) in pg_aggregate.dat.
        let aggkind = agg.get("aggkind").and_then(|s| s.bytes().next()).unwrap_or(b'n');
        out.push_str(&format!(
            "    M5Aggregate {{ aggfnoid: {aggfnoid}, aggkind: {aggkind}, aggtransfn: {transfn_oid}, \
             aggfinalfn: {finalfn_oid}, aggtranstype: {transtype}, initval: {initval} }},\n",
        ));
    }
    out.push_str("];\n");

    // The supporting pg_proc rows (agg procs + transfn/finalfn), as M3Proc.
    out.push_str("pub static SEED_PG_PROC_M5_AGG: &[M3Proc] = &[\n");
    for rec in &proc_rows {
        emit_m5_proc_row(&mut out, rec, &type_oid);
    }
    out.push_str("];\n");
    out
}

/// Emit one M5 supporting-proc seed row (an agg proc or a transfn/finalfn), as an
/// `M3Proc` literal with names resolved to OIDs.
fn emit_m5_proc_row(
    out: &mut String,
    rec: &std::collections::HashMap<String, String>,
    type_oid: &impl Fn(&str) -> u32,
) {
    let oid: u32 = rec.get("oid").unwrap().parse().unwrap();
    let name = rec.get("proname").unwrap();
    let rettype = type_oid(rec.get("prorettype").map_or("", String::as_str));
    let argtypes: Vec<u32> = rec
        .get("proargtypes")
        .map(|s| s.split_whitespace().map(type_oid).collect())
        .unwrap_or_default();
    // Aggregate procs (prokind 'a') are not strict; the .dat default is 't'.
    let strict = rec.get("proisstrict").is_none_or(|s| s == "t");
    let retset = rec.get("proretset").is_some_and(|s| s == "t");
    let prosrc = rec.get("prosrc").cloned().unwrap_or_else(|| name.clone());
    let kind = rec.get("prokind").and_then(|s| s.bytes().next()).unwrap_or(b'f');
    let arglist = argtypes.iter().map(u32::to_string).collect::<Vec<_>>().join(", ");
    out.push_str(&format!(
        "    M3Proc {{ oid: {oid}, name: \"{name}\", rettype: {rettype}, \
         argtypes: &[{arglist}], strict: {strict}, retset: {retset}, prosrc: \"{prosrc}\" }}, \
         // prokind={}\n",
        kind as char,
    ));
}

/// The window functions M12 seeds (by `proname` + the pg_proc OID, since several
/// share a name across arities). Resolved from pg_proc.dat by OID. The polymorphic
/// (`anyelement`/`anycompatible`) arg/return types are seeded as their pseudo-type
/// OIDs; the executor resolves the concrete result type from the input at runtime.
const M12_WINDOW_FUNC_OIDS: &[u32] = &[
    3100, // row_number
    3101, // rank
    3102, // dense_rank
    3103, // percent_rank
    3104, // cume_dist
    3105, // ntile
    3106, 3107, 3108, // lag, lag(+offset), lag(+offset+default)
    3109, 3110, 3111, // lead variants
    3112, // first_value
    3113, // last_value
    3114, // nth_value
];

/// Emit the M12 window-function pg_proc seed rows (prokind 'w'), resolved from
/// pg_proc.dat by OID. Reuses the M3Proc struct (relies on the M3 seed defining it).
fn gen_m12_window_seeds(
    inc: &Path,
    types: &std::collections::HashMap<String, TypeProps>,
) -> String {
    let type_oid = |name: &str| -> u32 {
        if name.is_empty() {
            return 0;
        }
        if let Ok(n) = name.parse::<u32>() {
            return n;
        }
        // The polymorphic pseudo-types are not in the pg_type seed; map by well-known
        // OID (the executor resolves the concrete result type at runtime).
        match name {
            "anyelement" => return 2283,
            "anyarray" => return 2277,
            "anycompatible" => return 5077,
            "anycompatiblearray" => return 5078,
            "any" => return 2276,
            "internal" => return 2281,
            _ => {}
        }
        types.get(name).map_or_else(|| panic!("M12 window seed: unknown type {name}"), |t| t.oid)
    };

    let proc_text = std::fs::read_to_string(inc.join("pg_proc.dat")).unwrap();
    let proc_recs = parse_records(&proc_text);

    let mut out = String::from(
        "// Generated by build.rs (M12). Resolved pg_proc seed rows for the built-in\n\
         // window functions (prokind 'w'). Names/types resolved to OIDs.\n",
    );
    out.push_str("pub static SEED_PG_PROC_M12_WINDOW: &[M3Proc] = &[\n");
    for &want_oid in M12_WINDOW_FUNC_OIDS {
        let rec = proc_recs
            .iter()
            .find(|r| r.get("oid").and_then(|s| s.parse::<u32>().ok()) == Some(want_oid))
            .unwrap_or_else(|| panic!("M12 window seed: no pg_proc with oid {want_oid}"));
        emit_m5_proc_row(&mut out, rec, &type_oid);
    }
    out.push_str("];\n");
    out
}

/// Emit the M3 resolved seed tables for pg_proc and pg_operator. Symbolic type /
/// proc names in the `.dat` are resolved to numeric OIDs (genbki's BKI_LOOKUP).
fn gen_m3_operator_proc_seeds(
    inc: &Path,
    types: &std::collections::HashMap<String, TypeProps>,
) -> String {
    // typname -> oid (from the props map already parsed). A `.dat` type ref may be
    // a name, empty (-> 0), or a numeric OID literal like '0' (BKI_LOOKUP_OPT).
    let type_oid = |name: &str| -> u32 {
        if name.is_empty() {
            return 0;
        }
        if let Ok(n) = name.parse::<u32>() {
            return n;
        }
        types
            .get(name)
            .unwrap_or_else(|| panic!("M3 seed: unknown type {name}"))
            .oid
    };

    // proname -> oid, for resolving oprcode. pg_proc.dat has unique names for the
    // M3 set (no overloads among them).
    let proc_text = std::fs::read_to_string(inc.join("pg_proc.dat")).unwrap();
    let proc_recs = parse_records(&proc_text);
    let proc_oid = |name: &str| -> u32 {
        proc_recs
            .iter()
            .find(|r| r.get("proname").map(String::as_str) == Some(name))
            .and_then(|r| r.get("oid"))
            .unwrap_or_else(|| panic!("M3 seed: unknown proc {name}"))
            .parse()
            .unwrap()
    };

    let mut out = String::from(
        "// Generated by build.rs (M3). Resolved pg_proc + pg_operator seed rows for\n\
         // the int4 operator/function resolution path. Names resolved to OIDs.\n",
    );

    // pg_proc rows: (oid, name, rettype_oid, [argtype_oids], strict, retset, prosrc).
    out.push_str(
        "pub struct M3Proc {\n\
        \x20   pub oid: u32, pub name: &'static str, pub rettype: u32,\n\
        \x20   pub argtypes: &'static [u32], pub strict: bool, pub retset: bool,\n\
        \x20   pub prosrc: &'static str,\n\
        }\n",
    );
    out.push_str("pub static SEED_PG_PROC_M3: &[M3Proc] = &[\n");
    for &pname in M3_PROC_NAMES {
        let rec = proc_recs
            .iter()
            .find(|r| r.get("proname").map(String::as_str) == Some(pname))
            .unwrap_or_else(|| panic!("M3 seed: proc {pname} not in pg_proc.dat"));
        let oid: u32 = rec.get("oid").unwrap().parse().unwrap();
        let rettype = type_oid(rec.get("prorettype").map_or("", String::as_str));
        let argtypes: Vec<u32> = rec
            .get("proargtypes")
            .map(|s| s.split_whitespace().map(&type_oid).collect())
            .unwrap_or_default();
        // genbki proc defaults: proisstrict defaults to 't' in pg_proc.dat header.
        let strict = rec.get("proisstrict").is_none_or(|s| s == "t");
        let retset = rec.get("proretset").is_some_and(|s| s == "t");
        let prosrc = rec.get("prosrc").cloned().unwrap_or_else(|| pname.to_string());
        let arglist = argtypes.iter().map(u32::to_string).collect::<Vec<_>>().join(", ");
        out.push_str(&format!(
            "    M3Proc {{ oid: {oid}, name: \"{pname}\", rettype: {rettype}, \
             argtypes: &[{arglist}], strict: {strict}, retset: {retset}, prosrc: \"{prosrc}\" }},\n",
        ));
    }
    out.push_str("];\n");

    // pg_operator rows: (oid, name, kind 'b'/'l', left, right, result, code proc oid).
    let oper_text = std::fs::read_to_string(inc.join("pg_operator.dat")).unwrap();
    let oper_recs = parse_records(&oper_text);
    out.push_str(
        "pub struct M3Operator {\n\
        \x20   pub oid: u32, pub name: &'static str, pub kind: u8,\n\
        \x20   pub left: u32, pub right: u32, pub result: u32, pub code: u32,\n\
        }\n",
    );
    out.push_str("pub static SEED_PG_OPERATOR_M3: &[M3Operator] = &[\n");
    for &(op, left, right) in M3_OPERATORS {
        let left_oid = type_oid(left);
        let right_oid = type_oid(right);
        let rec = oper_recs
            .iter()
            .find(|r| {
                r.get("oprname").map(String::as_str) == Some(op)
                    && r.get("oprright").map_or(0, |s| type_oid(s)) == right_oid
                    // prefix operators have no oprleft in the .dat (defaults to 0).
                    && r.get("oprleft").map_or(0, |s| type_oid(s)) == left_oid
            })
            .unwrap_or_else(|| panic!("M3 seed: operator {op}({left},{right}) not in pg_operator.dat"));
        let oid: u32 = rec.get("oid").unwrap().parse().unwrap();
        let kind = if left.is_empty() { b'l' } else { b'b' };
        let result = type_oid(rec.get("oprresult").map_or("", String::as_str));
        let code = proc_oid(rec.get("oprcode").unwrap());
        out.push_str(&format!(
            "    M3Operator {{ oid: {oid}, name: \"{op}\", kind: {kind}, \
             left: {left_oid}, right: {right_oid}, result: {result}, code: {code} }},\n",
        ));
    }
    out.push_str("];\n");
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
