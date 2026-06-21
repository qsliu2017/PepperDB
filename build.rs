// Compiles PostgreSQL's flex/bison-generated parser (scan.c/gram.c) plus the thin
// C support files into a static archive and links it into the Rust backend.  These
// generated files are not ported to Rust 1:1; using PG's own parser guarantees
// grammar/lexer parity.  All node/list/make/error/memory helpers the parser calls
// are provided by the Rust port via #[no_mangle] shims (parser_link_shims.rs,
// backend_link_shims.rs) + csrc/pdb_parser_glue.c.

use std::path::Path;
use std::process::Command;

fn main() {
    let pg = "postgres/src";
    let inc = format!("{pg}/include");

    let sdk = Command::new("xcrun")
        .args(["--show-sdk-path"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

    // (source path, object name)
    let units = [
        (format!("{pg}/backend/parser/gram.c"), "gram.o"),
        (format!("{pg}/backend/parser/scan.c"), "scan.o"),
        (format!("{pg}/backend/parser/parser.c"), "parser.o"),
        (format!("{pg}/common/kwlookup.c"), "kwlookup.o"),
        (format!("{pg}/common/keywords.c"), "keywords.o"),
        (format!("{pg}/common/psprintf.c"), "psprintf.o"),
        ("csrc/pdb_parser_glue.c".to_string(), "pdb_parser_glue.o"),
    ];

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let mut objs = Vec::new();

    for (src, obj) in &units {
        println!("cargo:rerun-if-changed={src}");
        let obj_path = format!("{out_dir}/{obj}");
        let mut cmd = Command::new("clang");
        cmd.args([
            "-c",
            "-O2",
            "-fno-strict-aliasing",
            "-fwrapv",
            "-fPIC",
            "-Wno-everything",
        ]);
        cmd.arg("-I").arg(&inc);
        if let Some(sdk) = &sdk {
            cmd.arg("-isysroot").arg(sdk);
        }
        cmd.arg(src).arg("-o").arg(&obj_path);
        let status = cmd.status().expect("failed to spawn clang");
        assert!(status.success(), "clang failed for {src}");
        objs.push(obj_path);
    }

    let lib_path = format!("{out_dir}/libpdbparser.a");
    let _ = std::fs::remove_file(&lib_path);
    let status = Command::new("ar")
        .arg("crs")
        .arg(&lib_path)
        .args(&objs)
        .status()
        .expect("failed to spawn ar");
    assert!(status.success(), "ar failed");

    assert!(Path::new(&lib_path).exists());
    println!("cargo:rustc-link-search=native={out_dir}");
    println!("cargo:rustc-link-lib=static=pdbparser");
    println!("cargo:rerun-if-changed=csrc/pdb_parser_glue.c");

    // Force-keep #[no_mangle] server symbols that C extensions loaded via dlopen
    // (e.g. src/test/regress/regress.c) resolve against the postgres binary at
    // load time.  Without -u, macOS's linker dead-strips these because nothing in
    // the live graph references them, so flat-namespace dlopen fails.
    for sym in EXPORTED_FOR_DLOPEN {
        println!("cargo:rustc-link-arg=-Wl,-u,_{sym}");
    }
    // Additional force-keep symbols listed one-per-line in dev/dlopen_symbols.txt
    // (generated from `nm -u <ext>.dylib`). Keeps regress.dylib / plpgsql.dylib
    // dlopen-resolvable regardless of which #[no_mangle] fns the live graph
    // happens to reference. Robust against dead-strip / incremental variance.
    println!("cargo:rerun-if-changed=dev/dlopen_symbols.txt");
    if let Ok(list) = std::fs::read_to_string("dev/dlopen_symbols.txt") {
        for line in list.lines() {
            let s = line.trim();
            if !s.is_empty() && !s.starts_with('#') {
                println!("cargo:rustc-link-arg=-Wl,-u,_{s}");
            }
        }
    }
}

/// Server symbols that loadable C modules need exported (resolved at dlopen time).
/// Each must have exactly one #[no_mangle] definition in the Rust port.
const EXPORTED_FOR_DLOPEN: &[&str] = &[
    "BlessTupleDesc",
    "DecrTupleDescRefCount",
    "FindDefaultConversionProc",
    "GetAttributeByName",
    "IsBinaryCoercible",
    "IsCatalogTextUniqueIndexOid",
    "MemoryContextAlloc",
    "ProcessInterrupts",
    "cstring_to_text",
    "deconstruct_array_builtin",
    "get_call_result_type",
    "heap_form_tuple",
    "lookup_rowtype_tupdesc",
    "pg_detoast_datum",
    "pg_detoast_datum_packed",
    "pg_do_encoding_conversion_buf",
    "pg_encoding_max_length",
    "pg_encoding_mb2wchar_with_len",
    "pg_encoding_mblen",
    "pg_encoding_set_invalid",
    "pg_encoding_verifymbstr",
    "pg_encoding_wchar2mb_with_len",
    "pg_mblen_cstr",
    "pg_mblen_range",
    "pg_mblen_unbounded",
    "pg_mblen_with_len",
    "pg_snprintf",
    "pg_usleep",
    "superuser",
    "pg_char_to_encoding_private",
    "pg_encoding_to_char_private",
    "pg_valid_server_encoding_private",
];
