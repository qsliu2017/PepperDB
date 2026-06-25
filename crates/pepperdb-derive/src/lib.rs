//! Proc-macro derives for PepperDB, replacing PostgreSQL's Perl codegen.
//!
//! `#[derive(Catalog)]` on a `FormData_pg_<name>` struct emits the constants that
//! C's `genbki.pl` writes into `pg_<name>_d.h`: one `Anum_pg_<name>_<field>`
//! attribute-number per field (1-based, in declaration order) and `Natts_pg_<name>`.
//! The catalog name is taken from the struct name (strip the `FormData_` prefix),
//! so no attribute is required; `#[catalog(...)]`/`#[bki(...)]` annotations are
//! accepted and ignored (they carry BKI hints used elsewhere).

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Catalog, attributes(catalog, bki))]
pub fn derive_catalog(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident.to_string();

    let catname = struct_ident
        .strip_prefix("FormData_")
        .unwrap_or(&struct_ident);

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    &input.ident,
                    "#[derive(Catalog)] requires a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "#[derive(Catalog)] can only be applied to structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut consts = Vec::new();
    let mut n: i32 = 0;
    for f in fields {
        n += 1;
        let fname = f.ident.as_ref().unwrap().to_string();
        let anum = format_ident!("Anum_{}_{}", catname, fname);
        consts.push(quote! { pub const #anum: i32 = #n; });
    }
    let natts = format_ident!("Natts_{}", catname);
    consts.push(quote! { pub const #natts: i32 = #n; });

    quote! { #(#consts)* }.into()
}
