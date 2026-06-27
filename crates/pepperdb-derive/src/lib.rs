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

/// `#[process_global]` on a subsystem struct generates the process-wide
/// publish/access trio that mirrors the owned `Arc<T>` (built by
/// `SharedState::new`) into a private `OnceLock`:
///
/// - `T::set(Arc<T>) -> bool` -- first-wins publish (a second publish is ignored
///   so tests building multiple `SharedState`s do not panic); returns whether it won.
/// - `T::get() -> Option<&'static Arc<T>>` -- the published instance, if any.
/// - `T::expect() -> &'static Arc<T>` -- ditto, panicking if none is published
///   (for the postmaster paths that run only after shared-memory init).
///
/// `SharedState` remains the owner; the `OnceLock` is a published handle so code
/// without a `SharedState` clone (C-named shims, signal handlers) still reaches it.
#[proc_macro_attribute]
pub fn process_global(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let ty = &input.ident;

    let upper: String =
        ty.to_string()
            .chars()
            .enumerate()
            .fold(String::new(), |mut acc, (i, ch)| {
                if ch.is_ascii_uppercase() && i != 0 {
                    acc.push('_');
                }
                acc.push(ch.to_ascii_uppercase());
                acc
            });
    let cell = format_ident!("__PG_{}", upper);
    let not_published = format!("{ty} not published");

    quote! {
        #input

        static #cell: ::std::sync::OnceLock<::std::sync::Arc<#ty>> =
            ::std::sync::OnceLock::new();

        impl #ty {
            /// Publish the process-wide instance (first-wins; a second publish is
            /// ignored). Returns whether this call won.
            pub fn set(instance: ::std::sync::Arc<#ty>) -> bool {
                #cell.set(instance).is_ok()
            }
            /// The process-wide instance, if one has been published.
            pub fn get() -> ::core::option::Option<&'static ::std::sync::Arc<#ty>> {
                #cell.get()
            }
            /// The process-wide instance; panics if none has been published.
            #[allow(
                clippy::expect_used,
                reason = "process-global accessor: published by SharedState::new before any backend runs"
            )]
            pub fn expect() -> &'static ::std::sync::Arc<#ty> {
                #cell.get().expect(#not_published)
            }
        }
    }
    .into()
}

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
        #[allow(
            clippy::unwrap_used,
            reason = "fields come from Fields::Named, so every field has an ident"
        )]
        let fname = f.ident.as_ref().unwrap().to_string();
        let anum = format_ident!("Anum_{}_{}", catname, fname);
        consts.push(quote! { pub const #anum: i32 = #n; });
    }
    let natts = format_ident!("Natts_{}", catname);
    consts.push(quote! { pub const #natts: i32 = #n; });

    quote! { #(#consts)* }.into()
}
