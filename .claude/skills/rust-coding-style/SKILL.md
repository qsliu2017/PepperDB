---
name: rust-coding-style
description: Rust coding style rules for PepperDB. Use this skill whenever writing or refactoring Rust code in this project -- including new functions, struct methods, iterator patterns, and module organization. Also trigger when the user asks about code style, conventions, or "how should I write this".
---

# Rust Coding Style

These rules supplement the conventions in CLAUDE.md with specific patterns for how to structure Rust code in this project.

## Merge helpers into their owner

When a free function exists only to serve one struct's methods, fold it into the impl block. Constants used only by that function become local `const` inside the method body rather than module-level items. This keeps the public API surface obvious and co-locates implementation details with the code that uses them.

```rust
// Before -- scattered across the file
const MAGIC: u32 = 0x01000193;

fn compute_thing(data: &[u8]) -> u32 {
    // uses MAGIC ...
}

impl Widget {
    pub fn thing(&self) -> u32 {
        compute_thing(&self.data)
    }
}

// After -- self-contained
impl Widget {
    pub fn thing(&self) -> u32 {
        const MAGIC: u32 = 0x01000193;
        // logic inline ...
    }
}
```

If the helper is used by multiple methods on the same struct, make it a private method (`fn helper(&self)`) rather than a free function.

## Iterator chains over mutable accumulators

Prefer `.fold()`, `.map()`, `.flat_map()`, and other iterator combinators over `let mut acc` + `for` loop + mutation. The goal is to contain or eliminate `mut` bindings.

```rust
// Before
let mut sums = [0u32; N];
for i in 0..words {
    sums[i % N] = sums[i % N].wrapping_add(word(i));
}

// After
let sums = data
    .chunks_exact(4)
    .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
    .enumerate()
    .fold([0u32; N], |mut acc, (i, w)| {
        acc[i % N] = acc[i % N].wrapping_add(w);
        acc
    });
```

`fold` with `|mut acc, ...|` is fine -- the mutation is scoped to the closure and the binding itself (`let sums`) is immutable. The same principle applies to reducing sequential state:

```rust
// Before
let mut result = INIT;
for &s in &sums {
    for b in s.to_le_bytes() {
        result ^= b as u32;
        result = result.wrapping_mul(PRIME);
    }
}

// After
let result = sums
    .iter()
    .flat_map(|s| s.to_le_bytes())
    .fold(INIT, |r, b| (r ^ b as u32).wrapping_mul(PRIME));
```

Don't force iterator style when it hurts clarity or requires allocation (e.g., collecting into a Vec just to iterate again). A `for` loop is fine when the iterator version would be contorted or significantly less efficient.

## Use `page_field!` macro for fixed-offset struct fields

When a newtype wraps a byte buffer and has fields at known offsets, define them with the `page_field!` macro instead of separate constants + free-function calls. Each macro invocation is the single source of truth for name, type, and byte offset.

```rust
// Before -- scattered constant + verbose call sites
pub(crate) const PD_LOWER: usize = 12; // u16 at byte 12
let lower = read_u16(&self.0, PD_LOWER);
write_u16(&mut self.0, PD_LOWER, val);

// After -- macro is the definition, methods are the API
page_field!(pd_lower, u16, 12);
let lower = self.pd_lower();
self.set_pd_lower(val);
```

The macro generates a `const fn` getter and a setter (prefixed `set_`), so getters work in `const fn` contexts. Use `get_u16`/`get_u32`/`set_u16`/`set_u32` for dynamic offsets (e.g., tuple fields at `base + T_XMAX`).

When adding a new page-level field, add one `page_field!` line -- never a standalone offset constant.
