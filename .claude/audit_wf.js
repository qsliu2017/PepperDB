export const meta = {
  name: 'audit-translation-completeness-grounded',
  description: 'Per-file audit grounded in a deterministic real-body index: confirm every C function has a real Rust body somewhere',
  phases: [
    { title: 'Audit', detail: 'batches of files verified against the real-body index' },
  ],
}

const TOTAL = args && args.total ? args.total : 876
const BATCH = args && args.batch ? args.batch : 18
const nBatches = Math.ceil(TOTAL / BATCH)

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    files: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          c: { type: 'string' },
          rs: { type: 'string' },
          verdict: { type: 'string', enum: ['FULL', 'PARTIAL', 'MINIMAL_STUB', 'MISSING'] },
          c_fn_count: { type: 'integer' },
          translated_fn_count: { type: 'integer' },
          stubbed_or_missing: { type: 'array', items: { type: 'string' } },
          note: { type: 'string' },
        },
        required: ['c', 'rs', 'verdict', 'c_fn_count', 'translated_fn_count', 'stubbed_or_missing', 'note'],
      },
    },
  },
  required: ['files'],
}

const INSTR = `You audit translation completeness for a 1:1 PostgreSQL C->Rust port (cwd /Users/qsliu/Desktop/PepperDB).

GROUND TRUTH: /tmp/real_index.json is a JSON array of EVERY Rust function name that has a REAL (non-stub) body
ANYWHERE in src/. It was produced by a literal-aware brace matcher (skips string/char literals and comments) that
classifies a body as a stub ONLY when its entire body is unimplemented!()/todo!()/TODO(pg-port). Trust this index as
the authoritative answer to "does function NAME have a real Rust body somewhere".

A C function is TRANSLATED if its name - OR a known idiomatic-rename variant - is present in real_index.json. The port
LEGITIMATELY (a) places a function in a different .rs than its .c, (b) renames via these suffixes:
['_c','_cb','_fn','_slice','_internal','_impl','_pub','_stub','_v','_guts','_compat','_wrapper','_libc','_builtin','_worker','_local'],
(c) inlines static qsort comparators into Rust .sort_by closures, (d) turns C variadics into _slice helpers,
(e) gates USE_LIBXML/WIN32 bodies under #[cfg(...)]. ALL of these COUNT AS TRANSLATED.

KNOWN IDIOMATIC RENAMES (treat as TRANSLATED even though the exact C name is absent):
  cmpNodePtr (inlined as slice::sort_by in spgtextproc.rs),
  btmask_add_n -> btmask_add_slice, btmask_all_except_n -> btmask_all_except_slice (C variadic -> Rust slice),
  varstr_levenshtein -> levenshtein_internal (the LEVENSHTEIN_LESS_EQUAL include-template instantiations are
    collapsed into one generic levenshtein_internal where max_d<0 selects the plain variant; documented in
    src/utils/adt/levenshtein.rs).

DO NOT downgrade a file for ANY of these (they are NOT incompleteness):
  - the count of unimplemented!() calls in the mapped .rs (these are dependency-stub HELPERS for functions OWNED BY
    OTHER files, stubbed locally so the file compiles - irrelevant to whether THIS .c's functions are translated),
  - a function being translated in a different .rs (cross-file placement),
  - extern "C" declarations when a real body exists elsewhere in the index,
  - cfg-gated (USE_LIBXML/WIN32) bodies.

Process ONLY /tmp/cmap.json entries at indices [START, END). cmap.json is a JSON array of {c, rs, how}.
For EACH entry, do EXACTLY this (prefer a single python3 invocation):
  1. Load real_index.json into a set R. Load the variant suffix list V and the idiomatic-rename set above.
  2. Extract the C file's TOP-LEVEL function definitions: regex (?m)^([a-z_][A-Za-z0-9_]*)\\( on the .c text, then
     balance parens from that '(' and confirm the next non-blank/non-comment char after the matching ')' is '{'.
     Exclude keywords {if,for,while,switch,return,sizeof,elog,ereport,typedef,else,do,static,extern} and
     names starting 'pg_attribute' and PG_USED_FOR_ASSERTS_ONLY/pg_noinline. This is c_fn_count.
  3. For each C function name N: TRANSLATED iff (N in R) OR (N+suffix in R for any suffix in V) OR N in the idiomatic
     set. Count translated_fn_count; collect any that fail into stubbed_or_missing.
  4. verdict = FULL if translated_fn_count == c_fn_count, else PARTIAL (MISSING only if rs file truly does not exist).
  5. An #include-only shim .c (~0 functions) -> FULL.
Return the validated structured object for your assigned entries only. Your verdict MUST follow mechanically from the
index lookup in step 3 - do not apply subjective judgment or count stubs.`

phase('Audit')
const batchResults = await parallel(
  Array.from({ length: nBatches }, (_, i) => () => {
    const start = i * BATCH
    const end = Math.min(start + BATCH, TOTAL)
    return agent(
      INSTR.replace('[START, END)', `[${start}, ${end})`) + `\n\nYour assigned index range: START=${start}, END=${end}.`,
      { label: `audit ${start}-${end}`, phase: 'Audit', schema: VERDICT_SCHEMA, agentType: 'Explore' }
    )
  })
)

const all = batchResults.filter(Boolean).flatMap(r => r.files || [])
const by = { FULL: [], PARTIAL: [], MINIMAL_STUB: [], MISSING: [] }
for (const f of all) (by[f.verdict] || (by[f.verdict] = [])).push(f)
log(`Audited ${all.length} files: FULL=${by.FULL.length} PARTIAL=${by.PARTIAL.length} MINIMAL_STUB=${by.MINIMAL_STUB.length} MISSING=${by.MISSING.length}`)

return {
  audited: all.length,
  summary: { FULL: by.FULL.length, PARTIAL: by.PARTIAL.length, MINIMAL_STUB: by.MINIMAL_STUB.length, MISSING: by.MISSING.length },
  problems: all.filter(f => f.verdict !== 'FULL').map(f => ({ c: f.c, verdict: f.verdict, c_fn_count: f.c_fn_count, translated_fn_count: f.translated_fn_count, stubbed_or_missing: (f.stubbed_or_missing || []).slice(0, 20), note: f.note })),
}
