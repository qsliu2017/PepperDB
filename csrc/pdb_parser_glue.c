/*
 * Glue for the linked C parser (scan.c/gram.c/parser.c).
 *
 * PostgreSQL's err*() reporting functions are variadic; the Rust port exposes
 * non-variadic *_c() entry points that take an already-formatted message.  These
 * wrappers format the varargs with vsnprintf and forward to the Rust side
 * (pdb_rs_* in parser_link_shims.rs).  errcode()/errstart()/errfinish()/etc. are
 * non-variadic and shimmed directly in Rust.
 */
#include <stdio.h>
#include <stdarg.h>

extern int pdb_rs_errmsg(const char *msg);
extern int pdb_rs_errmsg_internal(const char *msg);
extern int pdb_rs_errdetail(const char *msg);
extern int pdb_rs_errhint(const char *msg);
extern int pdb_rs_errdetail_internal(const char *msg);
extern int pdb_rs_errcontext_msg(const char *msg);
extern int pdb_rs_errmsg_plural(const char *msg, unsigned long n);
extern char *pdb_rs_format_elog_string(const char *msg);

#define PDB_VFMT(target)                              \
    char buf[1024];                                   \
    va_list ap;                                       \
    va_start(ap, fmt);                                \
    vsnprintf(buf, sizeof(buf), fmt, ap);             \
    va_end(ap);                                       \
    return target(buf);

int errmsg(const char *fmt, ...)          { PDB_VFMT(pdb_rs_errmsg) }
int errmsg_internal(const char *fmt, ...) { PDB_VFMT(pdb_rs_errmsg_internal) }
int errdetail(const char *fmt, ...)       { PDB_VFMT(pdb_rs_errdetail) }
int errhint(const char *fmt, ...)         { PDB_VFMT(pdb_rs_errhint) }
int errdetail_internal(const char *fmt, ...) { PDB_VFMT(pdb_rs_errdetail_internal) }
int errcontext_msg(const char *fmt, ...)  { PDB_VFMT(pdb_rs_errcontext_msg) }

int errmsg_plural(const char *fmt_singular, const char *fmt_plural,
                  unsigned long n, ...)
{
    const char *fmt = (n == 1) ? fmt_singular : fmt_plural;
    char buf[1024];
    va_list ap;
    va_start(ap, n);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return pdb_rs_errmsg_plural(buf, n);
}

int pg_printf(const char *fmt, ...)
{
    va_list ap;
    int r;
    va_start(ap, fmt);
    r = vprintf(fmt, ap);
    va_end(ap);
    return r;
}

char *format_elog_string(const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return pdb_rs_format_elog_string(buf);
}

/* The Rust port's pg_vsnprintf is an unimplemented stub; provide a libc-backed
 * one so the va_list is handled natively. (PG's %m/%spec extensions are not
 * needed by the parser support code that calls this.) */
#include <stddef.h>
int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
    return vsnprintf(str, count, fmt, args);
}

/* appendStringInfo is variadic in C; format then hand the result to the Rust
 * appendStringInfoString (pdb_rs_append_string). */
extern void pdb_rs_append_string(void *str, const char *s);
void appendStringInfo(void *str, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    pdb_rs_append_string(str, buf);
}
