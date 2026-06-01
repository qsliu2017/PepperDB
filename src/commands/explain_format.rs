//! src/backend/commands/explain_format.c
//!
//! explain_format.c
//!   Format routines for explaining query execution plans
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/explain_format.c

use crate::prelude::*;

use crate::commands::explain_state::*;
use crate::lib::stringinfo::*;
use crate::nodes::pg_list::*;
use crate::utils::adt::json::escape_json;
use crate::{appendStringInfo, appendStringInfoCharMacro, foreach, current_cell};

/* OR-able flags for ExplainXMLTag() */
const X_OPENING: c_int = 0;
const X_CLOSING: c_int = 1;
const X_CLOSE_IMMEDIATE: c_int = 2;
const X_NOWHITESPACE: c_int = 4;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
}

/* TODO: src/backend/utils/adt/xml.c - not yet ported */
unsafe fn escape_xml(_str: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/xml.c
}

/* Local helper: render a C string into a Rust String for elog!/format! sites. */
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

/*
 * Explain a property, such as sort keys or targets, that takes the form of
 * a list of unlabeled items.  "data" is a list of C strings.
 */
pub unsafe fn ExplainPropertyList(qlabel: *const c_char, data: *mut List, es: *mut ExplainState) {
    let mut first: bool = true;

    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            ExplainIndentText(es);
            appendStringInfo!((*es).str, "{}: ", cstr_to_string(qlabel));
            foreach!(lc, data, {
                if !first {
                    appendStringInfoString((*es).str, c", ".as_ptr());
                }
                appendStringInfoString((*es).str, lfirst(current_cell!(lc)) as *const c_char);
                first = false;
            });
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }

        EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(qlabel, X_OPENING, es);
            foreach!(lc, data, {
                appendStringInfoSpaces((*es).str, (*es).indent * 2 + 2);
                appendStringInfoString((*es).str, c"<Item>".as_ptr());
                let str = escape_xml(lfirst(current_cell!(lc)) as *const c_char);
                appendStringInfoString((*es).str, str);
                pfree(str as *mut c_void);
                appendStringInfoString((*es).str, c"</Item>\n".as_ptr());
            });
            ExplainXMLTag(qlabel, X_CLOSING, es);
        }

        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces((*es).str, (*es).indent * 2);
            escape_json((*es).str, qlabel);
            appendStringInfoString((*es).str, c": [".as_ptr());
            foreach!(lc, data, {
                if !first {
                    appendStringInfoString((*es).str, c", ".as_ptr());
                }
                escape_json((*es).str, lfirst(current_cell!(lc)) as *const c_char);
                first = false;
            });
            appendStringInfoChar((*es).str, b']' as c_char);
        }

        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            appendStringInfo!((*es).str, "{}: ", cstr_to_string(qlabel));
            foreach!(lc, data, {
                appendStringInfoChar((*es).str, b'\n' as c_char);
                appendStringInfoSpaces((*es).str, (*es).indent * 2 + 2);
                appendStringInfoString((*es).str, c"- ".as_ptr());
                escape_yaml((*es).str, lfirst(current_cell!(lc)) as *const c_char);
            });
        }
    }
}

/*
 * Explain a property that takes the form of a list of unlabeled items within
 * another list.  "data" is a list of C strings.
 */
pub unsafe fn ExplainPropertyListNested(
    qlabel: *const c_char,
    data: *mut List,
    es: *mut ExplainState,
) {
    let mut first: bool = true;

    match (*es).format {
        EXPLAIN_FORMAT_TEXT | EXPLAIN_FORMAT_XML => {
            ExplainPropertyList(qlabel, data, es);
        }

        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces((*es).str, (*es).indent * 2);
            appendStringInfoChar((*es).str, b'[' as c_char);
            foreach!(lc, data, {
                if !first {
                    appendStringInfoString((*es).str, c", ".as_ptr());
                }
                escape_json((*es).str, lfirst(current_cell!(lc)) as *const c_char);
                first = false;
            });
            appendStringInfoChar((*es).str, b']' as c_char);
        }

        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            appendStringInfoString((*es).str, c"- [".as_ptr());
            foreach!(lc, data, {
                if !first {
                    appendStringInfoString((*es).str, c", ".as_ptr());
                }
                escape_yaml((*es).str, lfirst(current_cell!(lc)) as *const c_char);
                first = false;
            });
            appendStringInfoChar((*es).str, b']' as c_char);
        }
    }
}

/*
 * Explain a simple property.
 *
 * If "numeric" is true, the value is a number (or other value that
 * doesn't need quoting in JSON).
 *
 * If unit is non-NULL the text format will display it after the value.
 *
 * This usually should not be invoked directly, but via one of the datatype
 * specific routines ExplainPropertyText, ExplainPropertyInteger, etc.
 */
unsafe fn ExplainProperty(
    qlabel: *const c_char,
    unit: *const c_char,
    value: *const c_char,
    numeric: bool,
    es: *mut ExplainState,
) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            ExplainIndentText(es);
            if !unit.is_null() {
                appendStringInfo!(
                    (*es).str,
                    "{}: {} {}\n",
                    cstr_to_string(qlabel),
                    cstr_to_string(value),
                    cstr_to_string(unit)
                );
            } else {
                appendStringInfo!((*es).str, "{}: {}\n", cstr_to_string(qlabel), cstr_to_string(value));
            }
        }

        EXPLAIN_FORMAT_XML => {
            appendStringInfoSpaces((*es).str, (*es).indent * 2);
            ExplainXMLTag(qlabel, X_OPENING | X_NOWHITESPACE, es);
            let str = escape_xml(value);
            appendStringInfoString((*es).str, str);
            pfree(str as *mut c_void);
            ExplainXMLTag(qlabel, X_CLOSING | X_NOWHITESPACE, es);
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }

        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces((*es).str, (*es).indent * 2);
            escape_json((*es).str, qlabel);
            appendStringInfoString((*es).str, c": ".as_ptr());
            if numeric {
                appendStringInfoString((*es).str, value);
            } else {
                escape_json((*es).str, value);
            }
        }

        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            appendStringInfo!((*es).str, "{}: ", cstr_to_string(qlabel));
            if numeric {
                appendStringInfoString((*es).str, value);
            } else {
                escape_yaml((*es).str, value);
            }
        }
    }
}

/*
 * Explain a string-valued property.
 */
pub unsafe fn ExplainPropertyText(
    qlabel: *const c_char,
    value: *const c_char,
    es: *mut ExplainState,
) {
    ExplainProperty(qlabel, std::ptr::null(), value, false, es);
}

/*
 * Explain an integer-valued property.
 */
pub unsafe fn ExplainPropertyInteger(
    qlabel: *const c_char,
    unit: *const c_char,
    value: int64,
    es: *mut ExplainState,
) {
    let mut buf: [c_char; 32] = [0; 32];

    snprintf(buf.as_mut_ptr(), 32, c"%lld".as_ptr(), value as core::ffi::c_longlong);
    ExplainProperty(qlabel, unit, buf.as_ptr(), true, es);
}

/*
 * Explain an unsigned integer-valued property.
 */
pub unsafe fn ExplainPropertyUInteger(
    qlabel: *const c_char,
    unit: *const c_char,
    value: uint64,
    es: *mut ExplainState,
) {
    let mut buf: [c_char; 32] = [0; 32];

    snprintf(buf.as_mut_ptr(), 32, c"%llu".as_ptr(), value as core::ffi::c_ulonglong);
    ExplainProperty(qlabel, unit, buf.as_ptr(), true, es);
}

/*
 * Explain a float-valued property, using the specified number of
 * fractional digits.
 */
pub unsafe fn ExplainPropertyFloat(
    qlabel: *const c_char,
    unit: *const c_char,
    value: f64,
    ndigits: c_int,
    es: *mut ExplainState,
) {
    /* buf = psprintf("%.*f", ndigits, value); */
    let needed = snprintf(null_mut(), 0, c"%.*f".as_ptr(), ndigits, value) + 1;
    let buf = palloc(needed as Size) as *mut c_char;
    snprintf(buf, needed as usize, c"%.*f".as_ptr(), ndigits, value);
    ExplainProperty(qlabel, unit, buf, true, es);
    pfree(buf as *mut c_void);
}

/*
 * Explain a bool-valued property.
 */
pub unsafe fn ExplainPropertyBool(qlabel: *const c_char, value: bool, es: *mut ExplainState) {
    ExplainProperty(
        qlabel,
        std::ptr::null(),
        if value {
            c"true".as_ptr()
        } else {
            c"false".as_ptr()
        },
        true,
        es,
    );
}

/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
pub unsafe fn ExplainOpenGroup(
    objtype: *const c_char,
    labelname: *const c_char,
    labeled: bool,
    es: *mut ExplainState,
) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(objtype, X_OPENING, es);
            (*es).indent += 1;
        }

        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces((*es).str, 2 * (*es).indent);
            if !labelname.is_null() {
                escape_json((*es).str, labelname);
                appendStringInfoString((*es).str, c": ".as_ptr());
            }
            appendStringInfoChar((*es).str, if labeled { b'{' } else { b'[' } as c_char);

            /*
             * In JSON format, the grouping_stack is an integer list.  0 means
             * we've emitted nothing at this grouping level, 1 means we've
             * emitted something (and so the next item needs a comma). See
             * ExplainJSONLineEnding().
             */
            (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
            (*es).indent += 1;
        }

        EXPLAIN_FORMAT_YAML => {
            /*
             * In YAML format, the grouping stack is an integer list.  0 means
             * we've emitted nothing at this grouping level AND this grouping
             * level is unlabeled and must be marked with "- ".  See
             * ExplainYAMLLineStarting().
             */
            ExplainYAMLLineStarting(es);
            if !labelname.is_null() {
                appendStringInfo!((*es).str, "{}: ", cstr_to_string(labelname));
                (*es).grouping_stack = lcons_int(1, (*es).grouping_stack);
            } else {
                appendStringInfoString((*es).str, c"- ".as_ptr());
                (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
            }
            (*es).indent += 1;
        }
    }
}

/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
pub unsafe fn ExplainCloseGroup(
    objtype: *const c_char,
    _labelname: *const c_char,
    labeled: bool,
    es: *mut ExplainState,
) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            (*es).indent -= 1;
            ExplainXMLTag(objtype, X_CLOSING, es);
        }

        EXPLAIN_FORMAT_JSON => {
            (*es).indent -= 1;
            appendStringInfoChar((*es).str, b'\n' as c_char);
            appendStringInfoSpaces((*es).str, 2 * (*es).indent);
            appendStringInfoChar((*es).str, if labeled { b'}' } else { b']' } as c_char);
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }

        EXPLAIN_FORMAT_YAML => {
            (*es).indent -= 1;
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }
    }
}

/*
 * Open a group of related objects, without emitting actual data.
 *
 * Prepare the formatting state as though we were beginning a group with
 * the identified properties, but don't actually emit anything.  Output
 * subsequent to this call can be redirected into a separate output buffer,
 * and then eventually appended to the main output buffer after doing a
 * regular ExplainOpenGroup call (with the same parameters).
 *
 * The extra "depth" parameter is the new group's depth compared to current.
 * It could be more than one, in case the eventual output will be enclosed
 * in additional nesting group levels.  We assume we don't need to track
 * formatting state for those levels while preparing this group's output.
 *
 * There is no ExplainCloseSetAsideGroup --- in current usage, we always
 * pop this state with ExplainSaveGroup.
 */
pub unsafe fn ExplainOpenSetAsideGroup(
    _objtype: *const c_char,
    labelname: *const c_char,
    _labeled: bool,
    depth: c_int,
    es: *mut ExplainState,
) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            (*es).indent += depth;
        }

        EXPLAIN_FORMAT_JSON => {
            (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
            (*es).indent += depth;
        }

        EXPLAIN_FORMAT_YAML => {
            if !labelname.is_null() {
                (*es).grouping_stack = lcons_int(1, (*es).grouping_stack);
            } else {
                (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
            }
            (*es).indent += depth;
        }
    }
}

/*
 * Pop one level of grouping state, allowing for a re-push later.
 *
 * This is typically used after ExplainOpenSetAsideGroup; pass the
 * same "depth" used for that.
 *
 * This should not emit any output.  If state needs to be saved,
 * save it at *state_save.  Currently, an integer save area is sufficient
 * for all formats, but we might need to revisit that someday.
 */
pub unsafe fn ExplainSaveGroup(es: *mut ExplainState, depth: c_int, state_save: *mut c_int) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            (*es).indent -= depth;
        }

        EXPLAIN_FORMAT_JSON => {
            (*es).indent -= depth;
            *state_save = linitial_int((*es).grouping_stack);
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }

        EXPLAIN_FORMAT_YAML => {
            (*es).indent -= depth;
            *state_save = linitial_int((*es).grouping_stack);
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }
    }
}

/*
 * Re-push one level of grouping state, undoing the effects of ExplainSaveGroup.
 */
pub unsafe fn ExplainRestoreGroup(es: *mut ExplainState, depth: c_int, state_save: *mut c_int) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            (*es).indent += depth;
        }

        EXPLAIN_FORMAT_JSON => {
            (*es).grouping_stack = lcons_int(*state_save, (*es).grouping_stack);
            (*es).indent += depth;
        }

        EXPLAIN_FORMAT_YAML => {
            (*es).grouping_stack = lcons_int(*state_save, (*es).grouping_stack);
            (*es).indent += depth;
        }
    }
}

/*
 * Emit a "dummy" group that never has any members.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 */
pub unsafe fn ExplainDummyGroup(
    objtype: *const c_char,
    labelname: *const c_char,
    es: *mut ExplainState,
) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            ExplainXMLTag(objtype, X_CLOSE_IMMEDIATE, es);
        }

        EXPLAIN_FORMAT_JSON => {
            ExplainJSONLineEnding(es);
            appendStringInfoSpaces((*es).str, 2 * (*es).indent);
            if !labelname.is_null() {
                escape_json((*es).str, labelname);
                appendStringInfoString((*es).str, c": ".as_ptr());
            }
            escape_json((*es).str, objtype);
        }

        EXPLAIN_FORMAT_YAML => {
            ExplainYAMLLineStarting(es);
            if !labelname.is_null() {
                escape_yaml((*es).str, labelname);
                appendStringInfoString((*es).str, c": ".as_ptr());
            } else {
                appendStringInfoString((*es).str, c"- ".as_ptr());
            }
            escape_yaml((*es).str, objtype);
        }
    }
}

/*
 * Emit the start-of-output boilerplate.
 *
 * This is just enough different from processing a subgroup that we need
 * a separate pair of subroutines.
 */
pub unsafe fn ExplainBeginOutput(es: *mut ExplainState) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            appendStringInfoString(
                (*es).str,
                c"<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n".as_ptr(),
            );
            (*es).indent += 1;
        }

        EXPLAIN_FORMAT_JSON => {
            /* top-level structure is an array of plans */
            appendStringInfoChar((*es).str, b'[' as c_char);
            (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
            (*es).indent += 1;
        }

        EXPLAIN_FORMAT_YAML => {
            (*es).grouping_stack = lcons_int(0, (*es).grouping_stack);
        }
    }
}

/*
 * Emit the end-of-output boilerplate.
 */
pub unsafe fn ExplainEndOutput(es: *mut ExplainState) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* nothing to do */
        }

        EXPLAIN_FORMAT_XML => {
            (*es).indent -= 1;
            appendStringInfoString((*es).str, c"</explain>".as_ptr());
        }

        EXPLAIN_FORMAT_JSON => {
            (*es).indent -= 1;
            appendStringInfoString((*es).str, c"\n]".as_ptr());
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }

        EXPLAIN_FORMAT_YAML => {
            (*es).grouping_stack = list_delete_first((*es).grouping_stack);
        }
    }
}

/*
 * Put an appropriate separator between multiple plans
 */
pub unsafe fn ExplainSeparatePlans(es: *mut ExplainState) {
    match (*es).format {
        EXPLAIN_FORMAT_TEXT => {
            /* add a blank line */
            appendStringInfoChar((*es).str, b'\n' as c_char);
        }

        EXPLAIN_FORMAT_XML | EXPLAIN_FORMAT_JSON | EXPLAIN_FORMAT_YAML => {
            /* nothing to do */
        }
    }
}

/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML restricts tag names more than our other output formats, eg they can't
 * contain white space or slashes.  Replace invalid characters with dashes,
 * so that for example "I/O Read Time" becomes "I-O-Read-Time".
 */
unsafe fn ExplainXMLTag(tagname: *const c_char, flags: c_int, es: *mut ExplainState) {
    let valid =
        c"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.".as_ptr();

    if (flags & X_NOWHITESPACE) == 0 {
        appendStringInfoSpaces((*es).str, 2 * (*es).indent);
    }
    appendStringInfoCharMacro!((*es).str, b'<' as c_char);
    if (flags & X_CLOSING) != 0 {
        appendStringInfoCharMacro!((*es).str, b'/' as c_char);
    }
    let mut s = tagname;
    while *s != 0 {
        let ch = *s;
        appendStringInfoChar(
            (*es).str,
            if !strchr(valid, ch as c_int).is_null() {
                ch
            } else {
                b'-' as c_char
            },
        );
        s = s.add(1);
    }
    if (flags & X_CLOSE_IMMEDIATE) != 0 {
        appendStringInfoString((*es).str, c" /".as_ptr());
    }
    appendStringInfoCharMacro!((*es).str, b'>' as c_char);
    if (flags & X_NOWHITESPACE) == 0 {
        appendStringInfoCharMacro!((*es).str, b'\n' as c_char);
    }
}

/*
 * Indent a text-format line.
 *
 * We indent by two spaces per indentation level.  However, when emitting
 * data for a parallel worker there might already be data on the current line
 * (cf. ExplainOpenWorker); in that case, don't indent any more.
 */
pub unsafe fn ExplainIndentText(es: *mut ExplainState) {
    Assert!((*es).format == EXPLAIN_FORMAT_TEXT);
    if (*(*es).str).len == 0
        || *(*(*es).str).data.add(((*(*es).str).len - 1) as usize) == b'\n' as c_char
    {
        appendStringInfoSpaces((*es).str, (*es).indent * 2);
    }
}

/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
unsafe fn ExplainJSONLineEnding(es: *mut ExplainState) {
    Assert!((*es).format == EXPLAIN_FORMAT_JSON);
    if linitial_int((*es).grouping_stack) != 0 {
        appendStringInfoChar((*es).str, b',' as c_char);
    } else {
        *lfirst_int_mut(list_nth_cell((*es).grouping_stack, 0)) = 1;
    }
    appendStringInfoChar((*es).str, b'\n' as c_char);
}

/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabeled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
unsafe fn ExplainYAMLLineStarting(es: *mut ExplainState) {
    Assert!((*es).format == EXPLAIN_FORMAT_YAML);
    if linitial_int((*es).grouping_stack) == 0 {
        *lfirst_int_mut(list_nth_cell((*es).grouping_stack, 0)) = 1;
    } else {
        appendStringInfoChar((*es).str, b'\n' as c_char);
        appendStringInfoSpaces((*es).str, (*es).indent * 2);
    }
}

/*
 * YAML is a superset of JSON; unfortunately, the YAML quoting rules are
 * ridiculously complicated -- as documented in sections 5.3 and 7.3.3 of
 * http://yaml.org/spec/1.2/spec.html -- so we chose to just quote everything.
 * Empty strings, strings with leading or trailing whitespace, and strings
 * containing a variety of special characters must certainly be quoted or the
 * output is invalid; and other seemingly harmless strings like "0xa" or
 * "true" must be quoted, lest they be interpreted as a hexadecimal or Boolean
 * constant rather than a string.
 */
unsafe fn escape_yaml(buf: StringInfo, str: *const c_char) {
    escape_json(buf, str);
}
