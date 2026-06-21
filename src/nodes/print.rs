//! nodes/print.c - various print routines (used mostly for debugging).

use crate::prelude::*;

use crate::{foreach, current_cell, IsA};

use crate::lib::stringinfo::{initStringInfo, StringInfoData};
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lfirst, list_length, lnext, List};
use crate::nodes::parsenodes::{
    RangeTblEntry, RTEKind,
};
use crate::nodes::parsenodes::RTEKind::*;
use crate::nodes::pathnodes::{EquivalenceClass, EquivalenceMember, PathKey};
use crate::nodes::primnodes::{
    Const, Expr, FuncExpr, OpExpr, TargetEntry, Var, INDEX_VAR, INNER_VAR, OUTER_VAR,
};
use crate::parser::parsetree::{get_rte_attribute_name, rt_fetch};
use crate::access::common::printtup::debugtup;
use crate::executor::tuptable::{TupleTableSlot, TupIsNull};
use crate::utils::fmgr::OidOutputFunctionCall;

use crate::{appendStringInfo, Assert};

// ---------------------------------------------------------------------------
// Stubs for callees not yet ported.
// ---------------------------------------------------------------------------

// nodes/nodeFuncs.c
unsafe fn nodeToStringWithLocations(_obj: *const c_void) -> *mut c_char {
    crate::nodes::outfuncs::nodeToStringWithLocations(_obj)
}

// utils/cache/lsyscache.c
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    crate::utils::cache::lsyscache::getTypeOutputInfo(_type, _typOutput, _typIsVarlena)
}
unsafe fn get_opname(_opno: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_opname(_opno)
}
unsafe fn get_func_name(_funcid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_func_name(_funcid)
}

// optimizer/util/clauses.c (get_leftop/get_rightop from nodeFuncs.h)
pub unsafe fn get_leftop(clause: *const Expr) -> *mut Node {
    let op = clause as *const OpExpr;
    if !(*op).args.is_null() {
        lfirst(crate::nodes::pg_list::list_nth_cell((*op).args, 0)) as *mut Node
    } else {
        null_mut()
    }
}
pub unsafe fn get_rightop(clause: *const Expr) -> *mut Node {
    let op = clause as *const OpExpr;
    if list_length((*op).args) >= 2 {
        lfirst(crate::nodes::pg_list::list_nth_cell((*op).args, 1)) as *mut Node
    } else {
        null_mut()
    }
}

// ---------------------------------------------------------------------------
// print
//	  print contents of Node to stdout
// ---------------------------------------------------------------------------
pub unsafe fn print(obj: *const c_void) {
    let s: *mut c_char;
    let f: *mut c_char;

    s = nodeToStringWithLocations(obj);
    f = format_node_dump(s);
    pfree(s as *mut c_void);
    print!("{}\n", cstr(f));
    use std::io::Write;
    let _ = std::io::stdout().flush();
    pfree(f as *mut c_void);
}

/*
 * pprint
 *	  pretty-print contents of Node to stdout
 */
pub unsafe fn pprint(obj: *const c_void) {
    let s: *mut c_char;
    let f: *mut c_char;

    s = nodeToStringWithLocations(obj);
    f = pretty_format_node_dump(s);
    pfree(s as *mut c_void);
    print!("{}\n", cstr(f));
    use std::io::Write;
    let _ = std::io::stdout().flush();
    pfree(f as *mut c_void);
}

/*
 * elog_node_display
 *	  send pretty-printed contents of Node to postmaster log
 */
pub unsafe fn elog_node_display(lev: c_int, title: *const c_char, obj: *const c_void, pretty: bool) {
    let s: *mut c_char;
    let f: *mut c_char;

    s = nodeToStringWithLocations(obj);
    if pretty {
        f = pretty_format_node_dump(s);
    } else {
        f = format_node_dump(s);
    }
    pfree(s as *mut c_void);
    // ereport(lev, (errmsg_internal("%s:", title), errdetail_internal("%s", f)));
    elog!(lev, "{}: {}", cstr(title), cstr(f));
    pfree(f as *mut c_void);
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version just tries to break at whitespace.
 */
pub unsafe fn format_node_dump(dump: *const c_char) -> *mut c_char {
    const LINELEN: usize = 78;
    let mut line: [c_char; LINELEN + 1] = [0; LINELEN + 1];
    let mut str: StringInfoData = std::mem::zeroed();
    let mut i: c_int;
    let mut j: c_int;
    let mut k: c_int;

    initStringInfo(&mut str);
    i = 0;
    loop {
        j = 0;
        while (j as usize) < LINELEN && *dump.offset(i as isize) != 0 {
            line[j as usize] = *dump.offset(i as isize);
            i += 1;
            j += 1;
        }
        if *dump.offset(i as isize) == 0 {
            break;
        }
        if *dump.offset(i as isize) == b' ' as c_char {
            /* ok to break at adjacent space */
            i += 1;
        } else {
            k = j - 1;
            while k > 0 {
                if line[k as usize] == b' ' as c_char {
                    break;
                }
                k -= 1;
            }
            if k > 0 {
                /* back up; will reprint all after space */
                i -= j - k - 1;
                j = k;
            }
        }
        line[j as usize] = 0;
        appendStringInfo!(&mut str, "{}\n", carr(&line));
    }
    if j > 0 {
        line[j as usize] = 0;
        appendStringInfo!(&mut str, "{}\n", carr(&line));
    }
    str.data
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version tries to indent intelligently.
 */
pub unsafe fn pretty_format_node_dump(dump: *const c_char) -> *mut c_char {
    const INDENTSTOP: c_int = 3;
    const MAXINDENT: c_int = 60;
    const LINELEN: usize = 78;
    let mut line: [c_char; LINELEN + 1] = [0; LINELEN + 1];
    let mut str: StringInfoData = std::mem::zeroed();
    let mut indentLev: c_int;
    let mut indentDist: c_int;
    let mut i: c_int;
    let mut j: c_int;

    initStringInfo(&mut str);
    indentLev = 0; /* logical indent level */
    indentDist = 0; /* physical indent distance */
    i = 0;
    loop {
        j = 0;
        while j < indentDist {
            line[j as usize] = b' ' as c_char;
            j += 1;
        }
        while (j as usize) < LINELEN && *dump.offset(i as isize) != 0 {
            line[j as usize] = *dump.offset(i as isize);
            match line[j as usize] as u8 as char {
                '}' => {
                    if j != indentDist {
                        /* print data before the } */
                        line[j as usize] = 0;
                        appendStringInfo!(&mut str, "{}\n", carr(&line));
                    }
                    /* print the } at indentDist */
                    line[indentDist as usize] = b'}' as c_char;
                    line[(indentDist + 1) as usize] = 0;
                    appendStringInfo!(&mut str, "{}\n", carr(&line));
                    /* outdent */
                    if indentLev > 0 {
                        indentLev -= 1;
                        indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
                    }
                    j = indentDist - 1;
                    /* j will equal indentDist on next loop iteration */
                    /* suppress whitespace just after } */
                    while *dump.offset((i + 1) as isize) == b' ' as c_char {
                        i += 1;
                    }
                }
                ')' => {
                    /* force line break after ), unless another ) follows */
                    if *dump.offset((i + 1) as isize) != b')' as c_char {
                        line[(j + 1) as usize] = 0;
                        appendStringInfo!(&mut str, "{}\n", carr(&line));
                        j = indentDist - 1;
                        while *dump.offset((i + 1) as isize) == b' ' as c_char {
                            i += 1;
                        }
                    }
                }
                '{' => {
                    /* force line break before { */
                    if j != indentDist {
                        line[j as usize] = 0;
                        appendStringInfo!(&mut str, "{}\n", carr(&line));
                    }
                    /* indent */
                    indentLev += 1;
                    indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
                    j = 0;
                    while j < indentDist {
                        line[j as usize] = b' ' as c_char;
                        j += 1;
                    }
                    line[j as usize] = *dump.offset(i as isize);
                }
                ':' => {
                    /* force line break before : */
                    if j != indentDist {
                        line[j as usize] = 0;
                        appendStringInfo!(&mut str, "{}\n", carr(&line));
                    }
                    j = indentDist;
                    line[j as usize] = *dump.offset(i as isize);
                }
                _ => {}
            }
            i += 1;
            j += 1;
        }
        line[j as usize] = 0;
        if *dump.offset(i as isize) == 0 {
            break;
        }
        appendStringInfo!(&mut str, "{}\n", carr(&line));
    }
    if j > 0 {
        appendStringInfo!(&mut str, "{}\n", carr(&line));
    }
    str.data
}

/*
 * print_rt
 *	  print contents of range table
 */
pub unsafe fn print_rt(rtable: *const List) {
    let mut i: c_int = 1;

    print!("resno\trefname  \trelid\tinFromCl\n");
    print!("-----\t---------\t-----\t--------\n");
    foreach!(l, rtable, {
        let rte = lfirst(current_cell!(l)) as *mut RangeTblEntry;

        match (*rte).rtekind {
            RTE_RELATION => {
                print!(
                    "{}\t{}\t{}\t{}",
                    i,
                    cstr((*(*rte).eref).aliasname),
                    (*rte).relid,
                    (*rte).relkind as u8 as char
                );
            }
            RTE_SUBQUERY => {
                print!("{}\t{}\t[subquery]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_JOIN => {
                print!("{}\t{}\t[join]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_FUNCTION => {
                print!("{}\t{}\t[rangefunction]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_TABLEFUNC => {
                print!("{}\t{}\t[table function]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_VALUES => {
                print!("{}\t{}\t[values list]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_CTE => {
                print!("{}\t{}\t[cte]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_NAMEDTUPLESTORE => {
                print!("{}\t{}\t[tuplestore]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_RESULT => {
                print!("{}\t{}\t[result]", i, cstr((*(*rte).eref).aliasname));
            }
            RTE_GROUP => {
                print!("{}\t{}\t[group]", i, cstr((*(*rte).eref).aliasname));
            }
            #[allow(unreachable_patterns)]
            _ => {
                print!("{}\t{}\t[unknown rtekind]", i, cstr((*(*rte).eref).aliasname));
            }
        }

        print!(
            "\t{}\t{}\n",
            if (*rte).inh { "inh" } else { "" },
            if (*rte).inFromCl { "inFromCl" } else { "" }
        );
        i += 1;
    });
}

/*
 * print_expr
 *	  print an expression
 */
pub unsafe fn print_expr(expr: *const Node, rtable: *const List) {
    if expr.is_null() {
        print!("<>");
        return;
    }

    if IsA!(expr, T_Var) {
        let var = expr as *const Var;
        let relname: *const c_char;
        let attname: *mut c_char;

        match (*var).varno {
            INNER_VAR => {
                relname = b"INNER\0".as_ptr() as *const c_char;
                attname = b"?\0".as_ptr() as *mut c_char;
            }
            OUTER_VAR => {
                relname = b"OUTER\0".as_ptr() as *const c_char;
                attname = b"?\0".as_ptr() as *mut c_char;
            }
            INDEX_VAR => {
                relname = b"INDEX\0".as_ptr() as *const c_char;
                attname = b"?\0".as_ptr() as *mut c_char;
            }
            _ => {
                let rte: *mut RangeTblEntry;

                Assert!((*var).varno > 0 && (*var).varno <= list_length(rtable));
                rte = rt_fetch((*var).varno as Index, rtable);
                relname = (*(*rte).eref).aliasname;
                attname = get_rte_attribute_name(rte, (*var).varattno);
            }
        }
        print!("{}.{}", cstr(relname), cstr(attname));
    } else if IsA!(expr, T_Const) {
        let c = expr as *const Const;
        let mut typoutput: Oid = 0;
        let mut typIsVarlena: bool = false;
        let outputstr: *mut c_char;

        if (*c).constisnull {
            print!("NULL");
            return;
        }

        getTypeOutputInfo((*c).consttype, &mut typoutput, &mut typIsVarlena);

        outputstr = OidOutputFunctionCall(typoutput, (*c).constvalue);
        print!("{}", cstr(outputstr));
        pfree(outputstr as *mut c_void);
    } else if IsA!(expr, T_OpExpr) {
        let e = expr as *const OpExpr;
        let opname: *mut c_char;

        opname = get_opname((*e).opno);
        if list_length((*e).args) > 1 {
            print_expr(get_leftop(e as *const Expr), rtable);
            print!(
                " {} ",
                if !opname.is_null() {
                    cstr(opname)
                } else {
                    "(invalid operator)".to_string()
                }
            );
            print_expr(get_rightop(e as *const Expr), rtable);
        } else {
            print!(
                "{} ",
                if !opname.is_null() {
                    cstr(opname)
                } else {
                    "(invalid operator)".to_string()
                }
            );
            print_expr(get_leftop(e as *const Expr), rtable);
        }
    } else if IsA!(expr, T_FuncExpr) {
        let e = expr as *const FuncExpr;
        let funcname: *mut c_char;

        funcname = get_func_name((*e).funcid);
        print!(
            "{}(",
            if !funcname.is_null() {
                cstr(funcname)
            } else {
                "(invalid function)".to_string()
            }
        );
        foreach!(l, (*e).args, {
            print_expr(lfirst(current_cell!(l)) as *const Node, rtable);
            if !lnext((*e).args, current_cell!(l)).is_null() {
                print!(",");
            }
        });
        print!(")");
    } else {
        print!("unknown expr");
    }
}

/*
 * print_pathkeys -
 *	  pathkeys list of PathKeys
 */
pub unsafe fn print_pathkeys(pathkeys: *const List, rtable: *const List) {

    print!("(");
    foreach!(i, pathkeys, {
        let pathkey = lfirst(current_cell!(i)) as *mut PathKey;
        let mut eclass: *mut EquivalenceClass;
        let mut first = true;

        eclass = (*pathkey).pk_eclass;
        /* chase up, in case pathkey is non-canonical */
        while !(*eclass).ec_merged.is_null() {
            eclass = (*eclass).ec_merged;
        }

        print!("(");
        foreach!(k, (*eclass).ec_members, {
            let mem = lfirst(current_cell!(k)) as *mut EquivalenceMember;

            if first {
                first = false;
            } else {
                print!(", ");
            }
            print_expr((*mem).em_expr as *const Node, rtable);
        });
        print!(")");
        if !lnext(pathkeys, current_cell!(i)).is_null() {
            print!(", ");
        }
    });
    print!(")\n");
}

/*
 * print_tl
 *	  print targetlist in a more legible way.
 */
pub unsafe fn print_tl(tlist: *const List, rtable: *const List) {

    print!("(\n");
    foreach!(tl, tlist, {
        let tle = lfirst(current_cell!(tl)) as *mut TargetEntry;

        print!(
            "\t{} {}\t",
            (*tle).resno,
            if !(*tle).resname.is_null() {
                cstr((*tle).resname)
            } else {
                "<null>".to_string()
            }
        );
        if (*tle).ressortgroupref != 0 {
            print!("({}):\t", (*tle).ressortgroupref);
        } else {
            print!("    :\t");
        }
        print_expr((*tle).expr as *const Node, rtable);
        print!("\n");
    });
    print!(")\n");
}

/*
 * print_slot
 *	  print out the tuple with the given TupleTableSlot
 */
pub unsafe fn print_slot(slot: *mut TupleTableSlot) {
    if TupIsNull(slot) {
        print!("tuple is null.\n");
        return;
    }
    if (*slot).tts_tupleDescriptor.is_null() {
        print!("no tuple descriptor.\n");
        return;
    }

    debugtup(slot, null_mut());
}

// ---------------------------------------------------------------------------
// helpers: render a NUL-terminated C string for Rust formatting / appends.
// ---------------------------------------------------------------------------

/// Render a `*const c_char` C string as a Rust `String` (up to the NUL).
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}

/// Render a NUL-terminated fixed `[c_char; N]` buffer as a Rust `String`.
unsafe fn carr(buf: &[c_char]) -> String {
    let mut s = String::new();
    for &b in buf {
        if b == 0 {
            break;
        }
        s.push(b as u8 as char);
    }
    s
}
