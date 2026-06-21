// section: ruleutils C lines 6749-13709

fn get_rule_windowclause(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut sep: *const ::std::os::raw::c_char = ::std::ptr::null();
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        foreach!(lc, (*query).windowClause, {
            let wc = lfirst!(lc) as *mut WindowClause;
            if (*wc).name.is_null() {
                continue; // ignore anonymous windows
            }
            if sep.is_null() {
                appendContextKeyword(context,
                    b" WINDOW \0".as_ptr() as _, -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            } else {
                appendStringInfoString(buf, sep);
            }
            appendStringInfo!(buf, "{} AS ",
                ::std::ffi::CStr::from_ptr(quote_identifier((*wc).name)).to_string_lossy());
            get_rule_windowspec(wc, (*query).targetList, context);
            sep = b", \0".as_ptr() as _;
        });
    }
}

// Display a window definition
fn get_rule_windowspec(wc: *mut WindowClause, target_list: *mut List, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut needspace = false;
        let mut sep: *const ::std::os::raw::c_char;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();

        appendStringInfoChar(buf, b'(' as _);
        if !(*wc).refname.is_null() {
            appendStringInfoString(buf, quote_identifier((*wc).refname));
            needspace = true;
        }
        /* partition clauses are always inherited, so only print if no refname */
        if !(*wc).partitionClause.is_null() && (*wc).refname.is_null() {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            appendStringInfoString(buf, b"PARTITION BY \0".as_ptr() as _);
            sep = b"\0".as_ptr() as _;
            foreach!(lc, (*wc).partitionClause, {
                let grp = lfirst!(lc) as *mut SortGroupClause;
                appendStringInfoString(buf, sep);
                get_rule_sortgroupclause((*grp).tleSortGroupRef, target_list, false, context);
                sep = b", \0".as_ptr() as _;
            });
            needspace = true;
        }
        /* print ordering clause only if not inherited */
        if !(*wc).orderClause.is_null() && !(*wc).copiedOrder {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            appendStringInfoString(buf, b"ORDER BY \0".as_ptr() as _);
            get_rule_orderby((*wc).orderClause, target_list, false, context);
            needspace = true;
        }
        /* framing clause is never inherited, so print unless it's default */
        if ((*wc).frameOptions & FRAMEOPTION_NONDEFAULT) != 0 {
            if needspace { appendStringInfoChar(buf, b' ' as _); }
            get_window_frame_options((*wc).frameOptions, (*wc).startOffset, (*wc).endOffset, context);
        }
        appendStringInfoChar(buf, b')' as _);
    }
}

// Append the description of a window's framing options to context->buf
fn get_window_frame_options(frame_options: i32, start_offset: *mut Node, end_offset: *mut Node, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if (frame_options & FRAMEOPTION_NONDEFAULT) != 0 {
            if (frame_options & FRAMEOPTION_RANGE) != 0 {
                appendStringInfoString(buf, b"RANGE \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_ROWS) != 0 {
                appendStringInfoString(buf, b"ROWS \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_GROUPS) != 0 {
                appendStringInfoString(buf, b"GROUPS \0".as_ptr() as _);
            } else {
                debug_assert!(false);
            }
            if (frame_options & FRAMEOPTION_BETWEEN) != 0 {
                appendStringInfoString(buf, b"BETWEEN \0".as_ptr() as _);
            }
            if (frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING) != 0 {
                appendStringInfoString(buf, b"UNBOUNDED PRECEDING \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_START_CURRENT_ROW) != 0 {
                appendStringInfoString(buf, b"CURRENT ROW \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_START_OFFSET) != 0 {
                get_rule_expr(start_offset, context, false);
                if (frame_options & FRAMEOPTION_START_OFFSET_PRECEDING) != 0 {
                    appendStringInfoString(buf, b" PRECEDING \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_START_OFFSET_FOLLOWING) != 0 {
                    appendStringInfoString(buf, b" FOLLOWING \0".as_ptr() as _);
                } else { debug_assert!(false); }
            } else { debug_assert!(false); }
            if (frame_options & FRAMEOPTION_BETWEEN) != 0 {
                appendStringInfoString(buf, b"AND \0".as_ptr() as _);
                if (frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) != 0 {
                    appendStringInfoString(buf, b"UNBOUNDED FOLLOWING \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_END_CURRENT_ROW) != 0 {
                    appendStringInfoString(buf, b"CURRENT ROW \0".as_ptr() as _);
                } else if (frame_options & FRAMEOPTION_END_OFFSET) != 0 {
                    get_rule_expr(end_offset, context, false);
                    if (frame_options & FRAMEOPTION_END_OFFSET_PRECEDING) != 0 {
                        appendStringInfoString(buf, b" PRECEDING \0".as_ptr() as _);
                    } else if (frame_options & FRAMEOPTION_END_OFFSET_FOLLOWING) != 0 {
                        appendStringInfoString(buf, b" FOLLOWING \0".as_ptr() as _);
                    } else { debug_assert!(false); }
                } else { debug_assert!(false); }
            }
            if (frame_options & FRAMEOPTION_EXCLUDE_CURRENT_ROW) != 0 {
                appendStringInfoString(buf, b"EXCLUDE CURRENT ROW \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_EXCLUDE_GROUP) != 0 {
                appendStringInfoString(buf, b"EXCLUDE GROUP \0".as_ptr() as _);
            } else if (frame_options & FRAMEOPTION_EXCLUDE_TIES) != 0 {
                appendStringInfoString(buf, b"EXCLUDE TIES \0".as_ptr() as _);
            }
            /* we will now have a trailing space; remove it */
            (*buf).len -= 1;
            *(*buf).data.add((*buf).len as usize) = b'\0' as _;
        }
    }
}

// Return the description of a window's framing options as a palloc'd string
pub unsafe fn get_window_frame_options_for_explain(
    frame_options: i32,
    start_offset: *mut Node,
    end_offset: *mut Node,
    dpcontext: *mut List,
    forceprefix: bool,
) -> *mut ::std::os::raw::c_char {
    let mut buf: StringInfoData = ::std::mem::zeroed();
    let mut context: deparse_context = ::std::mem::zeroed();
    initStringInfo(&mut buf);
    context.buf = &mut buf;
    context.namespaces = dpcontext;
    context.resultDesc = ::std::ptr::null_mut();
    context.targetList = ::std::ptr::null_mut();
    context.windowClause = ::std::ptr::null_mut();
    context.varprefix = forceprefix;
    context.prettyFlags = 0;
    context.wrapColumn = WRAP_COLUMN_DEFAULT;
    context.indentLevel = 0;
    context.colNamesVisible = true;
    context.inGroupBy = false;
    context.varInOrderBy = false;
    context.appendparents = ::std::ptr::null_mut();
    get_window_frame_options(frame_options, start_offset, end_offset, &mut context);
    buf.data
}

// get_insert_query_def - Parse back an INSERT parsetree
fn get_insert_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut select_rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut values_rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut sep: *const ::std::os::raw::c_char;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut strippedexprs: *mut List = ::std::ptr::null_mut();

        /* Insert the WITH clause if given */
        get_with_clause(query, context);

        /*
         * If it's an INSERT ... SELECT or multi-row VALUES, there will be a
         * single RTE for the SELECT or VALUES.  Plain VALUES has neither.
         */
        foreach!(lc, (*query).rtable, {
            rte = lfirst!(lc) as *mut RangeTblEntry;
            if (*rte).rtekind == RTE_SUBQUERY {
                if !select_rte.is_null() { elog!(ERROR, "too many subquery RTEs in INSERT"); }
                select_rte = rte;
            }
            if (*rte).rtekind == RTE_VALUES {
                if !values_rte.is_null() { elog!(ERROR, "too many values RTEs in INSERT"); }
                values_rte = rte;
            }
        });
        if !select_rte.is_null() && !values_rte.is_null() {
            elog!(ERROR, "both subquery and values RTEs in INSERT");
        }

        /* Start the query with INSERT INTO relname */
        rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            (*context).indentLevel += PRETTYINDENT_STD as i32;
            appendStringInfoChar(buf, b' ' as _);
        }
        appendStringInfo!(buf, "INSERT INTO {}",
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());

        /* Print the relation alias, if needed; INSERT requires explicit AS */
        get_rte_alias(rte, (*query).resultRelation, true, context);
        /* always want a space here */
        appendStringInfoChar(buf, b' ' as _);

        /*
         * Add the insert-column-names list.
         */
        strippedexprs = ::std::ptr::null_mut();
        sep = b"\0".as_ptr() as _;
        if !(*query).targetList.is_null() {
            appendStringInfoChar(buf, b'(' as _);
        }
        foreach!(lc, (*query).targetList, {
            let tle = lfirst!(lc) as *mut TargetEntry;
            if (*tle).resjunk { continue; } // ignore junk entries
            appendStringInfoString(buf, sep);
            sep = b", \0".as_ptr() as _;
            appendStringInfoString(buf, quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));
            strippedexprs = lappend(strippedexprs,
                processIndirection((*tle).expr as *mut Node, context));
        });
        if !(*query).targetList.is_null() {
            appendStringInfoString(buf, b") \0".as_ptr() as _);
        }

        if (*query).r#override == OVERRIDING_SYSTEM_VALUE {
            appendStringInfoString(buf, b"OVERRIDING SYSTEM VALUE \0".as_ptr() as _);
        } else if (*query).r#override == OVERRIDING_USER_VALUE {
            appendStringInfoString(buf, b"OVERRIDING USER VALUE \0".as_ptr() as _);
        }

        if !select_rte.is_null() {
            /* Add the SELECT */
            get_query_def((*select_rte).subquery, buf, (*context).namespaces,
                ::std::ptr::null_mut(), false,
                (*context).prettyFlags, (*context).wrapColumn, (*context).indentLevel);
        } else if !values_rte.is_null() {
            /* Add the multi-VALUES expression lists */
            get_values_def((*values_rte).values_lists, context);
        } else if !strippedexprs.is_null() {
            /* Add the single-VALUES expression list */
            appendContextKeyword(context, b"VALUES (\0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
            get_rule_list_toplevel(strippedexprs, context, false);
            appendStringInfoChar(buf, b')' as _);
        } else {
            /* No expressions, so it must be DEFAULT VALUES */
            appendStringInfoString(buf, b"DEFAULT VALUES\0".as_ptr() as _);
        }

        /* Add ON CONFLICT if present */
        if !(*query).onConflict.is_null() {
            let confl = (*query).onConflict;
            appendStringInfoString(buf, b" ON CONFLICT\0".as_ptr() as _);
            if !(*confl).arbiterElems.is_null() {
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr((*confl).arbiterElems as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                if !(*confl).arbiterWhere.is_null() {
                    let save_varprefix = (*context).varprefix;
                    /*
                     * Force non-prefixing of Vars, since parser assumes that they
                     * belong to target relation.  WHERE clause does not use
                     * InferenceElem, so this is separately required.
                     */
                    (*context).varprefix = false;
                    appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
                    get_rule_expr((*confl).arbiterWhere, context, false);
                    (*context).varprefix = save_varprefix;
                }
            } else if OidIsValid((*confl).constraint) {
                let constraint = get_constraint_name((*confl).constraint);
                if constraint.is_null() {
                    elog!(ERROR, "cache lookup failed for constraint {}", (*confl).constraint);
                }
                appendStringInfo!(buf, " ON CONSTRAINT {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(constraint)).to_string_lossy());
            }
            if (*confl).action == ONCONFLICT_NOTHING {
                appendStringInfoString(buf, b" DO NOTHING\0".as_ptr() as _);
            } else {
                appendStringInfoString(buf, b" DO UPDATE SET \0".as_ptr() as _);
                /* Deparse targetlist */
                get_update_query_targetlist_def(query, (*confl).onConflictSet, context, rte);
                /* Add a WHERE clause if given */
                if !(*confl).onConflictWhere.is_null() {
                    appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
                    get_rule_expr((*confl).onConflictWhere, context, false);
                }
            }
        }

        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_update_query_def - Parse back an UPDATE parsetree
fn get_update_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with UPDATE relname SET */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "UPDATE {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        appendStringInfoString(buf, b" SET \0".as_ptr() as _);
        /* Deparse targetlist */
        get_update_query_targetlist_def(query, (*query).targetList, context, rte);
        /* Add the FROM clause if needed */
        get_from_clause(query, b" FROM \0".as_ptr() as _, context);
        /* Add a WHERE clause if given */
        if !(*(*query).jointree).quals.is_null() {
            appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            get_rule_expr((*(*query).jointree).quals, context, false);
        }
        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_update_query_targetlist_def - Parse back an UPDATE targetlist
fn get_update_query_targetlist_def(
    query: *mut Query,
    target_list: *mut List,
    context: *mut deparse_context,
    rte: *mut RangeTblEntry,
) {
    unsafe {
        let buf = (*context).buf;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut next_ma_cell: *mut ListCell;
        let mut remaining_ma_columns: i32;
        let mut sep: *const ::std::os::raw::c_char;
        let mut cur_ma_sublink: *mut SubLink;
        let mut ma_sublinks: *mut List = ::std::ptr::null_mut();

        /*
         * Prepare to deal with MULTIEXPR assignments: collect the source SubLinks
         * into a list.  We expect them to appear, in ID order, in resjunk tlist
         * entries.
         */
        if (*query).hasSubLinks {
            foreach!(lc, target_list, {
                let tle = lfirst!(lc) as *mut TargetEntry;
                if (*tle).resjunk && IsA!((*tle).expr, T_SubLink) {
                    let sl = (*tle).expr as *mut SubLink;
                    if (*sl).subLinkType == MULTIEXPR_SUBLINK {
                        ma_sublinks = lappend(ma_sublinks, sl as *mut _);
                        debug_assert!((*sl).subLinkId == list_length(ma_sublinks));
                    }
                }
            });
        }
        next_ma_cell = list_head(ma_sublinks);
        cur_ma_sublink = ::std::ptr::null_mut();
        remaining_ma_columns = 0;

        /* Add the comma separated list of 'attname = value' */
        sep = b"\0".as_ptr() as _;
        foreach!(lc, target_list, {
            let tle = lfirst!(lc) as *mut TargetEntry;
            let mut expr: *mut Node;

            if (*tle).resjunk { continue; } // ignore junk entries

            /* Emit separator (OK whether we're in multiassignment or not) */
            appendStringInfoString(buf, sep);
            sep = b", \0".as_ptr() as _;

            /*
             * Check to see if we're starting a multiassignment group: if so,
             * output a left paren.
             */
            if !next_ma_cell.is_null() && cur_ma_sublink.is_null() {
                /*
                 * We must dig down into the expr to see if it's a PARAM_MULTIEXPR
                 * Param.  That could be buried under FieldStores and
                 * SubscriptingRefs and CoerceToDomains (cf processIndirection()),
                 * and underneath those there could be an implicit type coercion.
                 */
                expr = (*tle).expr as *mut Node;
                loop {
                    if expr.is_null() { break; }
                    if IsA!(expr, T_FieldStore) {
                        let fstore = expr as *mut FieldStore;
                        expr = linitial!((*fstore).newvals) as *mut Node;
                    } else if IsA!(expr, T_SubscriptingRef) {
                        let sbsref = expr as *mut SubscriptingRef;
                        if (*sbsref).refassgnexpr.is_null() { break; }
                        expr = (*sbsref).refassgnexpr as *mut Node;
                    } else if IsA!(expr, T_CoerceToDomain) {
                        let cdomain = expr as *mut CoerceToDomain;
                        if (*cdomain).coercionformat != COERCE_IMPLICIT_CAST { break; }
                        expr = (*cdomain).arg as *mut Node;
                    } else { break; }
                }
                expr = strip_implicit_coercions(expr);

                if !expr.is_null()
                    && IsA!(expr, T_Param)
                    && (*(expr as *mut Param)).paramkind == PARAM_MULTIEXPR
                {
                    cur_ma_sublink = lfirst!(next_ma_cell) as *mut SubLink;
                    next_ma_cell = lnext(ma_sublinks, next_ma_cell);
                    remaining_ma_columns = count_nonjunk_tlist_entries(
                        (*((*cur_ma_sublink).subselect as *mut Query)).targetList);
                    debug_assert!(
                        (*(expr as *mut Param)).paramid == (((*cur_ma_sublink).subLinkId << 16) | 1));
                    appendStringInfoChar(buf, b'(' as _);
                }
            }

            /*
             * Put out name of target column; look in the catalogs, not at
             * tle->resname, since resname will fail to track RENAME.
             */
            appendStringInfoString(buf,
                quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));

            /*
             * Print any indirection needed (subfields or subscripts), and strip
             * off the top-level nodes representing the indirection assignments.
             */
            expr = processIndirection((*tle).expr as *mut Node, context);

            /*
             * If we're in a multiassignment, skip printing anything more, unless
             * this is the last column; in which case, what we print should be the
             * sublink, not the Param.
             */
            if !cur_ma_sublink.is_null() {
                remaining_ma_columns -= 1;
                if remaining_ma_columns > 0 { continue; } // not the last column of multiassignment
                appendStringInfoChar(buf, b')' as _);
                expr = cur_ma_sublink as *mut Node;
                cur_ma_sublink = ::std::ptr::null_mut();
            }

            appendStringInfoString(buf, b" = \0".as_ptr() as _);
            get_rule_expr(expr, context, false);
        });
    }
}

// get_delete_query_def - Parse back a DELETE parsetree
fn get_delete_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with DELETE FROM relname */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "DELETE FROM {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        /* Add the USING clause if given */
        get_from_clause(query, b" USING \0".as_ptr() as _, context);
        /* Add a WHERE clause if given */
        if !(*(*query).jointree).quals.is_null() {
            appendContextKeyword(context, b" WHERE \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 1);
            get_rule_expr((*(*query).jointree).quals, context, false);
        }
        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_merge_query_def - Parse back a MERGE parsetree
fn get_merge_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut lc: *mut ListCell = ::std::ptr::null_mut();
        let mut have_not_matched_by_source = false;

        /* Insert the WITH clause if given */
        get_with_clause(query, context);
        /* Start the query with MERGE INTO relname */
        let rte = rt_fetch((*query).resultRelation, (*query).rtable);
        debug_assert!((*rte).rtekind == RTE_RELATION);
        if PRETTY_INDENT(context) {
            appendStringInfoChar(buf, b' ' as _);
            (*context).indentLevel += PRETTYINDENT_STD as i32;
        }
        appendStringInfo!(buf, "MERGE INTO {}{}",
            ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid, ::std::ptr::null_mut())).to_string_lossy());
        /* Print the relation alias, if needed */
        get_rte_alias(rte, (*query).resultRelation, false, context);
        /* Print the source relation and join clause */
        get_from_clause(query, b" USING \0".as_ptr() as _, context);
        appendContextKeyword(context, b" ON \0".as_ptr() as _,
            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
        get_rule_expr((*query).mergeJoinCondition, context, false);

        /*
         * Test for any NOT MATCHED BY SOURCE actions.  If there are none, then
         * any NOT MATCHED BY TARGET actions are output as "WHEN NOT MATCHED", per
         * SQL standard.  Otherwise, we have a non-SQL-standard query, so output
         * "BY SOURCE" / "BY TARGET" qualifiers for all NOT MATCHED actions, to be
         * more explicit.
         */
        foreach!(lc, (*query).mergeActionList, {
            let action = lfirst_node!(MergeAction, T_MergeAction, lc);
            if (*action).matchKind == MERGE_WHEN_NOT_MATCHED_BY_SOURCE {
                have_not_matched_by_source = true;
                break;
            }
        });

        /* Print each merge action */
        foreach!(lc, (*query).mergeActionList, {
            let action = lfirst_node!(MergeAction, T_MergeAction, lc);

            appendContextKeyword(context, b" WHEN \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 2);
            match (*action).matchKind {
                MERGE_WHEN_MATCHED => {
                    appendStringInfoString(buf, b"MATCHED\0".as_ptr() as _);
                }
                MERGE_WHEN_NOT_MATCHED_BY_SOURCE => {
                    appendStringInfoString(buf, b"NOT MATCHED BY SOURCE\0".as_ptr() as _);
                }
                MERGE_WHEN_NOT_MATCHED_BY_TARGET => {
                    if have_not_matched_by_source {
                        appendStringInfoString(buf, b"NOT MATCHED BY TARGET\0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b"NOT MATCHED\0".as_ptr() as _);
                    }
                }
                _ => {
                    elog!(ERROR, "unrecognized matchKind: {}", (*action).matchKind as i32);
                }
            }
            if !(*action).qual.is_null() {
                appendContextKeyword(context, b" AND \0".as_ptr() as _,
                    -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 3);
                get_rule_expr((*action).qual, context, false);
            }
            appendContextKeyword(context, b" THEN \0".as_ptr() as _,
                -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 3);

            if (*action).commandType == CMD_INSERT {
                /* This generally matches get_insert_query_def() */
                let mut strippedexprs: *mut List = ::std::ptr::null_mut();
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut lc2: *mut ListCell = ::std::ptr::null_mut();

                appendStringInfoString(buf, b"INSERT\0".as_ptr() as _);
                if !(*action).targetList.is_null() {
                    appendStringInfoString(buf, b" (\0".as_ptr() as _);
                }
                foreach!(lc2, (*action).targetList, {
                    let tle = lfirst!(lc2) as *mut TargetEntry;
                    debug_assert!(!(*tle).resjunk);
                    appendStringInfoString(buf, sep);
                    sep = b", \0".as_ptr() as _;
                    appendStringInfoString(buf, quote_identifier(get_attname((*rte).relid, (*tle).resno, false)));
                    strippedexprs = lappend(strippedexprs,
                        processIndirection((*tle).expr as *mut Node, context));
                });
                if !(*action).targetList.is_null() {
                    appendStringInfoChar(buf, b')' as _);
                }
                if (*action).r#override == OVERRIDING_SYSTEM_VALUE {
                    appendStringInfoString(buf, b" OVERRIDING SYSTEM VALUE\0".as_ptr() as _);
                } else if (*action).r#override == OVERRIDING_USER_VALUE {
                    appendStringInfoString(buf, b" OVERRIDING USER VALUE\0".as_ptr() as _);
                }
                if !strippedexprs.is_null() {
                    appendContextKeyword(context, b" VALUES (\0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD as i32, 4);
                    get_rule_list_toplevel(strippedexprs, context, false);
                    appendStringInfoChar(buf, b')' as _);
                } else {
                    appendStringInfoString(buf, b" DEFAULT VALUES\0".as_ptr() as _);
                }
            } else if (*action).commandType == CMD_UPDATE {
                appendStringInfoString(buf, b"UPDATE SET \0".as_ptr() as _);
                get_update_query_targetlist_def(query, (*action).targetList, context, rte);
            } else if (*action).commandType == CMD_DELETE {
                appendStringInfoString(buf, b"DELETE\0".as_ptr() as _);
            } else if (*action).commandType == CMD_NOTHING {
                appendStringInfoString(buf, b"DO NOTHING\0".as_ptr() as _);
            }
        });

        /* Add RETURNING if present */
        if !(*query).returningList.is_null() {
            get_returning_clause(query, context);
        }
    }
}

// get_utility_query_def - Parse back a UTILITY parsetree
fn get_utility_query_def(query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if !(*query).utilityStmt.is_null() && IsA!((*query).utilityStmt, T_NotifyStmt) {
            let stmt = (*query).utilityStmt as *mut NotifyStmt;
            appendContextKeyword(context, b"\0".as_ptr() as _, 0, PRETTYINDENT_STD as i32, 1);
            appendStringInfo!(buf, "NOTIFY {}",
                ::std::ffi::CStr::from_ptr(quote_identifier((*stmt).conditionname)).to_string_lossy());
            if !(*stmt).payload.is_null() {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                simple_quote_literal(buf, (*stmt).payload);
            }
        } else {
            /* Currently only NOTIFY utility commands can appear in rules */
            elog!(ERROR, "unexpected utility statement type");
        }
    }
}

/*
 * Display a Var appropriately.
 *
 * In some cases (currently only when recursing into an unnamed join)
 * the Var's varlevelsup has to be interpreted with respect to a context
 * above the current one; levelsup indicates the offset.
 *
 * If istoplevel is true, the Var is at the top level of a SELECT's
 * targetlist, which means we need special treatment of whole-row Vars.
 * Instead of the normal "tab.*", we'll print "tab.*::typename".
 *
 * Returns the attname of the Var, or NULL if the Var has no attname.
 */
fn get_variable(
    var: *mut Var,
    levelsup: i32,
    istoplevel: bool,
    context: *mut deparse_context,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let buf = (*context).buf;
        let rte: *mut RangeTblEntry;
        let mut attnum: AttrNumber;
        let netlevelsup: i32;
        let dpns: *mut deparse_namespace;
        let mut varno: i32;
        let mut varattno: AttrNumber;
        let colinfo: *mut deparse_columns;
        let refname: *mut ::std::os::raw::c_char;
        let attname: *mut ::std::os::raw::c_char;
        let mut need_prefix: bool;

        /* Find appropriate nesting depth */
        netlevelsup = (*var).varlevelsup as i32 + levelsup;
        if netlevelsup >= list_length((*context).namespaces) {
            elog!(ERROR, "bogus varlevelsup: {} offset {}",
                (*var).varlevelsup, levelsup);
        }
        dpns = list_nth((*context).namespaces, netlevelsup) as *mut deparse_namespace;

        /*
         * If we have a syntactic referent for the Var, and we're working from a
         * parse tree, prefer to use the syntactic referent.  Otherwise, fall back
         * on the semantic referent.
         */
        if (*var).varnosyn > 0 && (*dpns).plan.is_null() {
            varno = (*var).varnosyn as i32;
            varattno = (*var).varattnosyn;
        } else {
            varno = (*var).varno as i32;
            varattno = (*var).varattno;
        }

        /*
         * Try to find the relevant RTE in this rtable.  In a plan tree, it's
         * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
         * down into the subplans, or INDEX_VAR, which is resolved similarly. Also
         * find the aliases previously assigned for this RTE.
         */
        if varno >= 1 && varno <= list_length((*dpns).rtable) {
            /*
             * We might have been asked to map child Vars to some parent relation.
             */
            if !(*context).appendparents.is_null() && !(*dpns).appendrels.is_null() {
                let mut pvarno = varno;
                let mut pvarattno = varattno;
                let mut appinfo = *(*dpns).appendrels.add(pvarno as usize);
                let mut found = false;

                /* Only map up to inheritance parents, not UNION ALL appendrels */
                while !appinfo.is_null()
                    && (*rt_fetch((*appinfo).parent_relid as i32, (*dpns).rtable)).rtekind == RTE_RELATION
                {
                    found = false;
                    if pvarattno > 0 {
                        // system columns stay as-is
                        if pvarattno > (*appinfo).num_child_cols {
                            break; // safety check
                        }
                        pvarattno = *(*appinfo).parent_colnos.add(pvarattno as usize - 1);
                        if pvarattno == 0 {
                            break; // Var is local to child
                        }
                    }
                    pvarno = (*appinfo).parent_relid as i32;
                    found = true;
                    /* If the parent is itself a child, continue up. */
                    debug_assert!(pvarno > 0 && pvarno <= list_length((*dpns).rtable));
                    appinfo = *(*dpns).appendrels.add(pvarno as usize);
                }
                /*
                 * If we found an ancestral rel, and that rel is included in
                 * appendparents, print that column not the original one.
                 */
                if found && bms_is_member(pvarno, (*context).appendparents) {
                    varno = pvarno;
                    varattno = pvarattno;
                }
            }

            rte = rt_fetch(varno, (*dpns).rtable);

            /* might be returning old/new column value */
            if (*var).varreturningtype == VAR_RETURNING_OLD {
                refname = (*dpns).ret_old_alias;
            } else if (*var).varreturningtype == VAR_RETURNING_NEW {
                refname = (*dpns).ret_new_alias;
            } else {
                refname = list_nth((*dpns).rtable_names, varno - 1) as *mut ::std::os::raw::c_char;
            }

            colinfo = deparse_columns_fetch(varno, dpns);
            attnum = varattno;
        } else {
            resolve_special_varno(var as *mut Node, context, get_special_variable, ::std::ptr::null_mut());
            return ::std::ptr::null_mut();
        }

        /*
         * The planner will sometimes emit Vars referencing resjunk elements of a
         * subquery's target list.  If that is the case, drill down to the subplan
         * and print the contents of the referenced tlist item.
         */
        if ((*rte).rtekind == RTE_SUBQUERY || (*rte).rtekind == RTE_CTE)
            && attnum > list_length((*rte).eref.colnames)
            && !(*dpns).inner_plan.is_null()
        {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();

            tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
            if tle.is_null() {
                elog!(ERROR, "invalid attnum {} for relation \"{}\"",
                    attnum,
                    ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy());
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);

            /*
             * Force parentheses because our caller probably assumed a Var is a
             * simple expression.
             */
            if !IsA!((*tle).expr, T_Var) { appendStringInfoChar(buf, b'(' as _); }
            get_rule_expr((*tle).expr as *mut Node, context, true);
            if !IsA!((*tle).expr, T_Var) { appendStringInfoChar(buf, b')' as _); }

            pop_child_plan(dpns, &mut save_dpns);
            return ::std::ptr::null_mut();
        }

        /*
         * If it's an unnamed join, look at the expansion of the alias variable.
         * If it's a simple reference to one of the input vars, then recursively
         * print the name of that var instead.
         */
        if (*rte).rtekind == RTE_JOIN && (*rte).alias.is_null() {
            if (*rte).joinaliasvars.is_null() {
                elog!(ERROR, "cannot decompile join alias var in plan tree");
            }
            if attnum > 0 {
                let aliasvar = list_nth((*rte).joinaliasvars, attnum as i32 - 1) as *mut Var;
                /* we intentionally don't strip implicit coercions here */
                if !aliasvar.is_null() && IsA!(aliasvar, T_Var) {
                    return get_variable(aliasvar, (*var).varlevelsup as i32 + levelsup, istoplevel, context);
                }
            }
            /*
             * Unnamed join has no refname.
             */
            // refname is already set, but for unnamed join it should be NULL
            // (asserted by the C code via Assert(refname == NULL))
            debug_assert!(refname.is_null());
        }

        if attnum == InvalidAttrNumber {
            attname = ::std::ptr::null_mut();
        } else if attnum > 0 {
            /* Get column name to use from the colinfo struct */
            if attnum > (*colinfo).num_cols {
                elog!(ERROR, "invalid attnum {} for relation \"{}\"",
                    attnum,
                    ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy());
            }
            attname = *(*colinfo).colnames.add(attnum as usize - 1);
            /*
             * If we find a Var referencing a dropped column, print something
             * rather than fail.
             */
            if attname.is_null() {
                attname = b"?dropped?column?\0".as_ptr() as *mut _;
            }
        } else {
            /* System column - name is fixed, get it from the catalog */
            attname = get_rte_attribute_name(rte, attnum);
        }

        need_prefix = (*context).varprefix
            || attname.is_null()
            || (*var).varreturningtype != VAR_RETURNING_DEFAULT;

        /*
         * If we're considering a plain Var in an ORDER BY (but not GROUP BY)
         * clause, we may need to add a table-name prefix.
         */
        if (*context).varInOrderBy && !(*context).inGroupBy && !need_prefix {
            let mut colno = 0i32;
            let mut lc_tl: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_tl, (*context).targetList, {
                let tle = lfirst!(lc_tl) as *mut TargetEntry;
                let colname: *mut ::std::os::raw::c_char;
                if (*tle).resjunk { continue; } // ignore junk entries
                colno += 1;
                /* This must match colname-choosing logic in get_target_list() */
                if !(*context).resultDesc.is_null() && colno <= (*(*context).resultDesc).natts {
                    colname = NameStr!((*TupleDescAttr((*context).resultDesc, colno - 1)).attname);
                } else {
                    colname = (*tle).resname;
                }
                if !colname.is_null()
                    && !attname.is_null()
                    && libc::strcmp(colname, attname) == 0
                    && !equal(var as *mut _, (*tle).expr as *mut _)
                {
                    need_prefix = true;
                    break;
                }
            });
        }

        if !refname.is_null() && need_prefix {
            appendStringInfoString(buf, quote_identifier(refname));
            appendStringInfoChar(buf, b'.' as _);
        }
        if !attname.is_null() {
            appendStringInfoString(buf, quote_identifier(attname));
        } else {
            appendStringInfoChar(buf, b'*' as _);
            if istoplevel {
                appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod((*var).vartype, (*var).vartypmod)).to_string_lossy());
            }
        }

        attname
    }
}

/*
 * Deparse a Var which references OUTER_VAR, INNER_VAR, or INDEX_VAR.
 * This routine is actually a callback for resolve_special_varno.
 */
fn get_special_variable(node: *mut Node, context: *mut deparse_context, _callback_arg: *mut ::std::os::raw::c_void) {
    unsafe {
        let buf = (*context).buf;
        /*
         * For a non-Var referent, force parentheses because our caller probably
         * assumed a Var is a simple expression.
         */
        if !IsA!(node, T_Var) { appendStringInfoChar(buf, b'(' as _); }
        get_rule_expr(node, context, true);
        if !IsA!(node, T_Var) { appendStringInfoChar(buf, b')' as _); }
    }
}

/*
 * Chase through plan references to special varnos (OUTER_VAR, INNER_VAR,
 * INDEX_VAR) until we find a real Var or some kind of non-Var node; then,
 * invoke the callback provided.
 */
fn resolve_special_varno(
    node: *mut Node,
    context: *mut deparse_context,
    callback: rsv_callback,
    callback_arg: *mut ::std::os::raw::c_void,
) {
    unsafe {
        /* This function is recursive, so let's be paranoid. */
        check_stack_depth();

        /* If it's not a Var, invoke the callback. */
        if !IsA!(node, T_Var) {
            callback(node, context, callback_arg);
            return;
        }

        /* Find appropriate nesting depth */
        let var = node as *mut Var;
        let dpns = list_nth((*context).namespaces, (*var).varlevelsup as i32) as *mut deparse_namespace;

        /*
         * If varno is special, recurse.  (Don't worry about varnosyn; if we're
         * here, we already decided not to use that.)
         */
        if (*var).varno == OUTER_VAR && !(*dpns).outer_tlist.is_null() {
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let save_appendparents = (*context).appendparents;

            let tle = get_tle_by_resno((*dpns).outer_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for OUTER_VAR var: {}", (*var).varattno);
            }

            /*
             * If we're descending to the first child of an Append or MergeAppend,
             * update appendparents.
             */
            if IsA!((*dpns).plan, T_Append) {
                (*context).appendparents = bms_union((*context).appendparents,
                    (*((*dpns).plan as *mut Append)).apprelids);
            } else if IsA!((*dpns).plan, T_MergeAppend) {
                (*context).appendparents = bms_union((*context).appendparents,
                    (*((*dpns).plan as *mut MergeAppend)).apprelids);
            }

            push_child_plan(dpns, (*dpns).outer_plan, &mut save_dpns);
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            pop_child_plan(dpns, &mut save_dpns);
            (*context).appendparents = save_appendparents;
            return;
        } else if (*var).varno == INNER_VAR && !(*dpns).inner_tlist.is_null() {
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();

            let tle = get_tle_by_resno((*dpns).inner_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INNER_VAR var: {}", (*var).varattno);
            }
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            pop_child_plan(dpns, &mut save_dpns);
            return;
        } else if (*var).varno == INDEX_VAR && !(*dpns).index_tlist.is_null() {
            let tle = get_tle_by_resno((*dpns).index_tlist, (*var).varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INDEX_VAR var: {}", (*var).varattno);
            }
            resolve_special_varno((*tle).expr as *mut Node, context, callback, callback_arg);
            return;
        } else if (*var).varno < 1 || (*var).varno > list_length((*dpns).rtable) as u32 {
            elog!(ERROR, "bogus varno: {}", (*var).varno);
        }

        /* Not special.  Just invoke the callback. */
        callback(node, context, callback_arg);
    }
}

/*
 * Get the name of a field of an expression of composite type.
 * The expression is usually a Var, but we handle other cases too.
 *
 * levelsup is an extra offset to interpret the Var's varlevelsup correctly.
 */
fn get_name_for_var_field(
    var: *mut Var,
    fieldno: i32,
    levelsup: i32,
    context: *mut deparse_context,
) -> *const ::std::os::raw::c_char {
    unsafe {
        let mut rte: *mut RangeTblEntry = ::std::ptr::null_mut();
        let mut attnum: AttrNumber = 0;
        let netlevelsup: i32;
        let mut dpns: *mut deparse_namespace = ::std::ptr::null_mut();
        let mut varno: i32;
        let mut varattno: AttrNumber;
        let mut tupdesc: TupleDesc = ::std::ptr::null_mut();
        let mut expr: *mut Node;

        /*
         * If it's a RowExpr that was expanded from a whole-row Var, use the
         * column names attached to it.
         */
        if IsA!(var, T_RowExpr) {
            let r = var as *mut RowExpr;
            if fieldno > 0 && fieldno <= list_length((*r).colnames) {
                return strVal!(list_nth((*r).colnames, fieldno - 1));
            }
        }

        /*
         * If it's a Param of type RECORD, try to find what the Param refers to.
         */
        if IsA!(var, T_Param) {
            let param = var as *mut Param;
            let mut ancestor_cell: *mut ListCell = ::std::ptr::null_mut();
            let mut local_dpns: *mut deparse_namespace = ::std::ptr::null_mut();

            let expr_r = find_param_referent(param, context, &mut local_dpns, &mut ancestor_cell);
            if !expr_r.is_null() {
                /* Found a match, so recurse to decipher the field name */
                let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
                let result: *const ::std::os::raw::c_char;

                push_ancestor_plan(local_dpns, ancestor_cell, &mut save_dpns);
                result = get_name_for_var_field(expr_r as *mut Var, fieldno, 0, context);
                pop_ancestor_plan(local_dpns, &mut save_dpns);
                return result;
            }
        }

        /*
         * If it's a Var of type RECORD, we have to find what the Var refers to;
         * if not, we can use get_expr_result_tupdesc().
         */
        if !IsA!(var, T_Var) || (*var).vartype != RECORDOID {
            tupdesc = get_expr_result_tupdesc(var as *mut Node, false);
            /* Got the tupdesc, so we can extract the field name */
            debug_assert!(fieldno >= 1 && fieldno <= (*tupdesc).natts);
            return NameStr!((*TupleDescAttr(tupdesc, fieldno - 1)).attname);
        }

        /* Find appropriate nesting depth */
        netlevelsup = (*var).varlevelsup as i32 + levelsup;
        if netlevelsup >= list_length((*context).namespaces) {
            elog!(ERROR, "bogus varlevelsup: {} offset {}", (*var).varlevelsup, levelsup);
        }
        dpns = list_nth((*context).namespaces, netlevelsup) as *mut deparse_namespace;

        /*
         * If we have a syntactic referent for the Var, and we're working from a
         * parse tree, prefer to use the syntactic referent.
         */
        if (*var).varnosyn > 0 && (*dpns).plan.is_null() {
            varno = (*var).varnosyn as i32;
            varattno = (*var).varattnosyn;
        } else {
            varno = (*var).varno as i32;
            varattno = (*var).varattno;
        }

        /*
         * Try to find the relevant RTE in this rtable.
         */
        if varno >= 1 && varno <= list_length((*dpns).rtable) {
            rte = rt_fetch(varno, (*dpns).rtable);
            attnum = varattno;
        } else if varno == OUTER_VAR as i32 && !(*dpns).outer_tlist.is_null() {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).outer_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for OUTER_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).outer_plan, &mut save_dpns);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            pop_child_plan(dpns, &mut save_dpns);
            return result;
        } else if varno == INNER_VAR as i32 && !(*dpns).inner_tlist.is_null() {
            let tle: *mut TargetEntry;
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).inner_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INNER_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            pop_child_plan(dpns, &mut save_dpns);
            return result;
        } else if varno == INDEX_VAR as i32 && !(*dpns).index_tlist.is_null() {
            let tle: *mut TargetEntry;
            let result: *const ::std::os::raw::c_char;

            tle = get_tle_by_resno((*dpns).index_tlist, varattno);
            if tle.is_null() {
                elog!(ERROR, "bogus varattno for INDEX_VAR var: {}", varattno);
            }
            debug_assert!(netlevelsup == 0);
            result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
            return result;
        } else {
            elog!(ERROR, "bogus varno: {}", varno);
            return ::std::ptr::null(); // keep compiler quiet
        }

        if attnum == InvalidAttrNumber {
            /* Var is whole-row reference to RTE, so select the right field */
            return get_rte_attribute_name(rte, fieldno as AttrNumber);
        }

        /*
         * This part has essentially the same logic as the parser's
         * expandRecordVariable() function.
         */
        expr = var as *mut Node; // default if we can't drill down

        match (*rte).rtekind {
            RTE_RELATION | RTE_VALUES | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
                /*
                 * This case should not occur: a column of a table, values list,
                 * or ENR shouldn't have type RECORD.  Fall through and fail (most
                 * likely) at the bottom.
                 */
            }
            RTE_SUBQUERY => {
                /* Subselect-in-FROM: examine sub-select's output expr */
                if !(*rte).subquery.is_null() {
                    let ste = get_tle_by_resno((*(*rte).subquery).targetList, attnum);
                    if ste.is_null() || (*ste).resjunk {
                        elog!(ERROR, "subquery {} does not have attribute {}",
                            ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(), attnum);
                    }
                    expr = (*ste).expr as *mut Node;
                    if IsA!(expr, T_Var) {
                        /*
                         * Recurse into the sub-select to see what its Var refers to.
                         */
                        let save_nslist = (*context).namespaces;
                        let parent_namespaces = list_copy_tail((*context).namespaces, netlevelsup);
                        let mut mydpns: deparse_namespace = ::std::mem::zeroed();
                        let result: *const ::std::os::raw::c_char;

                        set_deparse_for_query(&mut mydpns, (*rte).subquery, parent_namespaces);
                        (*context).namespaces = lcons(&mut mydpns as *mut _ as *mut _, parent_namespaces);
                        result = get_name_for_var_field(expr as *mut Var, fieldno, 0, context);
                        (*context).namespaces = save_nslist;
                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have complete
                     * RTE entries (in particular, rte->subquery is NULL).
                     */
                    if (*dpns).inner_plan.is_null() {
                        let dummy_name = palloc(32) as *mut ::std::os::raw::c_char;
                        debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_Result));
                        libc::snprintf(dummy_name, 32, b"f%d\0".as_ptr() as _, fieldno);
                        return dummy_name;
                    }
                    debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_SubqueryScan));

                    let tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
                    if tle.is_null() {
                        elog!(ERROR, "bogus varattno for subquery var: {}", attnum);
                    }
                    debug_assert!(netlevelsup == 0);
                    let mut save_dpns2: deparse_namespace = ::std::mem::zeroed();
                    push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns2);
                    let result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
                    pop_child_plan(dpns, &mut save_dpns2);
                    return result;
                }
            }
            RTE_JOIN => {
                /* Join RTE --- recursively inspect the alias variable */
                if (*rte).joinaliasvars.is_null() {
                    elog!(ERROR, "cannot decompile join alias var in plan tree");
                }
                debug_assert!(attnum > 0 && attnum <= list_length((*rte).joinaliasvars) as AttrNumber);
                expr = list_nth((*rte).joinaliasvars, attnum as i32 - 1) as *mut Node;
                debug_assert!(!expr.is_null());
                /* we intentionally don't strip implicit coercions here */
                if IsA!(expr, T_Var) {
                    return get_name_for_var_field(expr as *mut Var, fieldno,
                        (*var).varlevelsup as i32 + levelsup, context);
                }
                /* else fall through to inspect the expression */
            }
            RTE_FUNCTION | RTE_TABLEFUNC => {
                /*
                 * We couldn't get here unless a function is declared with one of
                 * its result columns as RECORD, which is not allowed.
                 */
            }
            RTE_CTE => {
                /* CTE reference: examine subquery's output expr */
                let mut cte: *mut CommonTableExpr = ::std::ptr::null_mut();
                let ctelevelsup: u32 = (*rte).ctelevelsup + netlevelsup as u32;
                let mut lc_cte: *mut ListCell = ::std::ptr::null_mut();

                if ctelevelsup >= list_length((*context).namespaces) as u32 {
                    lc_cte = ::std::ptr::null_mut();
                } else {
                    let ctedpns = list_nth((*context).namespaces, ctelevelsup as i32) as *mut deparse_namespace;
                    foreach!(lc_cte, (*ctedpns).ctes, {
                        cte = lfirst!(lc_cte) as *mut CommonTableExpr;
                        if libc::strcmp((*cte).ctename, (*rte).ctename) == 0 { break; }
                    });
                }

                if !lc_cte.is_null() {
                    let ctequery = (*cte).ctequery as *mut Query;
                    let ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
                    if ste.is_null() || (*ste).resjunk {
                        elog!(ERROR, "CTE {} does not have attribute {}",
                            ::std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(), attnum);
                    }
                    expr = (*ste).expr as *mut Node;
                    if IsA!(expr, T_Var) {
                        let save_nslist = (*context).namespaces;
                        let parent_namespaces = list_copy_tail((*context).namespaces, ctelevelsup as i32);
                        let mut mydpns: deparse_namespace = ::std::mem::zeroed();
                        let result: *const ::std::os::raw::c_char;

                        set_deparse_for_query(&mut mydpns, ctequery, parent_namespaces);
                        (*context).namespaces = lcons(&mut mydpns as *mut _ as *mut _, parent_namespaces);
                        result = get_name_for_var_field(expr as *mut Var, fieldno, 0, context);
                        (*context).namespaces = save_nslist;
                        return result;
                    }
                    /* else fall through to inspect the expression */
                } else {
                    /*
                     * We're deparsing a Plan tree so we don't have a CTE list.
                     */
                    if (*dpns).inner_plan.is_null() {
                        let dummy_name = palloc(32) as *mut ::std::os::raw::c_char;
                        debug_assert!(!(*dpns).plan.is_null() && IsA!((*dpns).plan, T_Result));
                        libc::snprintf(dummy_name, 32, b"f%d\0".as_ptr() as _, fieldno);
                        return dummy_name;
                    }
                    debug_assert!(!(*dpns).plan.is_null()
                        && (IsA!((*dpns).plan, T_CteScan) || IsA!((*dpns).plan, T_WorkTableScan)));

                    let tle = get_tle_by_resno((*dpns).inner_tlist, attnum);
                    if tle.is_null() {
                        elog!(ERROR, "bogus varattno for subquery var: {}", attnum);
                    }
                    debug_assert!(netlevelsup == 0);
                    let mut save_dpns2: deparse_namespace = ::std::mem::zeroed();
                    push_child_plan(dpns, (*dpns).inner_plan, &mut save_dpns2);
                    let result = get_name_for_var_field((*tle).expr as *mut Var, fieldno, levelsup, context);
                    pop_child_plan(dpns, &mut save_dpns2);
                    return result;
                }
            }
            RTE_GROUP => {
                /*
                 * We couldn't get here: any Vars that reference the RTE_GROUP RTE
                 * should have been replaced with the underlying grouping
                 * expressions.
                 */
            }
            _ => {}
        }

        /*
         * We now have an expression we can't expand any more, so see if
         * get_expr_result_tupdesc() can do anything with it.
         */
        tupdesc = get_expr_result_tupdesc(expr, false);
        /* Got the tupdesc, so we can extract the field name */
        debug_assert!(fieldno >= 1 && fieldno <= (*tupdesc).natts);
        NameStr!((*TupleDescAttr(tupdesc, fieldno - 1)).attname)
    }
}

/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpns_p and *ancestor_cell_p
 * appropriately for calling push_ancestor_plan().  If no referent can be
 * found, return NULL.
 */
fn find_param_referent(
    param: *mut Param,
    context: *mut deparse_context,
    dpns_p: *mut *mut deparse_namespace,
    ancestor_cell_p: *mut *mut ListCell,
) -> *mut Node {
    unsafe {
        /* Initialize output parameters to prevent compiler warnings */
        *dpns_p = ::std::ptr::null_mut();
        *ancestor_cell_p = ::std::ptr::null_mut();

        /*
         * If it's a PARAM_EXEC parameter, look for a matching NestLoopParam or
         * SubPlan argument.
         */
        if (*param).paramkind == PARAM_EXEC {
            let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
            let mut child_plan = (*dpns).plan;
            let mut lc: *mut ListCell = ::std::ptr::null_mut();

            foreach!(lc, (*dpns).ancestors, {
                let ancestor = lfirst!(lc) as *mut Node;
                let mut lc2: *mut ListCell = ::std::ptr::null_mut();

                /*
                 * NestLoops transmit params to their inner child only.
                 */
                if IsA!(ancestor, T_NestLoop)
                    && child_plan == innerPlan!(ancestor)
                {
                    let nl = ancestor as *mut NestLoop;
                    foreach!(lc2, (*nl).nestParams, {
                        let nlp = lfirst!(lc2) as *mut NestLoopParam;
                        if (*nlp).paramno == (*param).paramid {
                            /* Found a match, so return it */
                            *dpns_p = dpns;
                            *ancestor_cell_p = lc;
                            return (*nlp).paramval as *mut Node;
                        }
                    });
                }

                /*
                 * If ancestor is a SubPlan, check the arguments it provides.
                 */
                if IsA!(ancestor, T_SubPlan) {
                    let subplan = ancestor as *mut SubPlan;
                    let mut lc3: *mut ListCell = ::std::ptr::null_mut();
                    let mut lc4: *mut ListCell = ::std::ptr::null_mut();

                    forboth!(lc3, (*subplan).parParam, lc4, (*subplan).args, {
                        let paramid = lfirst_int!(lc3);
                        let arg = lfirst!(lc4) as *mut Node;

                        if paramid == (*param).paramid {
                            /*
                             * Found a match, so return it.  But, since Vars in
                             * the arg are to be evaluated in the surrounding
                             * context, we have to point to the next ancestor item
                             * that is *not* a SubPlan.
                             */
                            let mut rest: *mut ListCell = ::std::ptr::null_mut();
                            for_each_cell!(rest, (*dpns).ancestors,
                                lnext((*dpns).ancestors, lc), {
                                let ancestor2 = lfirst!(rest) as *mut Node;
                                if !IsA!(ancestor2, T_SubPlan) {
                                    *dpns_p = dpns;
                                    *ancestor_cell_p = rest;
                                    return arg;
                                }
                            });
                            elog!(ERROR, "SubPlan cannot be outermost ancestor");
                        }
                    });

                    /* SubPlan isn't a kind of Plan, so skip the rest */
                    continue;
                }

                /*
                 * We need not consider the ancestor's initPlan list, since
                 * initplans never have any parParams.
                 */

                /* No luck, crawl up to next ancestor */
                child_plan = ancestor as *mut Plan;
            });
        }

        /* No referent found */
        ::std::ptr::null_mut()
    }
}

/*
 * Try to find a subplan/initplan that emits the value for a PARAM_EXEC Param.
 *
 * If successful, return the generating subplan/initplan and set *column_p
 * to the subplan's 0-based output column number.
 * Otherwise, return NULL.
 */
fn find_param_generator(
    param: *mut Param,
    context: *mut deparse_context,
    column_p: *mut i32,
) -> *mut SubPlan {
    unsafe {
        /* Initialize output parameter to prevent compiler warnings */
        *column_p = 0;

        /*
         * If it's a PARAM_EXEC parameter, search the current plan node as well as
         * ancestor nodes looking for a subplan or initplan that emits the value.
         */
        if (*param).paramkind == PARAM_EXEC {
            let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
            let mut lc: *mut ListCell = ::std::ptr::null_mut();

            /* First check the innermost plan node's initplans */
            let result = find_param_generator_initplan(param, (*dpns).plan, column_p);
            if !result.is_null() { return result; }

            /*
             * The plan's targetlist might contain MULTIEXPR_SUBLINK SubPlans.
             */
            let mut lc_tle: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_tle, (*(*dpns).plan).targetlist, {
                let tle = lfirst!(lc_tle) as *mut TargetEntry;
                if !(*tle).expr.is_null() && IsA!((*tle).expr, T_SubPlan) {
                    let subplan = (*tle).expr as *mut SubPlan;
                    if (*subplan).subLinkType == MULTIEXPR_SUBLINK {
                        let mut idx = 0i32;
                        let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
                        foreach!(lc_p, (*subplan).setParam, {
                            let paramid = lfirst_int!(lc_p);
                            if paramid == (*param).paramid {
                                /* Found a match, so return it. */
                                *column_p = idx;
                                return subplan;
                            }
                            idx += 1;
                        });
                    }
                }
            });

            /* No luck, so check the ancestor nodes */
            foreach!(lc, (*dpns).ancestors, {
                let ancestor = lfirst!(lc) as *mut Node;

                /*
                 * If ancestor is a SubPlan, check the paramIds it provides.
                 */
                if IsA!(ancestor, T_SubPlan) {
                    let subplan = ancestor as *mut SubPlan;
                    let mut idx = 0i32;
                    let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
                    foreach!(lc_p, (*subplan).paramIds, {
                        let paramid = lfirst_int!(lc_p);
                        if paramid == (*param).paramid {
                            /* Found a match, so return it. */
                            *column_p = idx;
                            return subplan;
                        }
                        idx += 1;
                    });

                    /* SubPlan isn't a kind of Plan, so skip the rest */
                    continue;
                }

                /*
                 * Otherwise, it's some kind of Plan node, so check its initplans.
                 */
                let result2 = find_param_generator_initplan(param, ancestor as *mut Plan, column_p);
                if !result2.is_null() { return result2; }

                /* No luck, crawl up to next ancestor */
            });
        }

        /* No generator found */
        ::std::ptr::null_mut()
    }
}

// Subroutine for find_param_generator: search one Plan node's initplans
fn find_param_generator_initplan(param: *mut Param, plan: *mut Plan, column_p: *mut i32) -> *mut SubPlan {
    unsafe {
        let mut lc_sp: *mut ListCell = ::std::ptr::null_mut();
        foreach!(lc_sp, (*plan).initPlan, {
            let subplan = lfirst!(lc_sp) as *mut SubPlan;
            let mut idx = 0i32;
            let mut lc_p: *mut ListCell = ::std::ptr::null_mut();
            foreach!(lc_p, (*subplan).setParam, {
                let paramid = lfirst_int!(lc_p);
                if paramid == (*param).paramid {
                    /* Found a match, so return it. */
                    *column_p = idx;
                    return subplan;
                }
                idx += 1;
            });
        });
        ::std::ptr::null_mut()
    }
}

// Display a Param appropriately.
fn get_parameter(param: *mut Param, context: *mut deparse_context) {
    unsafe {
        let mut dpns: *mut deparse_namespace = ::std::ptr::null_mut();
        let mut ancestor_cell: *mut ListCell = ::std::ptr::null_mut();
        let mut column: i32 = 0;

        /*
         * If it's a PARAM_EXEC parameter, try to locate the expression from which
         * the parameter was computed.
         */
        let expr = find_param_referent(param, context, &mut dpns, &mut ancestor_cell);
        if !expr.is_null() {
            /* Found a match, so print it */
            let mut save_dpns: deparse_namespace = ::std::mem::zeroed();
            let save_varprefix: bool;
            let need_paren: bool;

            /* Switch attention to the ancestor plan node */
            push_ancestor_plan(dpns, ancestor_cell, &mut save_dpns);

            /*
             * Force prefixing of Vars, since they won't belong to the relation
             * being scanned in the original plan node.
             */
            save_varprefix = (*context).varprefix;
            (*context).varprefix = true;

            /*
             * A Param's expansion is typically a Var, Aggref, GroupingFunc, or
             * upper-level Param, which wouldn't need extra parentheses.
             */
            need_paren = !(IsA!(expr, T_Var)
                || IsA!(expr, T_Aggref)
                || IsA!(expr, T_GroupingFunc)
                || IsA!(expr, T_Param));
            if need_paren { appendStringInfoChar((*context).buf, b'(' as _); }

            get_rule_expr(expr, context, false);

            if need_paren { appendStringInfoChar((*context).buf, b')' as _); }

            (*context).varprefix = save_varprefix;

            pop_ancestor_plan(dpns, &mut save_dpns);

            return;
        }

        /*
         * Alternatively, maybe it's a subplan output.
         */
        let subplan = find_param_generator(param, context, &mut column);
        if !subplan.is_null() {
            let hashstr = if (*subplan).useHashTable { "hashed " } else { "" };
            appendStringInfo!((*context).buf, "({}{}).col{}",
                hashstr,
                ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy(),
                column + 1);
            return;
        }

        /*
         * If it's an external parameter, see if the outermost namespace provides
         * function argument names.
         */
        if (*param).paramkind == PARAM_EXTERN && !(*context).namespaces.is_null() {
            dpns = llast!((*context).namespaces) as *mut deparse_namespace;
            if !(*dpns).argnames.is_null()
                && (*param).paramid > 0
                && (*param).paramid <= (*dpns).numargs
            {
                let argname = *(*dpns).argnames.add((*param).paramid as usize - 1);
                if !argname.is_null() {
                    let mut should_qualify = false;
                    let mut lc: *mut ListCell = ::std::ptr::null_mut();

                    /*
                     * Qualify the parameter name if there are any other deparse
                     * namespaces with range tables.
                     */
                    foreach!(lc, (*context).namespaces, {
                        let depns = lfirst!(lc) as *mut deparse_namespace;
                        if !(*depns).rtable_names.is_null() {
                            should_qualify = true;
                            break;
                        }
                    });
                    if should_qualify {
                        appendStringInfoString((*context).buf, quote_identifier((*dpns).funcname));
                        appendStringInfoChar((*context).buf, b'.' as _);
                    }

                    appendStringInfoString((*context).buf, quote_identifier(argname));
                    return;
                }
            }
        }

        /*
         * Not PARAM_EXEC, or couldn't find referent: just print $N.
         *
         * It's a bug if we get here for anything except PARAM_EXTERN Params, but
         * in production builds printing $N seems more useful than failing.
         */
        debug_assert!((*param).paramkind == PARAM_EXTERN);
        appendStringInfo!((*context).buf, "${}", (*param).paramid);
    }
}

/*
 * get_simple_binary_op_name
 *
 * helper function for isSimpleNode
 * will return single char binary operator name, or NULL if it's not
 */
fn get_simple_binary_op_name(expr: *mut OpExpr) -> *const ::std::os::raw::c_char {
    unsafe {
        let args = (*expr).args;
        if list_length(args) == 2 {
            /* binary operator */
            let arg1 = linitial!(args) as *mut Node;
            let arg2 = lsecond!(args) as *mut Node;
            let op = generate_operator_name((*expr).opno, exprType(arg1), exprType(arg2));
            if !op.is_null() && libc::strlen(op) == 1 {
                return op;
            }
        }
        ::std::ptr::null()
    }
}

/*
 * isSimpleNode - check if given node is simple (doesn't need parenthesizing)
 *
 *  true   : simple in the context of parent node's type
 *  false  : not simple
 */
fn isSimpleNode(node: *mut Node, parent_node: *mut Node, pretty_flags: i32) -> bool {
    unsafe {
        if node.is_null() { return false; }

        match nodeTag!(node) {
            T_Var | T_Const | T_Param | T_CoerceToDomainValue | T_SetToDefault | T_CurrentOfExpr => {
                /* single words: always simple */
                true
            }
            T_SubscriptingRef
            | T_ArrayExpr
            | T_RowExpr
            | T_CoalesceExpr
            | T_MinMaxExpr
            | T_SQLValueFunction
            | T_XmlExpr
            | T_NextValueExpr
            | T_NullIfExpr
            | T_Aggref
            | T_GroupingFunc
            | T_WindowFunc
            | T_MergeSupportFunc
            | T_FuncExpr
            | T_JsonConstructorExpr
            | T_JsonExpr => {
                /* function-like: name(..) or name[..] */
                true
            }
            /* CASE keywords act as parentheses */
            T_CaseExpr => true,

            T_FieldSelect => {
                /*
                 * appears simple since . has top precedence, unless parent is
                 * T_FieldSelect itself!
                 */
                !IsA!(parent_node, T_FieldSelect)
            }
            T_FieldStore => {
                /* treat like FieldSelect (probably doesn't matter) */
                !IsA!(parent_node, T_FieldStore)
            }
            T_CoerceToDomain => {
                /* maybe simple, check args */
                isSimpleNode((*(node as *mut CoerceToDomain)).arg as *mut Node, node, pretty_flags)
            }
            T_RelabelType => {
                isSimpleNode((*(node as *mut RelabelType)).arg as *mut Node, node, pretty_flags)
            }
            T_CoerceViaIO => {
                isSimpleNode((*(node as *mut CoerceViaIO)).arg as *mut Node, node, pretty_flags)
            }
            T_ArrayCoerceExpr => {
                isSimpleNode((*(node as *mut ArrayCoerceExpr)).arg as *mut Node, node, pretty_flags)
            }
            T_ConvertRowtypeExpr => {
                isSimpleNode((*(node as *mut ConvertRowtypeExpr)).arg as *mut Node, node, pretty_flags)
            }
            T_ReturningExpr => {
                isSimpleNode((*(node as *mut ReturningExpr)).retexpr as *mut Node, node, pretty_flags)
            }
            T_OpExpr => {
                /* depends on parent node type; needs further checking */
                if (pretty_flags & PRETTYFLAG_PAREN) != 0 && IsA!(parent_node, T_OpExpr) {
                    let op = get_simple_binary_op_name(node as *mut OpExpr);
                    if op.is_null() { return false; }

                    /* We know only the basic operators + - and * / % */
                    let oc = *op as u8;
                    let is_lopriop = oc == b'+' || oc == b'-';
                    let is_hipriop = oc == b'*' || oc == b'/' || oc == b'%';
                    if !(is_lopriop || is_hipriop) { return false; }

                    let parent_op = get_simple_binary_op_name(parent_node as *mut OpExpr);
                    if parent_op.is_null() { return false; }

                    let poc = *parent_op as u8;
                    let is_lopriparent = poc == b'+' || poc == b'-';
                    let is_hipriparent = poc == b'*' || poc == b'/' || poc == b'%';
                    if !(is_lopriparent || is_hipriparent) { return false; }

                    if is_hipriop && is_lopriparent { return true; } // op binds tighter than parent
                    if is_lopriop && is_hipriparent { return false; }

                    /*
                     * Operators are same priority --- can skip parens only if
                     * we have (a - b) - c, not a - (b - c).
                     */
                    if node == linitial!((*(parent_node as *mut OpExpr)).args) as *mut Node {
                        return true;
                    }
                    return false;
                }
                /* else do the same stuff as for T_SubLink et al. */
                // fall through
                match nodeTag!(parent_node) {
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_BoolExpr      // lower precedence
                    | T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr => true, // other separators
                    _ => false,
                }
            }
            T_SubLink | T_NullTest | T_BooleanTest | T_DistinctExpr | T_JsonIsPredicate => {
                match nodeTag!(parent_node) {
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_BoolExpr      // lower precedence
                    | T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr => true, // other separators
                    _ => false,
                }
            }
            T_BoolExpr => {
                match nodeTag!(parent_node) {
                    T_BoolExpr => {
                        if (pretty_flags & PRETTYFLAG_PAREN) != 0 {
                            let r#type = (*(node as *mut BoolExpr)).boolop;
                            let parent_type = (*(parent_node as *mut BoolExpr)).boolop;
                            match r#type {
                                NOT_EXPR | AND_EXPR => {
                                    if parent_type == AND_EXPR || parent_type == OR_EXPR { return true; }
                                }
                                OR_EXPR => {
                                    if parent_type == OR_EXPR { return true; }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    T_FuncExpr => {
                        let r#type = (*(parent_node as *mut FuncExpr)).funcformat;
                        if r#type == COERCE_EXPLICIT_CAST || r#type == COERCE_IMPLICIT_CAST || r#type == COERCE_SQL_SYNTAX {
                            false
                        } else {
                            true // own parentheses
                        }
                    }
                    T_SubscriptingRef // other separators
                    | T_ArrayExpr   // other separators
                    | T_RowExpr     // other separators
                    | T_CoalesceExpr  // own parentheses
                    | T_MinMaxExpr  // own parentheses
                    | T_XmlExpr     // own parentheses
                    | T_NullIfExpr  // other separators
                    | T_Aggref      // own parentheses
                    | T_GroupingFunc // own parentheses
                    | T_WindowFunc  // own parentheses
                    | T_CaseExpr    // other separators
                    | T_JsonExpr => true, // own parentheses
                    _ => false,
                }
            }
            T_JsonValueExpr => {
                /* maybe simple, check args */
                isSimpleNode((*(node as *mut JsonValueExpr)).raw_expr as *mut Node, node, pretty_flags)
            }
            _ => {
                false // those we don't know: in dubio complexo
            }
        }
    }
}

/*
 * appendContextKeyword - append a keyword to buffer
 *
 * If prettyPrint is enabled, perform a line break, and adjust indentation.
 * Otherwise, just append the keyword.
 */
fn appendContextKeyword(
    context: *mut deparse_context,
    str: *const ::std::os::raw::c_char,
    indent_before: i32,
    indent_after: i32,
    indent_plus: i32,
) {
    unsafe {
        let buf = (*context).buf;

        if PRETTY_INDENT(context) {
            let indent_amount: i32;

            (*context).indentLevel += indent_before;

            /* remove any trailing spaces currently in the buffer ... */
            removeStringInfoSpaces(buf);
            /* ... then add a newline and some spaces */
            appendStringInfoChar(buf, b'\n' as _);

            if (*context).indentLevel < PRETTYINDENT_LIMIT as i32 {
                indent_amount = ::std::cmp::max((*context).indentLevel, 0) + indent_plus;
            } else {
                /*
                 * If we're indented more than PRETTYINDENT_LIMIT characters, try
                 * to conserve horizontal space by reducing the per-level
                 * indentation.
                 */
                let mut ia = PRETTYINDENT_LIMIT as i32
                    + ((*context).indentLevel - PRETTYINDENT_LIMIT as i32)
                        / (PRETTYINDENT_STD as i32 / 2);
                ia %= PRETTYINDENT_LIMIT as i32;
                /* scale/wrap logic affects indentLevel, but not indentPlus */
                ia += indent_plus;
                indent_amount = ia;
            }
            appendStringInfoSpaces(buf, indent_amount as u32);

            appendStringInfoString(buf, str);

            (*context).indentLevel += indent_after;
            if (*context).indentLevel < 0 {
                (*context).indentLevel = 0;
            }
        } else {
            appendStringInfoString(buf, str);
        }
    }
}

/*
 * removeStringInfoSpaces - delete trailing spaces from a buffer.
 *
 * Possibly this should move to stringinfo.c at some point.
 */
fn removeStringInfoSpaces(str: *mut StringInfo) {
    unsafe {
        while (*str).len > 0 && *(*str).data.add((*str).len as usize - 1) == b' ' as _ {
            (*str).len -= 1;
            *(*str).data.add((*str).len as usize) = b'\0' as _;
        }
    }
}

/*
 * get_rule_expr_paren - deparse expr using get_rule_expr,
 * embracing the string with parentheses if necessary for prettyPrint.
 *
 * Never embrace if prettyFlags=0, because it's done in the calling node.
 *
 * Any node that does *not* embrace its argument node by sql syntax should
 * use get_rule_expr_paren instead of get_rule_expr so parentheses can be
 * added.
 */
fn get_rule_expr_paren(
    node: *mut Node,
    context: *mut deparse_context,
    showimplicit: bool,
    parent_node: *mut Node,
) {
    unsafe {
        let need_paren = PRETTY_PAREN(context)
            && !isSimpleNode(node, parent_node, (*context).prettyFlags);

        if need_paren { appendStringInfoChar((*context).buf, b'(' as _); }
        get_rule_expr(node, context, showimplicit);
        if need_paren { appendStringInfoChar((*context).buf, b')' as _); }
    }
}

fn get_json_behavior(behavior: *mut JsonBehavior, context: *mut deparse_context, on: *const ::std::os::raw::c_char) {
    unsafe {
        /*
         * The order of array elements must correspond to the order of
         * JsonBehaviorType members.
         */
        let behavior_names: [&[u8]; 9] = [
            b" NULL\0",
            b" ERROR\0",
            b" EMPTY\0",
            b" TRUE\0",
            b" FALSE\0",
            b" UNKNOWN\0",
            b" EMPTY ARRAY\0",
            b" EMPTY OBJECT\0",
            b" DEFAULT \0",
        ];

        if ((*behavior).btype as usize) >= behavior_names.len() {
            elog!(ERROR, "invalid json behavior type: {}", (*behavior).btype as i32);
        }

        appendStringInfoString((*context).buf, behavior_names[(*behavior).btype as usize].as_ptr() as _);

        if (*behavior).btype == JSON_BEHAVIOR_DEFAULT {
            get_rule_expr((*behavior).expr, context, false);
        }

        appendStringInfo!((*context).buf, " ON {}",
            ::std::ffi::CStr::from_ptr(on).to_string_lossy());
    }
}

/*
 * get_json_expr_options
 *
 * Parse back common options for JSON_QUERY, JSON_VALUE, JSON_EXISTS and
 * JSON_TABLE columns.
 */
fn get_json_expr_options(
    jsexpr: *mut JsonExpr,
    context: *mut deparse_context,
    default_behavior: JsonBehaviorType,
) {
    unsafe {
        if (*jsexpr).op == JSON_QUERY_OP {
            if (*jsexpr).wrapper == JSW_CONDITIONAL {
                appendStringInfoString((*context).buf, b" WITH CONDITIONAL WRAPPER\0".as_ptr() as _);
            } else if (*jsexpr).wrapper == JSW_UNCONDITIONAL {
                appendStringInfoString((*context).buf, b" WITH UNCONDITIONAL WRAPPER\0".as_ptr() as _);
            } else if (*jsexpr).wrapper == JSW_NONE || (*jsexpr).wrapper == JSW_UNSPEC {
                /* The default */
                appendStringInfoString((*context).buf, b" WITHOUT WRAPPER\0".as_ptr() as _);
            }

            if (*jsexpr).omit_quotes {
                appendStringInfoString((*context).buf, b" OMIT QUOTES\0".as_ptr() as _);
            } else {
                /* The default */
                appendStringInfoString((*context).buf, b" KEEP QUOTES\0".as_ptr() as _);
            }
        }

        if !(*jsexpr).on_empty.is_null() && (*(*jsexpr).on_empty).btype != default_behavior {
            get_json_behavior((*jsexpr).on_empty, context, b"EMPTY\0".as_ptr() as _);
        }

        if !(*jsexpr).on_error.is_null() && (*(*jsexpr).on_error).btype != default_behavior {
            get_json_behavior((*jsexpr).on_error, context, b"ERROR\0".as_ptr() as _);
        }
    }
}

/*
 * get_rule_expr           - Parse back an expression
 *
 * Note: showimplicit determines whether we display any implicit cast that
 * is present at the top of the expression tree.  It is a passed argument,
 * not a field of the context struct, because we change the value as we
 * recurse down into the expression.  In general we suppress implicit casts
 * when the result type is known with certainty (eg, the arguments of an
 * OR must be boolean).  We display implicit casts for arguments of functions
 * and operators, since this is needed to be certain that the same function
 * or operator will be chosen when the expression is re-parsed.
 */
fn get_rule_expr(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;

        if node.is_null() {
            return;
        }

        /* Guard against excessively long or deeply-nested queries */
        check_stack_depth();

        /*
         * Each level of get_rule_expr must emit an indivisible term
         * (parenthesized if necessary) to ensure result is reparsed into the same
         * expression tree.  The only exception is that when the input is a List,
         * we emit the component items comma-separated with no surrounding
         * decoration; this is convenient for most callers.
         */
        match nodeTag(node) {
            T_Var => {
                get_variable(node as *mut Var, 0, false, context);
            }

            T_Const => {
                get_const_expr(node as *mut Const, context, 0);
            }

            T_Param => {
                get_parameter(node as *mut Param, context);
            }

            T_Aggref => {
                get_agg_expr(node as *mut Aggref, context, node as *mut Aggref);
            }

            T_GroupingFunc => {
                let gexpr = node as *mut GroupingFunc;
                appendStringInfoString(buf, b"GROUPING(\0".as_ptr() as _);
                get_rule_expr((*gexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_WindowFunc => {
                get_windowfunc_expr(node as *mut WindowFunc, context);
            }

            T_MergeSupportFunc => {
                appendStringInfoString(buf, b"MERGE_ACTION()\0".as_ptr() as _);
            }

            T_SubscriptingRef => {
                let sbsref = node as *mut SubscriptingRef;
                /*
                 * If the argument is a CaseTestExpr, we must be inside a
                 * FieldStore, ie, we are assigning to an element of an array
                 * within a composite column.  Since we already punted on
                 * displaying the FieldStore's target information, just punt
                 * here too, and display only the assignment source expression.
                 */
                if IsA!((*sbsref).refexpr, CaseTestExpr) {
                    assert!(!(*sbsref).refassgnexpr.is_null());
                    get_rule_expr((*sbsref).refassgnexpr as *mut Node, context, showimplicit);
                } else {
                    /*
                     * Parenthesize the argument unless it's a simple Var or a
                     * FieldSelect.  (In particular, if it's another
                     * SubscriptingRef, we *must* parenthesize to avoid confusion.)
                     */
                    let need_parens = !IsA!((*sbsref).refexpr, Var) &&
                        !IsA!((*sbsref).refexpr, FieldSelect);
                    if need_parens { appendStringInfoChar(buf, b'(' as _); }
                    get_rule_expr((*sbsref).refexpr as *mut Node, context, showimplicit);
                    if need_parens { appendStringInfoChar(buf, b')' as _); }

                    /*
                     * If there's a refassgnexpr, we want to print the node in the
                     * format "container[subscripts] := refassgnexpr".  This is
                     * not legal SQL, so decompilation of INSERT or UPDATE
                     * statements should always use processIndirection as part of
                     * the statement-level syntax.  We should only see this when
                     * EXPLAIN tries to print the targetlist of a plan resulting
                     * from such a statement.
                     */
                    if !(*sbsref).refassgnexpr.is_null() {
                        /*
                         * Use processIndirection to print this node's subscripts
                         * as well as any additional field selections or
                         * subscripting in immediate descendants.  It returns the
                         * RHS expr that is actually being "assigned".
                         */
                        let refassgnexpr = processIndirection(node, context);
                        appendStringInfoString(buf, b" := \0".as_ptr() as _);
                        get_rule_expr(refassgnexpr, context, showimplicit);
                    } else {
                        /* Just an ordinary container fetch, so print subscripts */
                        printSubscripts(sbsref, context);
                    }
                }
            }

            T_FuncExpr => {
                get_func_expr(node as *mut FuncExpr, context, showimplicit);
            }

            T_NamedArgExpr => {
                let na = node as *mut NamedArgExpr;
                appendStringInfo!(buf, "{} => ",
                    ::std::ffi::CStr::from_ptr(quote_identifier((*na).name)).to_string_lossy());
                get_rule_expr((*na).arg as *mut Node, context, showimplicit);
            }

            T_OpExpr => {
                get_oper_expr(node as *mut OpExpr, context);
            }

            T_DistinctExpr => {
                let expr = node as *mut DistinctExpr;
                let args = (*expr).args;
                let arg1 = linitial!(args) as *mut Node;
                let arg2 = lsecond!(args) as *mut Node;

                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg1, context, true, node);
                appendStringInfoString(buf, b" IS DISTINCT FROM \0".as_ptr() as _);
                get_rule_expr_paren(arg2, context, true, node);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_NullIfExpr => {
                let nullifexpr = node as *mut NullIfExpr;
                appendStringInfoString(buf, b"NULLIF(\0".as_ptr() as _);
                get_rule_expr((*nullifexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_ScalarArrayOpExpr => {
                let expr = node as *mut ScalarArrayOpExpr;
                let args = (*expr).args;
                let arg1 = linitial!(args) as *mut Node;
                let arg2 = lsecond!(args) as *mut Node;

                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg1, context, true, node);
                appendStringInfo!(buf, " {} {} (",
                    ::std::ffi::CStr::from_ptr(generate_operator_name((*expr).opno,
                        exprType(arg1),
                        get_base_element_type(exprType(arg2)))).to_string_lossy(),
                    if (*expr).useOr { "ANY" } else { "ALL" });
                get_rule_expr_paren(arg2, context, true, node);

                /*
                 * There's inherent ambiguity in "x op ANY/ALL (y)" when y is
                 * a bare sub-SELECT.  Since we're here, the sub-SELECT must
                 * be meant as a scalar sub-SELECT yielding an array value to
                 * be used in ScalarArrayOpExpr; but the grammar will
                 * preferentially interpret such a construct as an ANY/ALL
                 * SubLink.  To prevent misparsing the output that way, insert
                 * a dummy coercion (which will be stripped by parse analysis,
                 * so no inefficiency is added in dump and reload).  This is
                 * indeed most likely what the user wrote to get the construct
                 * accepted in the first place.
                 */
                if IsA!(arg2, SubLink) &&
                    (*(arg2 as *mut SubLink)).subLinkType == EXPR_SUBLINK
                {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod(exprType(arg2), exprTypmod(arg2))).to_string_lossy());
                }
                appendStringInfoChar(buf, b')' as _);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_BoolExpr => {
                let expr = node as *mut BoolExpr;
                let first_arg = linitial!((*expr).args) as *mut Node;

                match (*expr).boolop {
                    AND_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        get_rule_expr_paren(first_arg, context, false, node);
                        let mut lc = lnext!((*expr).args, list_head((*expr).args));
                        while !lc.is_null() {
                            appendStringInfoString(buf, b" AND \0".as_ptr() as _);
                            get_rule_expr_paren(crate::current_cell!(lc) as *mut Node, context, false, node);
                            lc = lnext!((*expr).args, lc);
                        }
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    OR_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        get_rule_expr_paren(first_arg, context, false, node);
                        let mut lc = lnext!((*expr).args, list_head((*expr).args));
                        while !lc.is_null() {
                            appendStringInfoString(buf, b" OR \0".as_ptr() as _);
                            get_rule_expr_paren(crate::current_cell!(lc) as *mut Node, context, false, node);
                            lc = lnext!((*expr).args, lc);
                        }
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    NOT_EXPR => {
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                        appendStringInfoString(buf, b"NOT \0".as_ptr() as _);
                        get_rule_expr_paren(first_arg, context, false, node);
                        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
                    }
                    _ => {
                        elog!(ERROR, "unrecognized boolop: {}", (*expr).boolop as i32);
                    }
                }
            }

            T_SubLink => {
                get_sublink_expr(node as *mut SubLink, context);
            }

            T_SubPlan => {
                let subplan = node as *mut SubPlan;
                /*
                 * We cannot see an already-planned subplan in rule deparsing,
                 * only while EXPLAINing a query plan.  We don't try to
                 * reconstruct the original SQL, just reference the subplan
                 * that appears elsewhere in EXPLAIN's result.  It does seem
                 * useful to show the subLinkType and testexpr (if any), and
                 * we also note whether the subplan will be hashed.
                 */
                match (*subplan).subLinkType {
                    EXISTS_SUBLINK => {
                        appendStringInfoString(buf, b"EXISTS(\0".as_ptr() as _);
                    }
                    ALL_SUBLINK => {
                        appendStringInfoString(buf, b"(ALL \0".as_ptr() as _);
                    }
                    ANY_SUBLINK => {
                        appendStringInfoString(buf, b"(ANY \0".as_ptr() as _);
                    }
                    ROWCOMPARE_SUBLINK => {
                        /* Parenthesizing the testexpr seems sufficient */
                        appendStringInfoChar(buf, b'(' as _);
                    }
                    EXPR_SUBLINK => {
                        /* No need to decorate these subplan references */
                        appendStringInfoChar(buf, b'(' as _);
                    }
                    MULTIEXPR_SUBLINK => {
                        /* MULTIEXPR isn't executed in the normal way */
                        appendStringInfoString(buf, b"(rescan \0".as_ptr() as _);
                    }
                    ARRAY_SUBLINK => {
                        appendStringInfoString(buf, b"ARRAY(\0".as_ptr() as _);
                    }
                    CTE_SUBLINK => {
                        /* This case is unreachable within expressions */
                        appendStringInfoString(buf, b"CTE(\0".as_ptr() as _);
                    }
                    _ => {}
                }

                if !(*subplan).testexpr.is_null() {
                    let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
                    /*
                     * Push SubPlan into ancestors list while deparsing
                     * testexpr, so that we can handle PARAM_EXEC references
                     * to the SubPlan's paramIds.  (This makes it look like
                     * the SubPlan is an "ancestor" of the current plan node,
                     * which is a little weird, but it does no harm.)  In this
                     * path, we don't need to mention the SubPlan explicitly,
                     * because the referencing Params will show its existence.
                     */
                    (*dpns).ancestors = lcons(subplan as *mut _, (*dpns).ancestors);

                    get_rule_expr((*subplan).testexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);

                    (*dpns).ancestors = list_delete_first((*dpns).ancestors);
                } else {
                    /* No referencing Params, so show the SubPlan's name */
                    if (*subplan).useHashTable {
                        appendStringInfo!(buf, "hashed {})",
                            ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy());
                    } else {
                        appendStringInfo!(buf, "{})",
                            ::std::ffi::CStr::from_ptr((*subplan).plan_name).to_string_lossy());
                    }
                }
            }

            T_AlternativeSubPlan => {
                let asplan = node as *mut AlternativeSubPlan;
                /*
                 * This case cannot be reached in normal usage, since no
                 * AlternativeSubPlan can appear either in parsetrees or
                 * finished plan trees.  We keep it just in case somebody
                 * wants to use this code to print planner data structures.
                 */
                appendStringInfoString(buf, b"(alternatives: \0".as_ptr() as _);
                let mut lc = list_head((*asplan).subplans);
                while !lc.is_null() {
                    let splan = crate::current_cell!(lc) as *mut SubPlan;
                    if (*splan).useHashTable {
                        appendStringInfo!(buf, "hashed {}",
                            ::std::ffi::CStr::from_ptr((*splan).plan_name).to_string_lossy());
                    } else {
                        appendStringInfoString(buf, (*splan).plan_name);
                    }
                    if !lnext!((*asplan).subplans, lc).is_null() {
                        appendStringInfoString(buf, b" or \0".as_ptr() as _);
                    }
                    lc = lnext!((*asplan).subplans, lc);
                }
                appendStringInfoChar(buf, b')' as _);
            }

            T_FieldSelect => {
                let fselect = node as *mut FieldSelect;
                let arg = (*fselect).arg as *mut Node;
                let fno = (*fselect).fieldnum;

                /*
                 * Parenthesize the argument unless it's an SubscriptingRef or
                 * another FieldSelect.  Note in particular that it would be
                 * WRONG to not parenthesize a Var argument; simplicity is not
                 * the issue here, having the right number of names is.
                 */
                let need_parens = !IsA!(arg, SubscriptingRef) && !IsA!(arg, FieldSelect);
                if need_parens { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr(arg, context, true);
                if need_parens { appendStringInfoChar(buf, b')' as _); }

                /*
                 * Get and print the field name.
                 */
                let fieldname = get_name_for_var_field(arg as *mut Var, fno as _, 0, context);
                appendStringInfo!(buf, ".{}", ::std::ffi::CStr::from_ptr(
                    quote_identifier(fieldname)).to_string_lossy());
            }

            T_FieldStore => {
                let fstore = node as *mut FieldStore;
                /*
                 * There is no good way to represent a FieldStore as real SQL,
                 * so decompilation of INSERT or UPDATE statements should
                 * always use processIndirection as part of the
                 * statement-level syntax.  We should only get here when
                 * EXPLAIN tries to print the targetlist of a plan resulting
                 * from such a statement.  The plan case is even harder than
                 * ordinary rules would be, because the planner tries to
                 * collapse multiple assignments to the same field or subfield
                 * into one FieldStore; so we can see a list of target fields
                 * not just one, and the arguments could be FieldStores
                 * themselves.  We don't bother to try to print the target
                 * field names; we just print the source arguments, with a
                 * ROW() around them if there's more than one.  This isn't
                 * terribly complete, but it's probably good enough for
                 * EXPLAIN's purposes; especially since anything more would be
                 * either hopelessly confusing or an even poorer
                 * representation of what the plan is actually doing.
                 */
                let need_parens = list_length((*fstore).newvals) != 1;
                if need_parens { appendStringInfoString(buf, b"ROW(\0".as_ptr() as _); }
                get_rule_expr((*fstore).newvals as *mut Node, context, showimplicit);
                if need_parens { appendStringInfoChar(buf, b')' as _); }
            }

            T_RelabelType => {
                let relabel = node as *mut RelabelType;
                let arg = (*relabel).arg as *mut Node;
                if (*relabel).relabelformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context,
                        (*relabel).resulttype, (*relabel).resulttypmod, node);
                }
            }

            T_CoerceViaIO => {
                let iocoerce = node as *mut CoerceViaIO;
                let arg = (*iocoerce).arg as *mut Node;
                if (*iocoerce).coerceformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context, (*iocoerce).resulttype, -1, node);
                }
            }

            T_ArrayCoerceExpr => {
                let acoerce = node as *mut ArrayCoerceExpr;
                let arg = (*acoerce).arg as *mut Node;
                if (*acoerce).coerceformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context,
                        (*acoerce).resulttype, (*acoerce).resulttypmod, node);
                }
            }

            T_ConvertRowtypeExpr => {
                let convert = node as *mut ConvertRowtypeExpr;
                let arg = (*convert).arg as *mut Node;
                if (*convert).convertformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr_paren(arg, context, false, node);
                } else {
                    get_coercion_expr(arg, context, (*convert).resulttype, -1, node);
                }
            }

            T_CollateExpr => {
                let collate = node as *mut CollateExpr;
                let arg = (*collate).arg as *mut Node;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren(arg, context, showimplicit, node);
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name((*collate).collOid)).to_string_lossy());
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_CaseExpr => {
                let caseexpr = node as *mut CaseExpr;
                appendContextKeyword(context, b"CASE\0".as_ptr() as _, 0, PRETTYINDENT_VAR, 0);
                if !(*caseexpr).arg.is_null() {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr((*caseexpr).arg as *mut Node, context, true);
                }
                let mut temp = list_head((*caseexpr).args);
                while !temp.is_null() {
                    let when = crate::current_cell!(temp) as *mut CaseWhen;
                    let mut w = (*when).expr as *mut Node;

                    if !(*caseexpr).arg.is_null() {
                        /*
                         * The parser should have produced WHEN clauses of the
                         * form "CaseTestExpr = RHS", possibly with an
                         * implicit coercion inserted above the CaseTestExpr.
                         * For accurate decompilation of rules it's essential
                         * that we show just the RHS.  However in an
                         * expression that's been through the optimizer, the
                         * WHEN clause could be almost anything (since the
                         * equality operator could have been expanded into an
                         * inline function).  If we don't recognize the form
                         * of the WHEN clause, just punt and display it as-is.
                         */
                        if IsA!(w, OpExpr) {
                            let args = (*(w as *mut OpExpr)).args;
                            if list_length(args) == 2 &&
                                IsA!(strip_implicit_coercions(linitial!(args) as *mut Node), CaseTestExpr)
                            {
                                w = lsecond!(args) as *mut Node;
                            }
                        }
                    }

                    if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                    appendContextKeyword(context, b"WHEN \0".as_ptr() as _, 0, 0, 0);
                    get_rule_expr(w, context, false);
                    appendStringInfoString(buf, b" THEN \0".as_ptr() as _);
                    get_rule_expr((*when).result as *mut Node, context, true);
                    temp = lnext!((*caseexpr).args, temp);
                }
                if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                appendContextKeyword(context, b"ELSE \0".as_ptr() as _, 0, 0, 0);
                get_rule_expr((*caseexpr).defresult as *mut Node, context, true);
                if !PRETTY_INDENT!(context) { appendStringInfoChar(buf, b' ' as _); }
                appendContextKeyword(context, b"END\0".as_ptr() as _, -(PRETTYINDENT_VAR as i32), 0, 0);
            }

            T_CaseTestExpr => {
                /*
                 * Normally we should never get here, since for expressions
                 * that can contain this node type we attempt to avoid
                 * recursing to it.  But in an optimized expression we might
                 * be unable to avoid that (see comments for CaseExpr).  If we
                 * do see one, print it as CASE_TEST_EXPR.
                 */
                appendStringInfoString(buf, b"CASE_TEST_EXPR\0".as_ptr() as _);
            }

            T_ArrayExpr => {
                let arrayexpr = node as *mut ArrayExpr;
                appendStringInfoString(buf, b"ARRAY[\0".as_ptr() as _);
                get_rule_expr((*arrayexpr).elements as *mut Node, context, true);
                appendStringInfoChar(buf, b']' as _);

                /*
                 * If the array isn't empty, we assume its elements are
                 * coerced to the desired type.  If it's empty, though, we
                 * need an explicit coercion to the array type.
                 */
                if (*arrayexpr).elements.is_null() {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod((*arrayexpr).array_typeid, -1)).to_string_lossy());
                }
            }

            T_RowExpr => {
                let rowexpr = node as *mut RowExpr;
                let mut tupdesc: TupleDesc = std::ptr::null_mut();

                /*
                 * If it's a named type and not RECORD, we may have to skip
                 * dropped columns and/or claim there are NULLs for added columns.
                 */
                if (*rowexpr).row_typeid != RECORDOID {
                    tupdesc = lookup_rowtype_tupdesc((*rowexpr).row_typeid, -1);
                    assert!(list_length((*rowexpr).args) <= (*tupdesc).natts);
                }

                /*
                 * SQL99 allows "ROW" to be omitted when there is more than
                 * one column, but for simplicity we always print it.
                 */
                appendStringInfoString(buf, b"ROW(\0".as_ptr() as _);
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut i: i32 = 0;
                let mut arg = list_head((*rowexpr).args);
                while !arg.is_null() {
                    let e = crate::current_cell!(arg) as *mut Node;
                    if tupdesc.is_null() || !(*TupleDescAttr(tupdesc, i as _)).attisdropped {
                        appendStringInfoString(buf, sep);
                        /* Whole-row Vars need special treatment here */
                        get_rule_expr_toplevel(e, context, true);
                        sep = b", \0".as_ptr() as _;
                    }
                    i += 1;
                    arg = lnext!((*rowexpr).args, arg);
                }
                if !tupdesc.is_null() {
                    while i < (*tupdesc).natts {
                        if !(*TupleDescAttr(tupdesc, i as _)).attisdropped {
                            appendStringInfoString(buf, sep);
                            appendStringInfoString(buf, b"NULL\0".as_ptr() as _);
                            sep = b", \0".as_ptr() as _;
                        }
                        i += 1;
                    }
                    ReleaseTupleDesc(tupdesc);
                }
                appendStringInfoChar(buf, b')' as _);
                if (*rowexpr).row_format == COERCE_EXPLICIT_CAST {
                    appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                        format_type_with_typemod((*rowexpr).row_typeid, -1)).to_string_lossy());
                }
            }

            T_RowCompareExpr => {
                let rcexpr = node as *mut RowCompareExpr;
                /*
                 * SQL99 allows "ROW" to be omitted when there is more than
                 * one column, but for simplicity we always print it.  Within
                 * a ROW expression, whole-row Vars need special treatment, so
                 * use get_rule_list_toplevel.
                 */
                appendStringInfoString(buf, b"(ROW(\0".as_ptr() as _);
                get_rule_list_toplevel((*rcexpr).largs, context, true);

                /*
                 * We assume that the name of the first-column operator will
                 * do for all the rest too.  This is definitely open to
                 * failure, eg if some but not all operators were renamed
                 * since the construct was parsed, but there seems no way to
                 * be perfect.
                 */
                appendStringInfo!(buf, ") {} ROW(",
                    ::std::ffi::CStr::from_ptr(generate_operator_name(
                        linitial_oid!((*rcexpr).opnos),
                        exprType(linitial!((*rcexpr).largs) as *mut Node),
                        exprType(linitial!((*rcexpr).rargs) as *mut Node))).to_string_lossy());
                get_rule_list_toplevel((*rcexpr).rargs, context, true);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
            }

            T_CoalesceExpr => {
                let coalesceexpr = node as *mut CoalesceExpr;
                appendStringInfoString(buf, b"COALESCE(\0".as_ptr() as _);
                get_rule_expr((*coalesceexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_MinMaxExpr => {
                let minmaxexpr = node as *mut MinMaxExpr;
                match (*minmaxexpr).op {
                    IS_GREATEST => { appendStringInfoString(buf, b"GREATEST(\0".as_ptr() as _); }
                    IS_LEAST    => { appendStringInfoString(buf, b"LEAST(\0".as_ptr() as _); }
                    _ => {}
                }
                get_rule_expr((*minmaxexpr).args as *mut Node, context, true);
                appendStringInfoChar(buf, b')' as _);
            }

            T_SQLValueFunction => {
                let svf = node as *mut SQLValueFunction;
                /*
                 * Note: this code knows that typmod for time, timestamp, and
                 * timestamptz just prints as integer.
                 */
                match (*svf).op {
                    SVFOP_CURRENT_DATE => {
                        appendStringInfoString(buf, b"CURRENT_DATE\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIME => {
                        appendStringInfoString(buf, b"CURRENT_TIME\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIME_N => {
                        appendStringInfo!(buf, "CURRENT_TIME({})", (*svf).typmod);
                    }
                    SVFOP_CURRENT_TIMESTAMP => {
                        appendStringInfoString(buf, b"CURRENT_TIMESTAMP\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_TIMESTAMP_N => {
                        appendStringInfo!(buf, "CURRENT_TIMESTAMP({})", (*svf).typmod);
                    }
                    SVFOP_LOCALTIME => {
                        appendStringInfoString(buf, b"LOCALTIME\0".as_ptr() as _);
                    }
                    SVFOP_LOCALTIME_N => {
                        appendStringInfo!(buf, "LOCALTIME({})", (*svf).typmod);
                    }
                    SVFOP_LOCALTIMESTAMP => {
                        appendStringInfoString(buf, b"LOCALTIMESTAMP\0".as_ptr() as _);
                    }
                    SVFOP_LOCALTIMESTAMP_N => {
                        appendStringInfo!(buf, "LOCALTIMESTAMP({})", (*svf).typmod);
                    }
                    SVFOP_CURRENT_ROLE => {
                        appendStringInfoString(buf, b"CURRENT_ROLE\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_USER => {
                        appendStringInfoString(buf, b"CURRENT_USER\0".as_ptr() as _);
                    }
                    SVFOP_USER => {
                        appendStringInfoString(buf, b"USER\0".as_ptr() as _);
                    }
                    SVFOP_SESSION_USER => {
                        appendStringInfoString(buf, b"SESSION_USER\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_CATALOG => {
                        appendStringInfoString(buf, b"CURRENT_CATALOG\0".as_ptr() as _);
                    }
                    SVFOP_CURRENT_SCHEMA => {
                        appendStringInfoString(buf, b"CURRENT_SCHEMA\0".as_ptr() as _);
                    }
                    _ => {}
                }
            }

            T_XmlExpr => {
                let xexpr = node as *mut XmlExpr;
                let mut needcomma = false;

                match (*xexpr).op {
                    IS_XMLCONCAT   => { appendStringInfoString(buf, b"XMLCONCAT(\0".as_ptr() as _); }
                    IS_XMLELEMENT  => { appendStringInfoString(buf, b"XMLELEMENT(\0".as_ptr() as _); }
                    IS_XMLFOREST   => { appendStringInfoString(buf, b"XMLFOREST(\0".as_ptr() as _); }
                    IS_XMLPARSE    => { appendStringInfoString(buf, b"XMLPARSE(\0".as_ptr() as _); }
                    IS_XMLPI       => { appendStringInfoString(buf, b"XMLPI(\0".as_ptr() as _); }
                    IS_XMLROOT     => { appendStringInfoString(buf, b"XMLROOT(\0".as_ptr() as _); }
                    IS_XMLSERIALIZE => { appendStringInfoString(buf, b"XMLSERIALIZE(\0".as_ptr() as _); }
                    IS_DOCUMENT    => { /* nothing */ }
                    _ => {}
                }
                if (*xexpr).op == IS_XMLPARSE || (*xexpr).op == IS_XMLSERIALIZE {
                    if (*xexpr).xmloption == XMLOPTION_DOCUMENT {
                        appendStringInfoString(buf, b"DOCUMENT \0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b"CONTENT \0".as_ptr() as _);
                    }
                }
                if !(*xexpr).name.is_null() {
                    appendStringInfo!(buf, "NAME {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier(
                            map_xml_name_to_sql_identifier((*xexpr).name))).to_string_lossy());
                    needcomma = true;
                }
                if !(*xexpr).named_args.is_null() {
                    if (*xexpr).op != IS_XMLFOREST {
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        appendStringInfoString(buf, b"XMLATTRIBUTES(\0".as_ptr() as _);
                        needcomma = false;
                    }
                    let mut arg = list_head((*xexpr).named_args);
                    let mut narg = list_head((*xexpr).arg_names);
                    while !arg.is_null() {
                        let e = crate::current_cell!(arg) as *mut Node;
                        let argname = strVal!(crate::current_cell!(narg) as *mut Node);
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        get_rule_expr(e, context, true);
                        appendStringInfo!(buf, " AS {}",
                            ::std::ffi::CStr::from_ptr(quote_identifier(
                                map_xml_name_to_sql_identifier(argname))).to_string_lossy());
                        needcomma = true;
                        arg = lnext!((*xexpr).named_args, arg);
                        narg = lnext!((*xexpr).arg_names, narg);
                    }
                    if (*xexpr).op != IS_XMLFOREST {
                        appendStringInfoChar(buf, b')' as _);
                    }
                }
                if !(*xexpr).args.is_null() {
                    if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                    match (*xexpr).op {
                        IS_XMLCONCAT | IS_XMLELEMENT | IS_XMLFOREST | IS_XMLPI | IS_XMLSERIALIZE => {
                            /* no extra decoration needed */
                            get_rule_expr((*xexpr).args as *mut Node, context, true);
                        }
                        IS_XMLPARSE => {
                            assert!(list_length((*xexpr).args) == 2);
                            get_rule_expr(linitial!((*xexpr).args) as *mut Node, context, true);
                            let con = lsecond!((*xexpr).args) as *mut Const;
                            assert!(!(*con).constisnull);
                            if DatumGetBool((*con).constvalue) {
                                appendStringInfoString(buf, b" PRESERVE WHITESPACE\0".as_ptr() as _);
                            } else {
                                appendStringInfoString(buf, b" STRIP WHITESPACE\0".as_ptr() as _);
                            }
                        }
                        IS_XMLROOT => {
                            assert!(list_length((*xexpr).args) == 3);
                            get_rule_expr(linitial!((*xexpr).args) as *mut Node, context, true);
                            appendStringInfoString(buf, b", VERSION \0".as_ptr() as _);
                            let con = lsecond!((*xexpr).args) as *mut Const;
                            if IsA!(con as *mut Node, Const) && (*con).constisnull {
                                appendStringInfoString(buf, b"NO VALUE\0".as_ptr() as _);
                            } else {
                                get_rule_expr(con as *mut Node, context, false);
                            }
                            let con3 = lthird!((*xexpr).args) as *mut Const;
                            if !(*con3).constisnull {
                                match DatumGetInt32((*con3).constvalue) {
                                    XML_STANDALONE_YES => {
                                        appendStringInfoString(buf, b", STANDALONE YES\0".as_ptr() as _);
                                    }
                                    XML_STANDALONE_NO => {
                                        appendStringInfoString(buf, b", STANDALONE NO\0".as_ptr() as _);
                                    }
                                    XML_STANDALONE_NO_VALUE => {
                                        appendStringInfoString(buf, b", STANDALONE NO VALUE\0".as_ptr() as _);
                                    }
                                    _ => {}
                                }
                            }
                            /* suppress STANDALONE NO VALUE */
                        }
                        IS_DOCUMENT => {
                            get_rule_expr_paren((*xexpr).args as *mut Node, context, false, node);
                        }
                        _ => {}
                    }
                }
                if (*xexpr).op == IS_XMLSERIALIZE {
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(
                            format_type_with_typemod((*xexpr).r#type, (*xexpr).typmod)).to_string_lossy());
                    if (*xexpr).indent {
                        appendStringInfoString(buf, b" INDENT\0".as_ptr() as _);
                    } else {
                        appendStringInfoString(buf, b" NO INDENT\0".as_ptr() as _);
                    }
                }
                if (*xexpr).op == IS_DOCUMENT {
                    appendStringInfoString(buf, b" IS DOCUMENT\0".as_ptr() as _);
                } else {
                    appendStringInfoChar(buf, b')' as _);
                }
            }

            T_NullTest => {
                let ntest = node as *mut NullTest;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren((*ntest).arg as *mut Node, context, true, node);

                /*
                 * For scalar inputs, we prefer to print as IS [NOT] NULL,
                 * which is shorter and traditional.  If it's a rowtype input
                 * but we're applying a scalar test, must print IS [NOT]
                 * DISTINCT FROM NULL to be semantically correct.
                 */
                if (*ntest).argisrow || !type_is_rowtype(exprType((*ntest).arg as *mut Node)) {
                    match (*ntest).nulltesttype {
                        IS_NULL     => { appendStringInfoString(buf, b" IS NULL\0".as_ptr() as _); }
                        IS_NOT_NULL => { appendStringInfoString(buf, b" IS NOT NULL\0".as_ptr() as _); }
                        _ => { elog!(ERROR, "unrecognized nulltesttype: {}", (*ntest).nulltesttype as i32); }
                    }
                } else {
                    match (*ntest).nulltesttype {
                        IS_NULL     => { appendStringInfoString(buf, b" IS NOT DISTINCT FROM NULL\0".as_ptr() as _); }
                        IS_NOT_NULL => { appendStringInfoString(buf, b" IS DISTINCT FROM NULL\0".as_ptr() as _); }
                        _ => { elog!(ERROR, "unrecognized nulltesttype: {}", (*ntest).nulltesttype as i32); }
                    }
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_BooleanTest => {
                let btest = node as *mut BooleanTest;
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr_paren((*btest).arg as *mut Node, context, false, node);
                match (*btest).booltesttype {
                    IS_TRUE        => { appendStringInfoString(buf, b" IS TRUE\0".as_ptr() as _); }
                    IS_NOT_TRUE    => { appendStringInfoString(buf, b" IS NOT TRUE\0".as_ptr() as _); }
                    IS_FALSE       => { appendStringInfoString(buf, b" IS FALSE\0".as_ptr() as _); }
                    IS_NOT_FALSE   => { appendStringInfoString(buf, b" IS NOT FALSE\0".as_ptr() as _); }
                    IS_UNKNOWN     => { appendStringInfoString(buf, b" IS UNKNOWN\0".as_ptr() as _); }
                    IS_NOT_UNKNOWN => { appendStringInfoString(buf, b" IS NOT UNKNOWN\0".as_ptr() as _); }
                    _ => { elog!(ERROR, "unrecognized booltesttype: {}", (*btest).booltesttype as i32); }
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            }

            T_CoerceToDomain => {
                let ctest = node as *mut CoerceToDomain;
                let arg = (*ctest).arg as *mut Node;
                if (*ctest).coercionformat == COERCE_IMPLICIT_CAST && !showimplicit {
                    /* don't show the implicit cast */
                    get_rule_expr(arg, context, false);
                } else {
                    get_coercion_expr(arg, context,
                        (*ctest).resulttype, (*ctest).resulttypmod, node);
                }
            }

            T_CoerceToDomainValue => {
                appendStringInfoString(buf, b"VALUE\0".as_ptr() as _);
            }

            T_SetToDefault => {
                appendStringInfoString(buf, b"DEFAULT\0".as_ptr() as _);
            }

            T_CurrentOfExpr => {
                let cexpr = node as *mut CurrentOfExpr;
                if !(*cexpr).cursor_name.is_null() {
                    appendStringInfo!(buf, "CURRENT OF {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier((*cexpr).cursor_name)).to_string_lossy());
                } else {
                    appendStringInfo!(buf, "CURRENT OF ${}", (*cexpr).cursor_param);
                }
            }

            T_NextValueExpr => {
                let nvexpr = node as *mut NextValueExpr;
                /*
                 * This isn't exactly nextval(), but that seems close enough
                 * for EXPLAIN's purposes.
                 */
                appendStringInfoString(buf, b"nextval(\0".as_ptr() as _);
                simple_quote_literal(buf,
                    generate_relation_name((*nvexpr).seqid, std::ptr::null_mut()));
                appendStringInfoChar(buf, b')' as _);
            }

            T_InferenceElem => {
                let iexpr = node as *mut InferenceElem;
                /*
                 * InferenceElem can only refer to target relation, so a
                 * prefix is not useful, and indeed would cause parse errors.
                 */
                let save_varprefix = (*context).varprefix;
                (*context).varprefix = false;

                /*
                 * Parenthesize the element unless it's a simple Var or a bare
                 * function call.  Follows pg_get_indexdef_worker().
                 */
                let mut need_parens = !IsA!((*iexpr).expr, Var);
                if IsA!((*iexpr).expr, FuncExpr) &&
                    (*((*iexpr).expr as *mut FuncExpr)).funcformat == COERCE_EXPLICIT_CALL
                {
                    need_parens = false;
                }

                if need_parens { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr((*iexpr).expr as *mut Node, context, false);
                if need_parens { appendStringInfoChar(buf, b')' as _); }

                (*context).varprefix = save_varprefix;

                if (*iexpr).infercollid != 0 {
                    appendStringInfo!(buf, " COLLATE {}",
                        ::std::ffi::CStr::from_ptr(generate_collation_name((*iexpr).infercollid)).to_string_lossy());
                }

                /* Add the operator class name, if not default */
                if (*iexpr).inferopclass != 0 {
                    let inferopclass = (*iexpr).inferopclass;
                    let inferopcinputtype = get_opclass_input_type((*iexpr).inferopclass);
                    get_opclass_name(inferopclass, inferopcinputtype, buf);
                }
            }

            T_ReturningExpr => {
                let ret_expr = node as *mut ReturningExpr;
                /*
                 * We cannot see a ReturningExpr in rule deparsing, only while
                 * EXPLAINing a query plan (ReturningExpr nodes are only ever
                 * added during query rewriting). Just display the expression
                 * returned (an expanded view column).
                 */
                get_rule_expr((*ret_expr).retexpr as *mut Node, context, showimplicit);
            }

            T_PartitionBoundSpec => {
                let spec = node as *mut PartitionBoundSpec;

                if (*spec).is_default {
                    appendStringInfoString(buf, b"DEFAULT\0".as_ptr() as _);
                } else {
                    match (*spec).strategy as u8 {
                        PARTITION_STRATEGY_HASH => {
                            assert!((*spec).modulus > 0 && (*spec).remainder >= 0);
                            assert!((*spec).modulus > (*spec).remainder);
                            appendStringInfoString(buf, b"FOR VALUES\0".as_ptr() as _);
                            appendStringInfo!(buf, " WITH (modulus {}, remainder {})",
                                (*spec).modulus, (*spec).remainder);
                        }
                        PARTITION_STRATEGY_LIST => {
                            assert!(!(*spec).listdatums.is_null());
                            appendStringInfoString(buf, b"FOR VALUES IN (\0".as_ptr() as _);
                            let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                            let mut cell = list_head((*spec).listdatums);
                            while !cell.is_null() {
                                let val = crate::current_cell!(cell) as *mut Const;
                                appendStringInfoString(buf, sep);
                                get_const_expr(val, context, -1);
                                sep = b", \0".as_ptr() as _;
                                cell = lnext!((*spec).listdatums, cell);
                            }
                            appendStringInfoChar(buf, b')' as _);
                        }
                        PARTITION_STRATEGY_RANGE => {
                            assert!(!(*spec).lowerdatums.is_null() &&
                                !(*spec).upperdatums.is_null() &&
                                list_length((*spec).lowerdatums) == list_length((*spec).upperdatums));
                            appendStringInfo!(buf, "FOR VALUES FROM {} TO {}",
                                ::std::ffi::CStr::from_ptr(get_range_partbound_string((*spec).lowerdatums)).to_string_lossy(),
                                ::std::ffi::CStr::from_ptr(get_range_partbound_string((*spec).upperdatums)).to_string_lossy());
                        }
                        _ => {
                            elog!(ERROR, "unrecognized partition strategy: {}", (*spec).strategy as i32);
                        }
                    }
                }
            }

            T_JsonValueExpr => {
                let jve = node as *mut JsonValueExpr;
                get_rule_expr((*jve).raw_expr as *mut Node, context, false);
                get_json_format((*jve).format, (*context).buf);
            }

            T_JsonConstructorExpr => {
                get_json_constructor(node as *mut JsonConstructorExpr, context, false);
            }

            T_JsonIsPredicate => {
                let pred = node as *mut JsonIsPredicate;
                if !PRETTY_PAREN!(context) { appendStringInfoChar((*context).buf, b'(' as _); }
                get_rule_expr_paren((*pred).expr, context, true, node);
                appendStringInfoString((*context).buf, b" IS JSON\0".as_ptr() as _);
                /* TODO: handle FORMAT clause */
                match (*pred).item_type {
                    JS_TYPE_SCALAR => { appendStringInfoString((*context).buf, b" SCALAR\0".as_ptr() as _); }
                    JS_TYPE_ARRAY  => { appendStringInfoString((*context).buf, b" ARRAY\0".as_ptr() as _); }
                    JS_TYPE_OBJECT => { appendStringInfoString((*context).buf, b" OBJECT\0".as_ptr() as _); }
                    _ => {}
                }
                if (*pred).unique_keys {
                    appendStringInfoString((*context).buf, b" WITH UNIQUE KEYS\0".as_ptr() as _);
                }
                if !PRETTY_PAREN!(context) { appendStringInfoChar((*context).buf, b')' as _); }
            }

            T_JsonExpr => {
                let jexpr = node as *mut JsonExpr;
                match (*jexpr).op {
                    JSON_EXISTS_OP => { appendStringInfoString(buf, b"JSON_EXISTS(\0".as_ptr() as _); }
                    JSON_QUERY_OP  => { appendStringInfoString(buf, b"JSON_QUERY(\0".as_ptr() as _); }
                    JSON_VALUE_OP  => { appendStringInfoString(buf, b"JSON_VALUE(\0".as_ptr() as _); }
                    _ => { elog!(ERROR, "unrecognized JsonExpr op: {}", (*jexpr).op as i32); }
                }

                get_rule_expr((*jexpr).formatted_expr, context, showimplicit);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_json_path_spec((*jexpr).path_spec, context, showimplicit);

                if !(*jexpr).passing_values.is_null() {
                    let mut needcomma = false;
                    appendStringInfoString(buf, b" PASSING \0".as_ptr() as _);
                    let mut lc1 = list_head((*jexpr).passing_names);
                    let mut lc2 = list_head((*jexpr).passing_values);
                    while !lc1.is_null() {
                        if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                        needcomma = true;
                        get_rule_expr(crate::current_cell!(lc2) as *mut Node, context, showimplicit);
                        appendStringInfo!(buf, " AS {}",
                            ::std::ffi::CStr::from_ptr(quote_identifier(
                                (*(crate::current_cell!(lc1) as *mut String)).sval)).to_string_lossy());
                        lc1 = lnext!((*jexpr).passing_names, lc1);
                        lc2 = lnext!((*jexpr).passing_values, lc2);
                    }
                }

                if (*jexpr).op != JSON_EXISTS_OP ||
                    (*(*jexpr).returning).typid != BOOLOID
                {
                    get_json_returning((*jexpr).returning, (*context).buf,
                        (*jexpr).op == JSON_QUERY_OP);
                }

                get_json_expr_options(jexpr, context,
                    if (*jexpr).op != JSON_EXISTS_OP { JSON_BEHAVIOR_NULL } else { JSON_BEHAVIOR_FALSE });

                appendStringInfoChar(buf, b')' as _);
            }

            T_List => {
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                let mut l = list_head(node as *mut List);
                while !l.is_null() {
                    appendStringInfoString(buf, sep);
                    get_rule_expr(crate::current_cell!(l) as *mut Node, context, showimplicit);
                    sep = b", \0".as_ptr() as _;
                    l = lnext!(node as *mut List, l);
                }
            }

            T_TableFunc => {
                get_tablefunc(node as *mut TableFunc, context, showimplicit);
            }

            _ => {
                elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as i32);
            }
        }
    }
}

/*
 * get_rule_expr_toplevel        - Parse back a toplevel expression
 *
 * Same as get_rule_expr(), except that if the expr is just a Var, we pass
 * istoplevel = true not false to get_variable().  This causes whole-row Vars
 * to get printed with decoration that will prevent expansion of "*".
 * We need to use this in contexts such as ROW() and VALUES(), where the
 * parser would expand "foo.*" appearing at top level.  (In principle we'd
 * use this in get_target_list() too, but that has additional worries about
 * whether to print AS, so it needs to invoke get_variable() directly anyway.)
 */
fn get_rule_expr_toplevel(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if !node.is_null() && IsA!(node, Var) {
            get_variable(node as *mut Var, 0, true, context);
        } else {
            get_rule_expr(node, context, showimplicit);
        }
    }
}

/*
 * get_rule_list_toplevel        - Parse back a list of toplevel expressions
 *
 * Apply get_rule_expr_toplevel() to each element of a List.
 *
 * This adds commas between the expressions, but caller is responsible
 * for printing surrounding decoration.
 */
fn get_rule_list_toplevel(lst: *mut List, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
        let mut lc = list_head(lst);
        while !lc.is_null() {
            let e = crate::current_cell!(lc) as *mut Node;
            appendStringInfoString((*context).buf, sep);
            get_rule_expr_toplevel(e, context, showimplicit);
            sep = b", \0".as_ptr() as _;
            lc = lnext!(lst, lc);
        }
    }
}

/*
 * get_rule_expr_funccall        - Parse back a function-call expression
 *
 * Same as get_rule_expr(), except that we guarantee that the output will
 * look like a function call, or like one of the things the grammar treats as
 * equivalent to a function call (see the func_expr_windowless production).
 * This is needed in places where the grammar uses func_expr_windowless and
 * you can't substitute a parenthesized a_expr.  If what we have isn't going
 * to look like a function call, wrap it in a dummy CAST() expression, which
 * will satisfy the grammar --- and, indeed, is likely what the user wrote to
 * produce such a thing.
 */
fn get_rule_expr_funccall(node: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if looks_like_function(node) {
            get_rule_expr(node, context, showimplicit);
        } else {
            let buf = (*context).buf;
            appendStringInfoString(buf, b"CAST(\0".as_ptr() as _);
            /* no point in showing any top-level implicit cast */
            get_rule_expr(node, context, false);
            appendStringInfo!(buf, " AS {})",
                ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod(exprType(node), exprTypmod(node))).to_string_lossy());
        }
    }
}

/*
 * Helper function to identify node types that satisfy func_expr_windowless.
 * If in doubt, "false" is always a safe answer.
 */
fn looks_like_function(node: *mut Node) -> bool {
    unsafe {
        if node.is_null() {
            return false; /* probably shouldn't happen */
        }
        match nodeTag(node) {
            T_FuncExpr => {
                /* OK, unless it's going to deparse as a cast */
                (*(node as *mut FuncExpr)).funcformat == COERCE_EXPLICIT_CALL ||
                (*(node as *mut FuncExpr)).funcformat == COERCE_SQL_SYNTAX
            }
            T_NullIfExpr | T_CoalesceExpr | T_MinMaxExpr |
            T_SQLValueFunction | T_XmlExpr | T_JsonExpr => {
                /* these are all accepted by func_expr_common_subexpr */
                true
            }
            _ => false,
        }
    }
}

/*
 * get_oper_expr           - Parse back an OpExpr node
 */
fn get_oper_expr(expr: *mut OpExpr, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let opno = (*expr).opno;
        let args = (*expr).args;

        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
        if list_length(args) == 2 {
            /* binary operator */
            let arg1 = linitial!(args) as *mut Node;
            let arg2 = lsecond!(args) as *mut Node;
            get_rule_expr_paren(arg1, context, true, expr as *mut Node);
            appendStringInfo!(buf, " {} ",
                ::std::ffi::CStr::from_ptr(generate_operator_name(opno,
                    exprType(arg1), exprType(arg2))).to_string_lossy());
            get_rule_expr_paren(arg2, context, true, expr as *mut Node);
        } else {
            /* prefix operator */
            let arg = linitial!(args) as *mut Node;
            appendStringInfo!(buf, "{} ",
                ::std::ffi::CStr::from_ptr(generate_operator_name(opno,
                    InvalidOid, exprType(arg))).to_string_lossy());
            get_rule_expr_paren(arg, context, true, expr as *mut Node);
        }
        if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
    }
}

/*
 * get_func_expr           - Parse back a FuncExpr node
 */
fn get_func_expr(expr: *mut FuncExpr, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;
        let funcoid = (*expr).funcid;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32 = 0;
        let mut argnames: *mut List = std::ptr::null_mut();
        let mut use_variadic: bool = false;

        /*
         * If the function call came from an implicit coercion, then just show the
         * first argument --- unless caller wants to see implicit coercions.
         */
        if (*expr).funcformat == COERCE_IMPLICIT_CAST && !showimplicit {
            get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context,
                false, expr as *mut Node);
            return;
        }

        /*
         * If the function call came from a cast, then show the first argument
         * plus an explicit cast operation.
         */
        if (*expr).funcformat == COERCE_EXPLICIT_CAST ||
           (*expr).funcformat == COERCE_IMPLICIT_CAST
        {
            let arg = linitial!((*expr).args) as *mut Node;
            let rettype = (*expr).funcresulttype;
            let mut coerced_typmod: i32 = 0;

            /* Get the typmod if this is a length-coercion function */
            exprIsLengthCoercion(expr as *mut Node, &mut coerced_typmod);

            get_coercion_expr(arg, context, rettype, coerced_typmod, expr as *mut Node);
            return;
        }

        /*
         * If the function was called using one of the SQL spec's random special
         * syntaxes, try to reproduce that.  If we don't recognize the function,
         * fall through.
         */
        if (*expr).funcformat == COERCE_SQL_SYNTAX {
            if get_func_sql_syntax(expr, context) {
                return;
            }
        }

        /*
         * Normal function: display as proname(args).  First we need to extract
         * the argument datatypes.
         */
        if list_length((*expr).args) > FUNC_MAX_ARGS as i32 {
            ereport!(ERROR,
                errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                /* C also: */ errmsg("too many arguments"));
        }
        nargs = 0;
        argnames = std::ptr::null_mut();
        let mut l = list_head((*expr).args);
        while !l.is_null() {
            let arg = crate::current_cell!(l) as *mut Node;
            if IsA!(arg, NamedArgExpr) {
                argnames = lappend(argnames, (*(arg as *mut NamedArgExpr)).name as *mut _);
            }
            argtypes[nargs as usize] = exprType(arg);
            nargs += 1;
            l = lnext!((*expr).args, l);
        }

        appendStringInfo!(buf, "{}(",
            ::std::ffi::CStr::from_ptr(generate_function_name(funcoid, nargs,
                argnames, argtypes.as_mut_ptr(),
                (*expr).funcvariadic,
                &mut use_variadic,
                (*context).inGroupBy)).to_string_lossy());
        nargs = 0;
        let mut l = list_head((*expr).args);
        while !l.is_null() {
            if nargs > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            if use_variadic && lnext!((*expr).args, l).is_null() {
                appendStringInfoString(buf, b"VARIADIC \0".as_ptr() as _);
            }
            get_rule_expr(crate::current_cell!(l) as *mut Node, context, true);
            nargs += 1;
            l = lnext!((*expr).args, l);
        }
        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_agg_expr            - Parse back an Aggref node
 */
fn get_agg_expr(aggref: *mut Aggref, context: *mut deparse_context, original_aggref: *mut Aggref) {
    get_agg_expr_helper(aggref, context, original_aggref,
        std::ptr::null(), std::ptr::null(), false);
}

/*
 * get_agg_expr_helper     - subroutine for get_agg_expr and
 *                          get_json_agg_constructor
 */
fn get_agg_expr_helper(
    aggref: *mut Aggref,
    context: *mut deparse_context,
    original_aggref: *mut Aggref,
    funcname: *const ::std::os::raw::c_char,
    options: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32;
        let mut use_variadic = false;

        /*
         * For a combining aggregate, we look up and deparse the corresponding
         * partial aggregate instead.  This is necessary because our input
         * argument list has been replaced; the new argument list always has just
         * one element, which will point to a partial Aggref that supplies us with
         * transition states to combine.
         */
        if DO_AGGSPLIT_COMBINE!((*aggref).aggsplit) {
            assert!(list_length((*aggref).args) == 1);
            let tle = linitial!((*aggref).args) as *mut TargetEntry;
            resolve_special_varno((*tle).expr as *mut Node, context,
                Some(get_agg_combine_expr), original_aggref as *mut _);
            return;
        }

        /*
         * Mark as PARTIAL, if appropriate.  We look to the original aggref so as
         * to avoid printing this when recursing from the code just above.
         */
        if DO_AGGSPLIT_SKIPFINAL!((*original_aggref).aggsplit) {
            appendStringInfoString(buf, b"PARTIAL \0".as_ptr() as _);
        }

        /* Extract the argument types as seen by the parser */
        nargs = get_aggregate_argtypes(aggref, argtypes.as_mut_ptr());

        let funcname_ptr = if !funcname.is_null() {
            funcname
        } else {
            generate_function_name((*aggref).aggfnoid, nargs, std::ptr::null_mut(),
                argtypes.as_mut_ptr(), (*aggref).aggvariadic,
                &mut use_variadic, (*context).inGroupBy)
        };

        /* Print the aggregate name, schema-qualified if needed */
        appendStringInfo!(buf, "{}({}",
            ::std::ffi::CStr::from_ptr(funcname_ptr).to_string_lossy(),
            if !(*aggref).aggdistinct.is_null() { "DISTINCT " } else { "" });

        if AGGKIND_IS_ORDERED_SET!((*aggref).aggkind) {
            /*
             * Ordered-set aggregates do not use "*" syntax.  Also, we needn't
             * worry about inserting VARIADIC.  So we can just dump the direct
             * args as-is.
             */
            assert!(!(*aggref).aggvariadic);
            get_rule_expr((*aggref).aggdirectargs as *mut Node, context, true);
            assert!(!(*aggref).aggorder.is_null());
            appendStringInfoString(buf, b") WITHIN GROUP (ORDER BY \0".as_ptr() as _);
            get_rule_orderby((*aggref).aggorder, (*aggref).args, false, context);
        } else {
            /* aggstar can be set only in zero-argument aggregates */
            if (*aggref).aggstar {
                appendStringInfoChar(buf, b'*' as _);
            } else {
                let mut i: i32 = 0;
                let mut l = list_head((*aggref).args);
                while !l.is_null() {
                    let tle = crate::current_cell!(l) as *mut TargetEntry;
                    let arg = (*tle).expr as *mut Node;
                    assert!(!IsA!(arg, NamedArgExpr));
                    if (*tle).resjunk {
                        l = lnext!((*aggref).args, l);
                        continue;
                    }
                    if i > 0 {
                        if is_json_objectagg {
                            /*
                             * the ABSENT ON NULL and WITH UNIQUE args are printed
                             * separately, so ignore them here
                             */
                            if i > 2 { break; }
                            appendStringInfoString(buf, b" : \0".as_ptr() as _);
                        } else {
                            appendStringInfoString(buf, b", \0".as_ptr() as _);
                        }
                    }
                    if use_variadic && i == nargs - 1 {
                        appendStringInfoString(buf, b"VARIADIC \0".as_ptr() as _);
                    }
                    get_rule_expr(arg, context, true);
                    i += 1;
                    l = lnext!((*aggref).args, l);
                }
            }

            if !(*aggref).aggorder.is_null() {
                appendStringInfoString(buf, b" ORDER BY \0".as_ptr() as _);
                get_rule_orderby((*aggref).aggorder, (*aggref).args, false, context);
            }
        }

        if !options.is_null() {
            appendStringInfoString(buf, options);
        }

        if !(*aggref).aggfilter.is_null() {
            appendStringInfoString(buf, b") FILTER (WHERE \0".as_ptr() as _);
            get_rule_expr((*aggref).aggfilter as *mut Node, context, false);
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * This is a helper function for get_agg_expr().  It's used when we deparse
 * a combining Aggref; resolve_special_varno locates the corresponding partial
 * Aggref and then calls this.
 */
unsafe extern "C" fn get_agg_combine_expr(
    node: *mut Node,
    context: *mut deparse_context,
    callback_arg: *mut ::std::os::raw::c_void,
) {
    let original_aggref = callback_arg as *mut Aggref;

    if !IsA!(node, Aggref) {
        elog!(ERROR, "combining Aggref does not point to an Aggref");
    }

    let aggref = node as *mut Aggref;
    get_agg_expr(aggref, context, original_aggref);
}

/*
 * get_windowfunc_expr - Parse back a WindowFunc node
 */
fn get_windowfunc_expr(wfunc: *mut WindowFunc, context: *mut deparse_context) {
    get_windowfunc_expr_helper(wfunc, context,
        std::ptr::null(), std::ptr::null(), false);
}

/*
 * get_windowfunc_expr_helper    - subroutine for get_windowfunc_expr and
 *                                get_json_agg_constructor
 */
fn get_windowfunc_expr_helper(
    wfunc: *mut WindowFunc,
    context: *mut deparse_context,
    funcname: *const ::std::os::raw::c_char,
    options: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut argtypes: [Oid; FUNC_MAX_ARGS] = [0; FUNC_MAX_ARGS];
        let mut nargs: i32 = 0;
        let mut argnames: *mut List = std::ptr::null_mut();

        if list_length((*wfunc).args) > FUNC_MAX_ARGS as i32 {
            ereport!(ERROR,
                errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                /* C also: */ errmsg("too many arguments"));
        }
        let mut l = list_head((*wfunc).args);
        while !l.is_null() {
            let arg = crate::current_cell!(l) as *mut Node;
            if IsA!(arg, NamedArgExpr) {
                argnames = lappend(argnames, (*(arg as *mut NamedArgExpr)).name as *mut _);
            }
            argtypes[nargs as usize] = exprType(arg);
            nargs += 1;
            l = lnext!((*wfunc).args, l);
        }

        let funcname_ptr = if !funcname.is_null() {
            funcname
        } else {
            generate_function_name((*wfunc).winfnoid, nargs, argnames,
                argtypes.as_mut_ptr(), false, std::ptr::null_mut(),
                (*context).inGroupBy)
        };

        appendStringInfo!(buf, "{}(", ::std::ffi::CStr::from_ptr(funcname_ptr).to_string_lossy());

        /* winstar can be set only in zero-argument aggregates */
        if (*wfunc).winstar {
            appendStringInfoChar(buf, b'*' as _);
        } else {
            if is_json_objectagg {
                get_rule_expr(linitial!((*wfunc).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" : \0".as_ptr() as _);
                get_rule_expr(lsecond!((*wfunc).args) as *mut Node, context, false);
            } else {
                get_rule_expr((*wfunc).args as *mut Node, context, true);
            }
        }

        if !options.is_null() {
            appendStringInfoString(buf, options);
        }

        if !(*wfunc).aggfilter.is_null() {
            appendStringInfoString(buf, b") FILTER (WHERE \0".as_ptr() as _);
            get_rule_expr((*wfunc).aggfilter as *mut Node, context, false);
        }

        appendStringInfoString(buf, b") OVER \0".as_ptr() as _);

        if !(*context).windowClause.is_null() {
            /* Query-decompilation case: search the windowClause list */
            let mut l = list_head((*context).windowClause);
            let mut found = false;
            while !l.is_null() {
                let wc = crate::current_cell!(l) as *mut WindowClause;
                if (*wc).winref == (*wfunc).winref {
                    if !(*wc).name.is_null() {
                        appendStringInfoString(buf, quote_identifier((*wc).name));
                    } else {
                        get_rule_windowspec(wc, (*context).targetList, context);
                    }
                    found = true;
                    break;
                }
                l = lnext!((*context).windowClause, l);
            }
            if !found {
                elog!(ERROR, "could not find window clause for winref {}", (*wfunc).winref);
            }
        } else {
            /*
             * In EXPLAIN, search the namespace stack for a matching WindowAgg
             * node (probably it's always the first entry), and print winname.
             */
            let mut l = list_head((*context).namespaces);
            let mut found = false;
            while !l.is_null() {
                let dpns = crate::current_cell!(l) as *mut deparse_namespace;
                if !(*dpns).plan.is_null() && IsA!((*dpns).plan as *mut Node, WindowAgg) {
                    let wagg = (*dpns).plan as *mut WindowAgg;
                    if (*wagg).winref == (*wfunc).winref {
                        appendStringInfoString(buf, quote_identifier((*wagg).winname));
                        found = true;
                        break;
                    }
                }
                l = lnext!((*context).namespaces, l);
            }
            if !found {
                elog!(ERROR, "could not find window clause for winref {}", (*wfunc).winref);
            }
        }
    }
}

/*
 * get_func_sql_syntax     - Parse back a SQL-syntax function call
 *
 * Returns true if we successfully deparsed, false if we did not
 * recognize the function.
 */
fn get_func_sql_syntax(expr: *mut FuncExpr, context: *mut deparse_context) -> bool {
    unsafe {
        let buf = (*context).buf;
        let funcoid = (*expr).funcid;

        match funcoid {
            F_TIMEZONE_INTERVAL_TIMESTAMP |
            F_TIMEZONE_INTERVAL_TIMESTAMPTZ |
            F_TIMEZONE_INTERVAL_TIMETZ |
            F_TIMEZONE_TEXT_TIMESTAMP |
            F_TIMEZONE_TEXT_TIMESTAMPTZ |
            F_TIMEZONE_TEXT_TIMETZ => {
                /* AT TIME ZONE ... note reversed argument order */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(lsecond!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" AT TIME ZONE \0".as_ptr() as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_TIMEZONE_TIMESTAMP | F_TIMEZONE_TIMESTAMPTZ | F_TIMEZONE_TIMETZ => {
                /* AT LOCAL */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" AT LOCAL)\0".as_ptr() as _);
                return true;
            }
            F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_INTERVAL |
            F_OVERLAPS_TIMESTAMPTZ_INTERVAL_TIMESTAMPTZ_TIMESTAMPTZ |
            F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_INTERVAL |
            F_OVERLAPS_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ_TIMESTAMPTZ |
            F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_INTERVAL |
            F_OVERLAPS_TIMESTAMP_INTERVAL_TIMESTAMP_TIMESTAMP |
            F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_INTERVAL |
            F_OVERLAPS_TIMESTAMP_TIMESTAMP_TIMESTAMP_TIMESTAMP |
            F_OVERLAPS_TIMETZ_TIMETZ_TIMETZ_TIMETZ |
            F_OVERLAPS_TIME_INTERVAL_TIME_INTERVAL |
            F_OVERLAPS_TIME_INTERVAL_TIME_TIME |
            F_OVERLAPS_TIME_TIME_TIME_INTERVAL |
            F_OVERLAPS_TIME_TIME_TIME_TIME => {
                /* (x1, x2) OVERLAPS (y1, y2) */
                appendStringInfoString(buf, b"((\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") OVERLAPS (\0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b", \0".as_ptr() as _);
                get_rule_expr(lfourth!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            F_EXTRACT_TEXT_DATE |
            F_EXTRACT_TEXT_TIME |
            F_EXTRACT_TEXT_TIMETZ |
            F_EXTRACT_TEXT_TIMESTAMP |
            F_EXTRACT_TEXT_TIMESTAMPTZ |
            F_EXTRACT_TEXT_INTERVAL => {
                /* EXTRACT (x FROM y) */
                appendStringInfoString(buf, b"EXTRACT(\0".as_ptr() as _);
                let con = linitial!((*expr).args) as *mut Const;
                assert!(IsA!(con as *mut Node, Const) &&
                    (*con).consttype == TEXTOID && !(*con).constisnull);
                appendStringInfoString(buf, TextDatumGetCString((*con).constvalue));
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_IS_NORMALIZED => {
                /* IS xxx NORMALIZED */
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr_paren(linitial!((*expr).args) as *mut Node, context, false, expr as *mut Node);
                appendStringInfoString(buf, b" IS\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    let con = lsecond!((*expr).args) as *mut Const;
                    assert!(IsA!(con as *mut Node, Const) &&
                        (*con).consttype == TEXTOID && !(*con).constisnull);
                    appendStringInfo!(buf, " {}",
                        ::std::ffi::CStr::from_ptr(TextDatumGetCString((*con).constvalue)).to_string_lossy());
                }
                appendStringInfoString(buf, b" NORMALIZED)\0".as_ptr() as _);
                return true;
            }
            F_PG_COLLATION_FOR => {
                /* COLLATION FOR */
                appendStringInfoString(buf, b"COLLATION FOR (\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_NORMALIZE => {
                /* NORMALIZE() */
                appendStringInfoString(buf, b"NORMALIZE(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 2 {
                    let con = lsecond!((*expr).args) as *mut Const;
                    assert!(IsA!(con as *mut Node, Const) &&
                        (*con).consttype == TEXTOID && !(*con).constisnull);
                    appendStringInfo!(buf, ", {}",
                        ::std::ffi::CStr::from_ptr(TextDatumGetCString((*con).constvalue)).to_string_lossy());
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_OVERLAY_BIT_BIT_INT4 | F_OVERLAY_BIT_BIT_INT4_INT4 |
            F_OVERLAY_BYTEA_BYTEA_INT4 | F_OVERLAY_BYTEA_BYTEA_INT4_INT4 |
            F_OVERLAY_TEXT_TEXT_INT4 | F_OVERLAY_TEXT_TEXT_INT4_INT4 => {
                /* OVERLAY() */
                appendStringInfoString(buf, b"OVERLAY(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" PLACING \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 4 {
                    appendStringInfoString(buf, b" FOR \0".as_ptr() as _);
                    get_rule_expr(lfourth!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_POSITION_BIT_BIT | F_POSITION_BYTEA_BYTEA | F_POSITION_TEXT_TEXT => {
                /* POSITION() ... extra parens since args are b_expr not a_expr */
                appendStringInfoString(buf, b"POSITION((\0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") IN (\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            F_SUBSTRING_BIT_INT4 | F_SUBSTRING_BIT_INT4_INT4 |
            F_SUBSTRING_BYTEA_INT4 | F_SUBSTRING_BYTEA_INT4_INT4 |
            F_SUBSTRING_TEXT_INT4 | F_SUBSTRING_TEXT_INT4_INT4 => {
                /* SUBSTRING FROM/FOR (i.e., integer-position variants) */
                appendStringInfoString(buf, b"SUBSTRING(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                if list_length((*expr).args) == 3 {
                    appendStringInfoString(buf, b" FOR \0".as_ptr() as _);
                    get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_SUBSTRING_TEXT_TEXT_TEXT => {
                /* SUBSTRING SIMILAR/ESCAPE */
                appendStringInfoString(buf, b"SUBSTRING(\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" SIMILAR \0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b" ESCAPE \0".as_ptr() as _);
                get_rule_expr(lthird!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_BTRIM_BYTEA_BYTEA | F_BTRIM_TEXT | F_BTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(BOTH\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_LTRIM_BYTEA_BYTEA | F_LTRIM_TEXT | F_LTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(LEADING\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_RTRIM_BYTEA_BYTEA | F_RTRIM_TEXT | F_RTRIM_TEXT_TEXT => {
                /* TRIM() */
                appendStringInfoString(buf, b"TRIM(TRAILING\0".as_ptr() as _);
                if list_length((*expr).args) == 2 {
                    appendStringInfoChar(buf, b' ' as _);
                    get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                }
                appendStringInfoString(buf, b" FROM \0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoChar(buf, b')' as _);
                return true;
            }
            F_SYSTEM_USER => {
                appendStringInfoString(buf, b"SYSTEM_USER\0".as_ptr() as _);
                return true;
            }
            F_XMLEXISTS => {
                /* XMLEXISTS ... extra parens because args are c_expr */
                appendStringInfoString(buf, b"XMLEXISTS((\0".as_ptr() as _);
                get_rule_expr(linitial!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b") PASSING (\0".as_ptr() as _);
                get_rule_expr(lsecond!((*expr).args) as *mut Node, context, false);
                appendStringInfoString(buf, b"))\0".as_ptr() as _);
                return true;
            }
            _ => {}
        }
        false
    }
}

/* ----------
 * get_coercion_expr
 *
 *  Make a string representation of a value coerced to a specific type
 * ----------
 */
fn get_coercion_expr(
    arg: *mut Node,
    context: *mut deparse_context,
    resulttype: Oid,
    resulttypmod: i32,
    parent_node: *mut Node,
) {
    unsafe {
        let buf = (*context).buf;

        /*
         * Since parse_coerce.c doesn't immediately collapse application of
         * length-coercion functions to constants, what we'll typically see in
         * such cases is a Const with typmod -1 and a length-coercion function
         * right above it.  Avoid generating redundant output. However, beware of
         * suppressing casts when the user actually wrote something like
         * 'foo'::text::char(3).
         *
         * Note: it might seem that we are missing the possibility of needing to
         * print a COLLATE clause for such a Const.  However, a Const could only
         * have nondefault collation in a post-constant-folding tree, in which the
         * length coercion would have been folded too.  See also the special
         * handling of CollateExpr in coerce_to_target_type(): any collation
         * marking will be above the coercion node, not below it.
         */
        if !arg.is_null() && IsA!(arg, Const) &&
            (*(arg as *mut Const)).consttype == resulttype &&
            (*(arg as *mut Const)).consttypmod == -1
        {
            /* Show the constant without normal ::typename decoration */
            get_const_expr(arg as *mut Const, context, -1);
        } else {
            if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
            get_rule_expr_paren(arg, context, false, parent_node);
            if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
        }

        /*
         * Never emit resulttype(arg) functional notation. A pg_proc entry could
         * take precedence, and a resulttype in pg_temp would require schema
         * qualification that format_type_with_typemod() would usually omit. We've
         * standardized on arg::resulttype, but CAST(arg AS resulttype) notation
         * would work fine.
         */
        appendStringInfo!(buf, "::{}",
            ::std::ffi::CStr::from_ptr(format_type_with_typemod(resulttype, resulttypmod)).to_string_lossy());
    }
}

/* ----------
 * get_const_expr
 *
 *  Make a string representation of a Const
 *
 * showtype can be -1 to never show "::typename" decoration, or +1 to always
 * show it, or 0 to show it only if the constant wouldn't be assumed to be
 * the right type by default.
 *
 * If the Const's collation isn't default for its type, show that too.
 * We mustn't do this when showtype is -1 (since that means the caller will
 * print "::typename", and we can't put a COLLATE clause in between).  It's
 * caller's responsibility that collation isn't missed in such cases.
 * ----------
 */
fn get_const_expr(constval: *mut Const, context: *mut deparse_context, showtype: i32) {
    unsafe {
        let buf = (*context).buf;
        let mut typoutput: Oid = 0;
        let mut typ_is_varlena: bool = false;
        let mut needlabel = false;

        if (*constval).constisnull {
            /*
             * Always label the type of a NULL constant to prevent misdecisions
             * about type when reparsing.
             */
            appendStringInfoString(buf, b"NULL\0".as_ptr() as _);
            if showtype >= 0 {
                appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                    format_type_with_typemod((*constval).consttype, (*constval).consttypmod)).to_string_lossy());
                get_const_collation(constval, context);
            }
            return;
        }

        getTypeOutputInfo((*constval).consttype, &mut typoutput, &mut typ_is_varlena);

        let extval = OidOutputFunctionCall(typoutput, (*constval).constvalue);

        match (*constval).consttype {
            INT4OID => {
                /*
                 * INT4 can be printed without any decoration, unless it is
                 * negative; in that case print it as '-nnn'::integer to ensure
                 * that the output will re-parse as a constant, not as a constant
                 * plus operator.  In most cases we could get away with printing
                 * (-nnn) instead, because of the way that gram.y handles negative
                 * literals; but that doesn't work for INT_MIN, and it doesn't
                 * seem that much prettier anyway.
                 */
                if *extval != b'-' as i8 {
                    appendStringInfoString(buf, extval);
                } else {
                    appendStringInfo!(buf, "'{}'",
                        ::std::ffi::CStr::from_ptr(extval).to_string_lossy());
                    needlabel = true; /* we must attach a cast */
                }
            }
            NUMERICOID => {
                /*
                 * NUMERIC can be printed without quotes if it looks like a float
                 * constant (not an integer, and not Infinity or NaN) and doesn't
                 * have a leading sign (for the same reason as for INT4).
                 */
                let s = ::std::ffi::CStr::from_ptr(extval).to_bytes();
                if s.first().map_or(false, |c| c.is_ascii_digit()) &&
                    s.iter().any(|&c| c == b'e' || c == b'E' || c == b'.')
                {
                    appendStringInfoString(buf, extval);
                } else {
                    appendStringInfo!(buf, "'{}'",
                        ::std::ffi::CStr::from_ptr(extval).to_string_lossy());
                    needlabel = true; /* we must attach a cast */
                }
            }
            BOOLOID => {
                if ::std::ffi::CStr::from_ptr(extval).to_bytes() == b"t" {
                    appendStringInfoString(buf, b"true\0".as_ptr() as _);
                } else {
                    appendStringInfoString(buf, b"false\0".as_ptr() as _);
                }
            }
            _ => {
                simple_quote_literal(buf, extval);
            }
        }

        pfree(extval as *mut _);

        if showtype < 0 {
            return;
        }

        /*
         * For showtype == 0, append ::typename unless the constant will be
         * implicitly typed as the right type when it is read in.
         *
         * XXX this code has to be kept in sync with the behavior of the parser,
         * especially make_const.
         */
        match (*constval).consttype {
            BOOLOID | UNKNOWNOID => {
                /* These types can be left unlabeled */
                needlabel = false;
            }
            INT4OID => {
                /* We determined above whether a label is needed */
            }
            NUMERICOID => {
                /*
                 * Float-looking constants will be typed as numeric, which we
                 * checked above; but if there's a nondefault typmod we need to
                 * show it.
                 */
                needlabel |= (*constval).consttypmod >= 0;
            }
            _ => {
                needlabel = true;
            }
        }
        if needlabel || showtype > 0 {
            appendStringInfo!(buf, "::{}", ::std::ffi::CStr::from_ptr(
                format_type_with_typemod((*constval).consttype, (*constval).consttypmod)).to_string_lossy());
        }

        get_const_collation(constval, context);
    }
}

/*
 * helper for get_const_expr: append COLLATE if needed
 */
fn get_const_collation(constval: *mut Const, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        if OidIsValid!((*constval).constcollid) {
            let typcollation = get_typcollation((*constval).consttype);
            if (*constval).constcollid != typcollation {
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name((*constval).constcollid)).to_string_lossy());
            }
        }
    }
}

/*
 * get_json_path_spec      - Parse back a JSON path specification
 */
fn get_json_path_spec(path_spec: *mut Node, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        if IsA!(path_spec, Const) {
            get_const_expr(path_spec as *mut Const, context, -1);
        } else {
            get_rule_expr(path_spec, context, showimplicit);
        }
    }
}

/*
 * get_json_format          - Parse back a JsonFormat node
 */
fn get_json_format(format: *mut JsonFormat, buf: *mut StringInfoData) {
    unsafe {
        if (*format).format_type == JS_FORMAT_DEFAULT {
            return;
        }
        appendStringInfoString(buf,
            if (*format).format_type == JS_FORMAT_JSONB {
                b" FORMAT JSONB\0".as_ptr() as _
            } else {
                b" FORMAT JSON\0".as_ptr() as _
            });
        if (*format).encoding != JS_ENC_DEFAULT {
            let encoding = if (*format).encoding == JS_ENC_UTF16 {
                "UTF16"
            } else if (*format).encoding == JS_ENC_UTF32 {
                "UTF32"
            } else {
                "UTF8"
            };
            appendStringInfo!(buf, " ENCODING {}", encoding);
        }
    }
}

/*
 * get_json_returning       - Parse back a JsonReturning structure
 */
fn get_json_returning(
    returning: *mut JsonReturning,
    buf: *mut StringInfoData,
    json_format_by_default: bool,
) {
    unsafe {
        if !OidIsValid!((*returning).typid) {
            return;
        }
        appendStringInfo!(buf, " RETURNING {}",
            ::std::ffi::CStr::from_ptr(format_type_with_typemod(
                (*returning).typid, (*returning).typmod)).to_string_lossy());

        if !json_format_by_default ||
            (*(*returning).format).format_type !=
            (if (*returning).typid == JSONBOID { JS_FORMAT_JSONB } else { JS_FORMAT_JSON })
        {
            get_json_format((*returning).format, buf);
        }
    }
}

/*
 * get_json_constructor     - Parse back a JsonConstructorExpr node
 */
fn get_json_constructor(
    ctor: *mut JsonConstructorExpr,
    context: *mut deparse_context,
    showimplicit: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let is_json_object: bool;

        if (*ctor).r#type == JSCTOR_JSON_OBJECTAGG {
            get_json_agg_constructor(ctor, context, b"JSON_OBJECTAGG\0".as_ptr() as _, true);
            return;
        } else if (*ctor).r#type == JSCTOR_JSON_ARRAYAGG {
            get_json_agg_constructor(ctor, context, b"JSON_ARRAYAGG\0".as_ptr() as _, false);
            return;
        }

        let funcname: *const ::std::os::raw::c_char = match (*ctor).r#type {
            JSCTOR_JSON_OBJECT    => b"JSON_OBJECT\0".as_ptr() as _,
            JSCTOR_JSON_ARRAY     => b"JSON_ARRAY\0".as_ptr() as _,
            JSCTOR_JSON_PARSE     => b"JSON\0".as_ptr() as _,
            JSCTOR_JSON_SCALAR    => b"JSON_SCALAR\0".as_ptr() as _,
            JSCTOR_JSON_SERIALIZE => b"JSON_SERIALIZE\0".as_ptr() as _,
            _ => {
                elog!(ERROR, "invalid JsonConstructorType {}", (*ctor).r#type as i32);
                std::ptr::null()
            }
        };

        appendStringInfo!(buf, "{}(", ::std::ffi::CStr::from_ptr(funcname).to_string_lossy());

        is_json_object = (*ctor).r#type == JSCTOR_JSON_OBJECT;
        let mut curridx: i32 = 0;
        let mut lc = list_head((*ctor).args);
        while !lc.is_null() {
            if curridx > 0 {
                let sep = if is_json_object && (curridx % 2) != 0 { " : " } else { ", " };
                appendStringInfoString(buf, sep.as_ptr() as _);
            }
            get_rule_expr(crate::current_cell!(lc) as *mut Node, context, true);
            curridx += 1;
            lc = lnext!((*ctor).args, lc);
        }

        get_json_constructor_options(ctor, buf);
        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * Append options, if any, to the JSON constructor being deparsed
 */
fn get_json_constructor_options(ctor: *mut JsonConstructorExpr, buf: *mut StringInfoData) {
    unsafe {
        if (*ctor).absent_on_null {
            if (*ctor).r#type == JSCTOR_JSON_OBJECT || (*ctor).r#type == JSCTOR_JSON_OBJECTAGG {
                appendStringInfoString(buf, b" ABSENT ON NULL\0".as_ptr() as _);
            }
        } else {
            if (*ctor).r#type == JSCTOR_JSON_ARRAY || (*ctor).r#type == JSCTOR_JSON_ARRAYAGG {
                appendStringInfoString(buf, b" NULL ON NULL\0".as_ptr() as _);
            }
        }

        if (*ctor).unique {
            appendStringInfoString(buf, b" WITH UNIQUE KEYS\0".as_ptr() as _);
        }

        /*
         * Append RETURNING clause if needed; JSON() and JSON_SCALAR() don't
         * support one.
         */
        if (*ctor).r#type != JSCTOR_JSON_PARSE && (*ctor).r#type != JSCTOR_JSON_SCALAR {
            get_json_returning((*ctor).returning, buf, true);
        }
    }
}

/*
 * get_json_agg_constructor - Parse back an aggregate JsonConstructorExpr node
 */
fn get_json_agg_constructor(
    ctor: *mut JsonConstructorExpr,
    context: *mut deparse_context,
    funcname: *const ::std::os::raw::c_char,
    is_json_objectagg: bool,
) {
    unsafe {
        let mut options = StringInfoData {
            data: std::ptr::null_mut(),
            len: 0,
            maxlen: 0,
            cursor: 0,
        };
        initStringInfo(&mut options);
        get_json_constructor_options(ctor, &mut options);

        if IsA!((*ctor).func as *mut Node, Aggref) {
            get_agg_expr_helper((*ctor).func as *mut Aggref, context,
                (*ctor).func as *mut Aggref,
                funcname, options.data, is_json_objectagg);
        } else if IsA!((*ctor).func as *mut Node, WindowFunc) {
            get_windowfunc_expr_helper((*ctor).func as *mut WindowFunc, context,
                funcname, options.data, is_json_objectagg);
        } else {
            elog!(ERROR, "invalid JsonConstructorExpr underlying node type: {}",
                nodeTag((*ctor).func as *mut Node) as i32);
        }
    }
}

/*
 * simple_quote_literal - Format a string as a SQL literal, append to buf
 */
fn simple_quote_literal(buf: *mut StringInfoData, val: *const ::std::os::raw::c_char) {
    unsafe {
        /*
         * We form the string literal according to the prevailing setting of
         * standard_conforming_strings; we never use E''. User is responsible for
         * making sure result is used correctly.
         */
        appendStringInfoChar(buf, b'\'' as _);
        let mut valptr = val;
        while *valptr != 0 {
            let ch = *valptr as u8;
            if SQL_STR_DOUBLE!(ch, !standard_conforming_strings) {
                appendStringInfoChar(buf, ch as _);
            }
            appendStringInfoChar(buf, ch as _);
            valptr = valptr.add(1);
        }
        appendStringInfoChar(buf, b'\'' as _);
    }
}

/* ----------
 * get_sublink_expr         - Parse back a sublink
 * ----------
 */
fn get_sublink_expr(sublink: *mut SubLink, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let query = (*sublink).subselect as *mut Query;
        let mut opname: *mut ::std::os::raw::c_char = std::ptr::null_mut();
        let mut need_paren: bool;

        if (*sublink).subLinkType == ARRAY_SUBLINK {
            appendStringInfoString(buf, b"ARRAY(\0".as_ptr() as _);
        } else {
            appendStringInfoChar(buf, b'(' as _);
        }

        /*
         * Note that we print the name of only the first operator, when there are
         * multiple combining operators.  This is an approximation that could go
         * wrong in various scenarios (operators in different schemas, renamed
         * operators, etc) but there is not a whole lot we can do about it, since
         * the syntax allows only one operator to be shown.
         */
        if !(*sublink).testexpr.is_null() {
            if IsA!((*sublink).testexpr, OpExpr) {
                /* single combining operator */
                let opexpr = (*sublink).testexpr as *mut OpExpr;
                get_rule_expr(linitial!((*opexpr).args) as *mut Node, context, true);
                opname = generate_operator_name((*opexpr).opno,
                    exprType(linitial!((*opexpr).args) as *mut Node),
                    exprType(lsecond!((*opexpr).args) as *mut Node));
            } else if IsA!((*sublink).testexpr, BoolExpr) {
                /* multiple combining operators, = or <> cases */
                let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
                appendStringInfoChar(buf, b'(' as _);
                let mut l = list_head((*((*sublink).testexpr as *mut BoolExpr)).args);
                while !l.is_null() {
                    let opexpr = crate::current_cell!(l) as *mut OpExpr;
                    appendStringInfoString(buf, sep);
                    get_rule_expr(linitial!((*opexpr).args) as *mut Node, context, true);
                    if opname.is_null() {
                        opname = generate_operator_name((*opexpr).opno,
                            exprType(linitial!((*opexpr).args) as *mut Node),
                            exprType(lsecond!((*opexpr).args) as *mut Node));
                    }
                    sep = b", \0".as_ptr() as _;
                    l = lnext!((*((*sublink).testexpr as *mut BoolExpr)).args, l);
                }
                appendStringInfoChar(buf, b')' as _);
            } else if IsA!((*sublink).testexpr, RowCompareExpr) {
                /* multiple combining operators, < <= > >= cases */
                let rcexpr = (*sublink).testexpr as *mut RowCompareExpr;
                appendStringInfoChar(buf, b'(' as _);
                get_rule_expr((*rcexpr).largs as *mut Node, context, true);
                opname = generate_operator_name(linitial_oid!((*rcexpr).opnos),
                    exprType(linitial!((*rcexpr).largs) as *mut Node),
                    exprType(linitial!((*rcexpr).rargs) as *mut Node));
                appendStringInfoChar(buf, b')' as _);
            } else {
                elog!(ERROR, "unrecognized testexpr type: {}",
                    nodeTag((*sublink).testexpr) as i32);
            }
        }

        need_paren = true;

        match (*sublink).subLinkType {
            EXISTS_SUBLINK => {
                appendStringInfoString(buf, b"EXISTS \0".as_ptr() as _);
            }
            ANY_SUBLINK => {
                if ::std::ffi::CStr::from_ptr(opname).to_bytes() == b"=" {
                    /* Represent = ANY as IN */
                    appendStringInfoString(buf, b" IN \0".as_ptr() as _);
                } else {
                    appendStringInfo!(buf, " {} ANY ",
                        ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
                }
            }
            ALL_SUBLINK => {
                appendStringInfo!(buf, " {} ALL ",
                    ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
            }
            ROWCOMPARE_SUBLINK => {
                appendStringInfo!(buf, " {} ",
                    ::std::ffi::CStr::from_ptr(opname).to_string_lossy());
            }
            EXPR_SUBLINK | MULTIEXPR_SUBLINK | ARRAY_SUBLINK => {
                need_paren = false;
            }
            CTE_SUBLINK | _ => {
                /* shouldn't occur in a SubLink */
                elog!(ERROR, "unrecognized sublink type: {}", (*sublink).subLinkType as i32);
            }
        }

        if need_paren {
            appendStringInfoChar(buf, b'(' as _);
        }

        get_query_def(query, buf, (*context).namespaces, std::ptr::null_mut(), false,
            (*context).prettyFlags, (*context).wrapColumn, (*context).indentLevel);

        if need_paren {
            appendStringInfoString(buf, b"))\0".as_ptr() as _);
        } else {
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/* ----------
 * get_xmltable             - Parse back a XMLTABLE function
 * ----------
 */
fn get_xmltable(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;

        appendStringInfoString(buf, b"XMLTABLE(\0".as_ptr() as _);

        if !(*tf).ns_uris.is_null() {
            let mut first = true;
            appendStringInfoString(buf, b"XMLNAMESPACES (\0".as_ptr() as _);
            let mut lc1 = list_head((*tf).ns_uris);
            let mut lc2 = list_head((*tf).ns_names);
            while !lc1.is_null() {
                let expr = crate::current_cell!(lc1) as *mut Node;
                let ns_node = crate::current_cell!(lc2) as *mut String;

                if !first { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                else { first = false; }

                if !ns_node.is_null() {
                    get_rule_expr(expr, context, showimplicit);
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier((*ns_node).sval)).to_string_lossy());
                } else {
                    appendStringInfoString(buf, b"DEFAULT \0".as_ptr() as _);
                    get_rule_expr(expr, context, showimplicit);
                }
                lc1 = lnext!((*tf).ns_uris, lc1);
                lc2 = lnext!((*tf).ns_names, lc2);
            }
            appendStringInfoString(buf, b"), \0".as_ptr() as _);
        }

        appendStringInfoChar(buf, b'(' as _);
        get_rule_expr((*tf).rowexpr as *mut Node, context, showimplicit);
        appendStringInfoString(buf, b") PASSING (\0".as_ptr() as _);
        get_rule_expr((*tf).docexpr as *mut Node, context, showimplicit);
        appendStringInfoChar(buf, b')' as _);

        if !(*tf).colexprs.is_null() {
            let mut colnum: i32 = 0;
            appendStringInfoString(buf, b" COLUMNS \0".as_ptr() as _);
            let mut l1 = list_head((*tf).colnames);
            let mut l2 = list_head((*tf).coltypes);
            let mut l3 = list_head((*tf).coltypmods);
            let mut l4 = list_head((*tf).colexprs);
            let mut l5 = list_head((*tf).coldefexprs);
            while !l1.is_null() {
                let colname = strVal!(crate::current_cell!(l1) as *mut Node);
                let typid = lfirst_oid!(l2);
                let typmod = lfirst_int!(l3);
                let colexpr = crate::current_cell!(l4) as *mut Node;
                let coldefexpr = crate::current_cell!(l5) as *mut Node;
                let ordinality = (*tf).ordinalitycol == colnum;
                let notnull = bms_is_member(colnum, (*tf).notnulls);

                if colnum > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                colnum += 1;

                appendStringInfo!(buf, "{} {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(colname)).to_string_lossy(),
                    if ordinality { "FOR ORDINALITY".to_string() }
                    else { ::std::ffi::CStr::from_ptr(format_type_with_typemod(typid, typmod)).to_string_lossy().into_owned() });

                if ordinality {
                    l1 = lnext!((*tf).colnames, l1);
                    l2 = lnext!((*tf).coltypes, l2);
                    l3 = lnext!((*tf).coltypmods, l3);
                    l4 = lnext!((*tf).colexprs, l4);
                    l5 = lnext!((*tf).coldefexprs, l5);
                    continue;
                }

                if !coldefexpr.is_null() {
                    appendStringInfoString(buf, b" DEFAULT (\0".as_ptr() as _);
                    get_rule_expr(coldefexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);
                }
                if !colexpr.is_null() {
                    appendStringInfoString(buf, b" PATH (\0".as_ptr() as _);
                    get_rule_expr(colexpr, context, showimplicit);
                    appendStringInfoChar(buf, b')' as _);
                }
                if notnull {
                    appendStringInfoString(buf, b" NOT NULL\0".as_ptr() as _);
                }

                l1 = lnext!((*tf).colnames, l1);
                l2 = lnext!((*tf).coltypes, l2);
                l3 = lnext!((*tf).coltypmods, l3);
                l4 = lnext!((*tf).colexprs, l4);
                l5 = lnext!((*tf).coldefexprs, l5);
            }
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_json_table_nested_columns - Parse back nested JSON_TABLE columns
 */
fn get_json_table_nested_columns(
    tf: *mut TableFunc,
    plan: *mut JsonTablePlan,
    context: *mut deparse_context,
    showimplicit: bool,
    needcomma: bool,
) {
    unsafe {
        if IsA!(plan as *mut Node, JsonTablePathScan) {
            let scan = plan as *mut JsonTablePathScan;
            if needcomma { appendStringInfoChar((*context).buf, b',' as _); }
            appendStringInfoChar((*context).buf, b' ' as _);
            appendContextKeyword(context, b"NESTED PATH \0".as_ptr() as _, 0, 0, 0);
            get_const_expr((*scan).path_value, context, -1);
            appendStringInfo!((*context).buf, " AS {}",
                ::std::ffi::CStr::from_ptr(quote_identifier((*scan).path_name)).to_string_lossy());
            get_json_table_columns(tf, scan, context, showimplicit);
        } else if IsA!(plan as *mut Node, JsonTableSiblingJoin) {
            let join = plan as *mut JsonTableSiblingJoin;
            get_json_table_nested_columns(tf, (*join).lplan, context, showimplicit, needcomma);
            get_json_table_nested_columns(tf, (*join).rplan, context, showimplicit, true);
        }
    }
}

/*
 * get_json_table_columns - Parse back JSON_TABLE columns
 */
fn get_json_table_columns(
    tf: *mut TableFunc,
    scan: *mut JsonTablePathScan,
    context: *mut deparse_context,
    showimplicit: bool,
) {
    unsafe {
        let buf = (*context).buf;
        let mut colnum: i32 = 0;

        appendStringInfoChar(buf, b' ' as _);
        appendContextKeyword(context, b"COLUMNS (\0".as_ptr() as _, 0, 0, 0);

        if PRETTY_INDENT!(context) {
            (*context).indentLevel += PRETTYINDENT_VAR;
        }

        let mut lc_colname = list_head((*tf).colnames);
        let mut lc_coltype = list_head((*tf).coltypes);
        let mut lc_coltypmod = list_head((*tf).coltypmods);
        let mut lc_colvalexpr = list_head((*tf).colvalexprs);
        while !lc_colname.is_null() {
            let colname = strVal!(crate::current_cell!(lc_colname) as *mut Node);
            let typid = lfirst_oid!(lc_coltype);
            let typmod = lfirst_int!(lc_coltypmod);
            let colexpr_node = crate::current_cell!(lc_colvalexpr);
            let colexpr: *mut JsonExpr = if colexpr_node.is_null() {
                std::ptr::null_mut()
            } else {
                colexpr_node as *mut JsonExpr
            };
            let default_behavior: JsonBehaviorType;

            /* Skip columns that don't belong to this scan. */
            if (*scan).colMin < 0 || colnum < (*scan).colMin {
                colnum += 1;
                lc_colname = lnext!((*tf).colnames, lc_colname);
                lc_coltype = lnext!((*tf).coltypes, lc_coltype);
                lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
                lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
                continue;
            }
            if colnum > (*scan).colMax { break; }

            if colnum > (*scan).colMin {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
            }

            colnum += 1;

            let ordinality = colexpr.is_null();

            appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);

            appendStringInfo!(buf, "{} {}",
                ::std::ffi::CStr::from_ptr(quote_identifier(colname)).to_string_lossy(),
                if ordinality { "FOR ORDINALITY".to_string() }
                else { ::std::ffi::CStr::from_ptr(format_type_with_typemod(typid, typmod)).to_string_lossy().into_owned() });

            if ordinality {
                lc_colname = lnext!((*tf).colnames, lc_colname);
                lc_coltype = lnext!((*tf).coltypes, lc_coltype);
                lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
                lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
                continue;
            }

            /*
             * Set default_behavior to guide get_json_expr_options() on whether to
             * emit the ON ERROR / EMPTY clauses.
             */
            if (*colexpr).op == JSON_EXISTS_OP {
                appendStringInfoString(buf, b" EXISTS\0".as_ptr() as _);
                default_behavior = JSON_BEHAVIOR_FALSE;
            } else {
                if (*colexpr).op == JSON_QUERY_OP {
                    let mut typcategory: ::std::os::raw::c_char = 0;
                    let mut typispreferred = false;
                    get_type_category_preferred(typid, &mut typcategory, &mut typispreferred);
                    if typcategory == TYPCATEGORY_STRING as i8 {
                        appendStringInfoString(buf,
                            if (*(*colexpr).format).format_type == JS_FORMAT_JSONB {
                                b" FORMAT JSONB\0".as_ptr() as _
                            } else {
                                b" FORMAT JSON\0".as_ptr() as _
                            });
                    }
                }
                default_behavior = JSON_BEHAVIOR_NULL;
            }

            appendStringInfoString(buf, b" PATH \0".as_ptr() as _);
            get_json_path_spec((*colexpr).path_spec, context, showimplicit);
            get_json_expr_options(colexpr, context, default_behavior);

            lc_colname = lnext!((*tf).colnames, lc_colname);
            lc_coltype = lnext!((*tf).coltypes, lc_coltype);
            lc_coltypmod = lnext!((*tf).coltypmods, lc_coltypmod);
            lc_colvalexpr = lnext!((*tf).colvalexprs, lc_colvalexpr);
        }

        if !(*scan).child.is_null() {
            get_json_table_nested_columns(tf, (*scan).child, context, showimplicit,
                (*scan).colMin >= 0);
        }

        if PRETTY_INDENT!(context) {
            (*context).indentLevel -= PRETTYINDENT_VAR;
        }

        appendContextKeyword(context, b")\0".as_ptr() as _, 0, 0, 0);
    }
}

/* ----------
 * get_json_table           - Parse back a JSON_TABLE function
 * ----------
 */
fn get_json_table(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        let buf = (*context).buf;
        let jexpr = (*tf).docexpr as *mut JsonExpr;
        let root = (*tf).plan as *mut JsonTablePathScan;

        appendStringInfoString(buf, b"JSON_TABLE(\0".as_ptr() as _);

        if PRETTY_INDENT!(context) {
            (*context).indentLevel += PRETTYINDENT_VAR;
        }

        appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);

        get_rule_expr((*jexpr).formatted_expr, context, showimplicit);
        appendStringInfoString(buf, b", \0".as_ptr() as _);
        get_const_expr((*root).path_value, context, -1);
        appendStringInfo!(buf, " AS {}",
            ::std::ffi::CStr::from_ptr(quote_identifier((*root).path_name)).to_string_lossy());

        if !(*jexpr).passing_values.is_null() {
            let mut needcomma = false;
            appendStringInfoChar(buf, b' ' as _);
            appendContextKeyword(context, b"PASSING \0".as_ptr() as _, 0, 0, 0);

            if PRETTY_INDENT!(context) { (*context).indentLevel += PRETTYINDENT_VAR; }

            let mut lc1 = list_head((*jexpr).passing_names);
            let mut lc2 = list_head((*jexpr).passing_values);
            while !lc1.is_null() {
                if needcomma { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                needcomma = true;
                appendContextKeyword(context, b"\0".as_ptr() as _, 0, 0, 0);
                get_rule_expr(crate::current_cell!(lc2) as *mut Node, context, false);
                appendStringInfo!(buf, " AS {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(
                        (*(crate::current_cell!(lc1) as *mut String)).sval)).to_string_lossy());
                lc1 = lnext!((*jexpr).passing_names, lc1);
                lc2 = lnext!((*jexpr).passing_values, lc2);
            }

            if PRETTY_INDENT!(context) { (*context).indentLevel -= PRETTYINDENT_VAR; }
        }

        get_json_table_columns(tf, root, context, showimplicit);

        if (*(*jexpr).on_error).btype != JSON_BEHAVIOR_EMPTY_ARRAY {
            get_json_behavior((*jexpr).on_error, context, b"ERROR\0".as_ptr() as _);
        }

        if PRETTY_INDENT!(context) { (*context).indentLevel -= PRETTYINDENT_VAR; }

        appendContextKeyword(context, b")\0".as_ptr() as _, 0, 0, 0);
    }
}

/* ----------
 * get_tablefunc             - Parse back a table function
 * ----------
 */
fn get_tablefunc(tf: *mut TableFunc, context: *mut deparse_context, showimplicit: bool) {
    unsafe {
        /* XMLTABLE and JSON_TABLE are the only existing implementations. */
        if (*tf).functype == TFT_XMLTABLE {
            get_xmltable(tf, context, showimplicit);
        } else if (*tf).functype == TFT_JSON_TABLE {
            get_json_table(tf, context, showimplicit);
        }
    }
}

/* ----------
 * get_from_clause           - Parse back a FROM clause
 *
 * "prefix" is the keyword that denotes the start of the list of FROM
 * elements. It is FROM when used to parse back SELECT and UPDATE, but
 * is USING when parsing back DELETE.
 * ----------
 */
fn get_from_clause(
    query: *mut Query,
    prefix: *const ::std::os::raw::c_char,
    context: *mut deparse_context,
) {
    unsafe {
        let buf = (*context).buf;
        let mut first = true;

        /*
         * We use the query's jointree as a guide to what to print.  However, we
         * must ignore auto-added RTEs that are marked not inFromCl. (These can
         * only appear at the top level of the jointree, so it's sufficient to
         * check here.)  This check also ensures we ignore the rule pseudo-RTEs
         * for NEW and OLD.
         */
        let mut l = list_head((*(*query).jointree).fromlist);
        while !l.is_null() {
            let jtnode = crate::current_cell!(l) as *mut Node;

            if IsA!(jtnode, RangeTblRef) {
                let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
                let rte = rt_fetch(varno, (*query).rtable);
                if !(*rte).inFromCl {
                    l = lnext!((*(*query).jointree).fromlist, l);
                    continue;
                }
            }

            if first {
                appendContextKeyword(context, prefix,
                    -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, 2);
                first = false;
                get_from_clause_item(jtnode, query, context);
            } else {
                let mut itembuf = StringInfoData {
                    data: std::ptr::null_mut(),
                    len: 0, maxlen: 0, cursor: 0,
                };
                appendStringInfoString(buf, b", \0".as_ptr() as _);

                /*
                 * Put the new FROM item's text into itembuf so we can decide
                 * after we've got it whether or not it needs to go on a new line.
                 */
                initStringInfo(&mut itembuf);
                (*context).buf = &mut itembuf;

                get_from_clause_item(jtnode, query, context);

                /* Restore context's output buffer */
                (*context).buf = buf;

                /* Consider line-wrapping if enabled */
                if PRETTY_INDENT!(context) && (*context).wrapColumn >= 0 {
                    /* Does the new item start with a new line? */
                    if itembuf.len > 0 && *itembuf.data == b'\n' as i8 {
                        /* If so, we shouldn't add anything */
                        /* instead, remove any trailing spaces currently in buf */
                        removeStringInfoSpaces(buf);
                    } else {
                        /* Locate the start of the current line in the buffer */
                        let trailing_nl = strrchr((*buf).data, b'\n' as i32);
                        let trailing_nl_ptr = if trailing_nl.is_null() {
                            (*buf).data
                        } else {
                            trailing_nl.add(1)
                        };

                        /*
                         * Add a newline, plus some indentation, if the new item
                         * would cause an overflow.
                         */
                        let trailing_len = libc::strlen(trailing_nl_ptr) as i32;
                        if trailing_len + itembuf.len > (*context).wrapColumn {
                            appendContextKeyword(context, b"\0".as_ptr() as _,
                                -(PRETTYINDENT_STD as i32),
                                PRETTYINDENT_STD,
                                PRETTYINDENT_VAR);
                        }
                    }
                }

                /* Add the new item */
                appendBinaryStringInfo(buf, itembuf.data, itembuf.len);

                /* clean up */
                pfree(itembuf.data as *mut _);
            }
            l = lnext!((*(*query).jointree).fromlist, l);
        }
    }
}

fn get_from_clause_item(jtnode: *mut Node, query: *mut Query, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;

        if IsA!(jtnode, RangeTblRef) {
            let varno = (*(jtnode as *mut RangeTblRef)).rtindex;
            let rte = rt_fetch(varno, (*query).rtable);
            let colinfo = deparse_columns_fetch(varno, dpns);
            let mut rtfunc1: *mut RangeTblFunction = std::ptr::null_mut();

            if (*rte).lateral {
                appendStringInfoString(buf, b"LATERAL \0".as_ptr() as _);
            }

            /* Print the FROM item proper */
            match (*rte).rtekind {
                RTE_RELATION => {
                    /* Normal relation RTE */
                    appendStringInfo!(buf, "{}{}",
                        ::std::ffi::CStr::from_ptr(only_marker(rte)).to_string_lossy(),
                        ::std::ffi::CStr::from_ptr(generate_relation_name((*rte).relid,
                            (*context).namespaces)).to_string_lossy());
                }
                RTE_SUBQUERY => {
                    /* Subquery RTE */
                    appendStringInfoChar(buf, b'(' as _);
                    get_query_def((*rte).subquery, buf, (*context).namespaces,
                        std::ptr::null_mut(), true,
                        (*context).prettyFlags, (*context).wrapColumn,
                        (*context).indentLevel);
                    appendStringInfoChar(buf, b')' as _);
                }
                RTE_FUNCTION => {
                    /* Function RTE */
                    rtfunc1 = linitial!((*rte).functions) as *mut RangeTblFunction;

                    /*
                     * Omit ROWS FROM() syntax for just one function, unless it
                     * has both a coldeflist and WITH ORDINALITY. If it has both,
                     * we must use ROWS FROM() syntax to avoid ambiguity about
                     * whether the coldeflist includes the ordinality column.
                     */
                    if list_length((*rte).functions) == 1 &&
                        ((*rtfunc1).funccolnames.is_null() || !(*rte).funcordinality)
                    {
                        get_rule_expr_funccall((*rtfunc1).funcexpr, context, true);
                        /* we'll print the coldeflist below, if it has one */
                    } else {
                        /*
                         * If all the function calls in the list are to unnest,
                         * and none need a coldeflist, then collapse the list back
                         * down to UNNEST(args).
                         */
                        let mut all_unnest = true;
                        let mut lc = list_head((*rte).functions);
                        while !lc.is_null() {
                            let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                            if !IsA!((*rtfunc).funcexpr, FuncExpr) ||
                                (*((*rtfunc).funcexpr as *mut FuncExpr)).funcid != F_UNNEST_ANYARRAY ||
                                !(*rtfunc).funccolnames.is_null()
                            {
                                all_unnest = false;
                                break;
                            }
                            lc = lnext!((*rte).functions, lc);
                        }

                        if all_unnest {
                            let mut allargs: *mut List = std::ptr::null_mut();
                            let mut lc = list_head((*rte).functions);
                            while !lc.is_null() {
                                let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                                let args = (*((*rtfunc).funcexpr as *mut FuncExpr)).args;
                                allargs = list_concat(allargs, args);
                                lc = lnext!((*rte).functions, lc);
                            }
                            appendStringInfoString(buf, b"UNNEST(\0".as_ptr() as _);
                            get_rule_expr(allargs as *mut Node, context, true);
                            appendStringInfoChar(buf, b')' as _);
                        } else {
                            let mut funcno: i32 = 0;
                            appendStringInfoString(buf, b"ROWS FROM(\0".as_ptr() as _);
                            let mut lc = list_head((*rte).functions);
                            while !lc.is_null() {
                                let rtfunc = crate::current_cell!(lc) as *mut RangeTblFunction;
                                if funcno > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                                get_rule_expr_funccall((*rtfunc).funcexpr, context, true);
                                if !(*rtfunc).funccolnames.is_null() {
                                    /* Reconstruct the column definition list */
                                    appendStringInfoString(buf, b" AS \0".as_ptr() as _);
                                    get_from_clause_coldeflist(rtfunc, std::ptr::null_mut(), context);
                                }
                                funcno += 1;
                                lc = lnext!((*rte).functions, lc);
                            }
                            appendStringInfoChar(buf, b')' as _);
                        }
                        /* prevent printing duplicate coldeflist below */
                        rtfunc1 = std::ptr::null_mut();
                    }
                    if (*rte).funcordinality {
                        appendStringInfoString(buf, b" WITH ORDINALITY\0".as_ptr() as _);
                    }
                }
                RTE_TABLEFUNC => {
                    get_tablefunc((*rte).tablefunc, context, true);
                }
                RTE_VALUES => {
                    /* Values list RTE */
                    appendStringInfoChar(buf, b'(' as _);
                    get_values_def((*rte).values_lists, context);
                    appendStringInfoChar(buf, b')' as _);
                }
                RTE_CTE => {
                    appendStringInfoString(buf, quote_identifier((*rte).ctename));
                }
                _ => {
                    elog!(ERROR, "unrecognized RTE kind: {}", (*rte).rtekind as i32);
                }
            }

            /* Print the relation alias, if needed */
            get_rte_alias(rte, varno, false, context);

            /* Print the column definitions or aliases, if needed */
            if !rtfunc1.is_null() && !(*rtfunc1).funccolnames.is_null() {
                /* Reconstruct the columndef list, which is also the aliases */
                get_from_clause_coldeflist(rtfunc1, colinfo, context);
            } else {
                /* Else print column aliases as needed */
                get_column_alias_list(colinfo, context);
            }

            /* Tablesample clause must go after any alias */
            if (*rte).rtekind == RTE_RELATION && !(*rte).tablesample.is_null() {
                get_tablesample_def((*rte).tablesample, context);
            }
        } else if IsA!(jtnode, JoinExpr) {
            let j = jtnode as *mut JoinExpr;
            let colinfo = deparse_columns_fetch((*j).rtindex, dpns);
            let need_paren_on_right = PRETTY_PAREN!(context) &&
                !IsA!((*j).rarg, RangeTblRef) &&
                !(IsA!((*j).rarg, JoinExpr) && (*((*j).rarg as *mut JoinExpr)).alias != std::ptr::null_mut());

            if !PRETTY_PAREN!(context) || (*j).alias != std::ptr::null_mut() {
                appendStringInfoChar(buf, b'(' as _);
            }

            get_from_clause_item((*j).larg, query, context);

            match (*j).jointype {
                JOIN_INNER => {
                    if !(*j).quals.is_null() {
                        appendContextKeyword(context, b" JOIN \0".as_ptr() as _,
                            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                    } else {
                        appendContextKeyword(context, b" CROSS JOIN \0".as_ptr() as _,
                            -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                    }
                }
                JOIN_LEFT => {
                    appendContextKeyword(context, b" LEFT JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                JOIN_FULL => {
                    appendContextKeyword(context, b" FULL JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                JOIN_RIGHT => {
                    appendContextKeyword(context, b" RIGHT JOIN \0".as_ptr() as _,
                        -(PRETTYINDENT_STD as i32), PRETTYINDENT_STD, PRETTYINDENT_JOIN);
                }
                _ => {
                    elog!(ERROR, "unrecognized join type: {}", (*j).jointype as i32);
                }
            }

            if need_paren_on_right { appendStringInfoChar(buf, b'(' as _); }
            get_from_clause_item((*j).rarg, query, context);
            if need_paren_on_right { appendStringInfoChar(buf, b')' as _); }

            if !(*j).usingClause.is_null() {
                let mut first = true;
                appendStringInfoString(buf, b" USING (\0".as_ptr() as _);
                /* Use the assigned names, not what's in usingClause */
                let mut lc = list_head((*colinfo).usingNames);
                while !lc.is_null() {
                    let colname = crate::current_cell!(lc) as *mut ::std::os::raw::c_char;
                    if first { first = false; }
                    else { appendStringInfoString(buf, b", \0".as_ptr() as _); }
                    appendStringInfoString(buf, quote_identifier(colname));
                    lc = lnext!((*colinfo).usingNames, lc);
                }
                appendStringInfoChar(buf, b')' as _);

                if !(*j).join_using_alias.is_null() {
                    appendStringInfo!(buf, " AS {}",
                        ::std::ffi::CStr::from_ptr(quote_identifier(
                            (*((*j).join_using_alias)).aliasname)).to_string_lossy());
                }
            } else if !(*j).quals.is_null() {
                appendStringInfoString(buf, b" ON \0".as_ptr() as _);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b'(' as _); }
                get_rule_expr((*j).quals, context, false);
                if !PRETTY_PAREN!(context) { appendStringInfoChar(buf, b')' as _); }
            } else if (*j).jointype != JOIN_INNER {
                /* If we didn't say CROSS JOIN above, we must provide an ON */
                appendStringInfoString(buf, b" ON TRUE\0".as_ptr() as _);
            }

            if !PRETTY_PAREN!(context) || (*j).alias != std::ptr::null_mut() {
                appendStringInfoChar(buf, b')' as _);
            }

            /* Yes, it's correct to put alias after the right paren ... */
            if (*j).alias != std::ptr::null_mut() {
                /*
                 * Note that it's correct to emit an alias clause if and only if
                 * there was one originally.  Otherwise we'd be converting a named
                 * join to unnamed or vice versa, which creates semantic
                 * subtleties we don't want.  However, we might print a different
                 * alias name than was there originally.
                 */
                appendStringInfo!(buf, " {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(
                        get_rtable_name((*j).rtindex, context))).to_string_lossy());
                get_column_alias_list(colinfo, context);
            }
        } else {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(jtnode) as i32);
        }
    }
}

/*
 * get_rte_alias - print the relation's alias, if needed
 *
 * If printed, the alias is preceded by a space, or by " AS " if use_as is true.
 */
fn get_rte_alias(
    rte: *mut RangeTblEntry,
    varno: i32,
    use_as: bool,
    context: *mut deparse_context,
) {
    unsafe {
        let dpns = linitial!((*context).namespaces) as *mut deparse_namespace;
        let refname = get_rtable_name(varno, context);
        let colinfo = deparse_columns_fetch(varno, dpns);
        let mut printalias = false;

        if !(*rte).alias.is_null() {
            /* Always print alias if user provided one */
            printalias = true;
        } else if (*colinfo).printaliases {
            /* Always print alias if we need to print column aliases */
            printalias = true;
        } else if (*rte).rtekind == RTE_RELATION {
            /*
             * No need to print alias if it's same as relation name (this would
             * normally be the case, but not if set_rtable_names had to resolve a
             * conflict).
             */
            if libc::strcmp(refname, get_relation_name((*rte).relid)) != 0 {
                printalias = true;
            }
        } else if (*rte).rtekind == RTE_FUNCTION {
            /*
             * For a function RTE, always print alias.  This covers possible
             * renaming of the function and/or instability of the FigureColname
             * rules for things that aren't simple functions.  Note we'd need to
             * force it anyway for the columndef list case.
             */
            printalias = true;
        } else if (*rte).rtekind == RTE_SUBQUERY || (*rte).rtekind == RTE_VALUES {
            /*
             * For a subquery, always print alias.  This makes the output
             * SQL-spec-compliant, even though we allow such aliases to be omitted
             * on input.
             */
            printalias = true;
        } else if (*rte).rtekind == RTE_CTE {
            /*
             * No need to print alias if it's same as CTE name (this would
             * normally be the case, but not if set_rtable_names had to resolve a
             * conflict).
             */
            if libc::strcmp(refname, (*rte).ctename) != 0 {
                printalias = true;
            }
        }

        if printalias {
            appendStringInfo!((*context).buf, "{}{}",
                if use_as { " AS " } else { " " },
                ::std::ffi::CStr::from_ptr(quote_identifier(refname)).to_string_lossy());
        }
    }
}

/*
 * get_column_alias_list - print column alias list for an RTE
 *
 * Caller must already have printed the relation's alias name.
 */
fn get_column_alias_list(colinfo: *mut deparse_columns, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut first = true;

        /* Don't print aliases if not needed */
        if !(*colinfo).printaliases {
            return;
        }

        for i in 0..(*colinfo).num_new_cols as usize {
            let colname = (*colinfo).new_colnames[i];
            if first {
                appendStringInfoChar(buf, b'(' as _);
                first = false;
            } else {
                appendStringInfoString(buf, b", \0".as_ptr() as _);
            }
            appendStringInfoString(buf, quote_identifier(colname));
        }
        if !first {
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/*
 * get_from_clause_coldeflist - reproduce FROM clause coldeflist
 *
 * When printing a top-level coldeflist (which is syntactically also the
 * relation's column alias list), use column names from colinfo.  But when
 * printing a coldeflist embedded inside ROWS FROM(), we prefer to use the
 * original coldeflist's names, which are available in rtfunc->funccolnames.
 * Pass NULL for colinfo to select the latter behavior.
 *
 * The coldeflist is appended immediately (no space) to buf.  Caller is
 * responsible for ensuring that an alias or AS is present before it.
 */
fn get_from_clause_coldeflist(
    rtfunc: *mut RangeTblFunction,
    colinfo: *mut deparse_columns,
    context: *mut deparse_context,
) {
    unsafe {
        let buf = (*context).buf;
        let mut i: i32 = 0;

        appendStringInfoChar(buf, b'(' as _);

        let mut l1 = list_head((*rtfunc).funccoltypes);
        let mut l2 = list_head((*rtfunc).funccoltypmods);
        let mut l3 = list_head((*rtfunc).funccolcollations);
        let mut l4 = list_head((*rtfunc).funccolnames);
        while !l1.is_null() {
            let atttypid = lfirst_oid!(l1);
            let atttypmod = lfirst_int!(l2);
            let attcollation = lfirst_oid!(l3);
            let attname: *const ::std::os::raw::c_char = if !colinfo.is_null() {
                (*colinfo).colnames[i as usize]
            } else {
                strVal!(crate::current_cell!(l4) as *mut Node)
            };

            assert!(!attname.is_null()); /* shouldn't be any dropped columns here */

            if i > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            appendStringInfo!(buf, "{} {}",
                ::std::ffi::CStr::from_ptr(quote_identifier(attname)).to_string_lossy(),
                ::std::ffi::CStr::from_ptr(format_type_with_typemod(atttypid, atttypmod)).to_string_lossy());
            if OidIsValid!(attcollation) && attcollation != get_typcollation(atttypid) {
                appendStringInfo!(buf, " COLLATE {}",
                    ::std::ffi::CStr::from_ptr(generate_collation_name(attcollation)).to_string_lossy());
            }

            i += 1;
            l1 = lnext!((*rtfunc).funccoltypes, l1);
            l2 = lnext!((*rtfunc).funccoltypmods, l2);
            l3 = lnext!((*rtfunc).funccolcollations, l3);
            l4 = lnext!((*rtfunc).funccolnames, l4);
        }

        appendStringInfoChar(buf, b')' as _);
    }
}

/*
 * get_tablesample_def          - print a TableSampleClause
 */
fn get_tablesample_def(tablesample: *mut TableSampleClause, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let argtypes: [Oid; 1] = [INTERNALOID];
        let mut nargs: i32 = 0;

        /*
         * We should qualify the handler's function name if it wouldn't be
         * resolved by lookup in the current search path.
         */
        appendStringInfo!(buf, " TABLESAMPLE {} (",
            ::std::ffi::CStr::from_ptr(generate_function_name(
                (*tablesample).tsmhandler, 1,
                std::ptr::null_mut(), argtypes.as_ptr() as *mut _,
                false, std::ptr::null_mut(), false)).to_string_lossy());

        let mut l = list_head((*tablesample).args);
        while !l.is_null() {
            if nargs > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            get_rule_expr(crate::current_cell!(l) as *mut Node, context, false);
            nargs += 1;
            l = lnext!((*tablesample).args, l);
        }
        appendStringInfoChar(buf, b')' as _);

        if !(*tablesample).repeatable.is_null() {
            appendStringInfoString(buf, b" REPEATABLE (\0".as_ptr() as _);
            get_rule_expr((*tablesample).repeatable as *mut Node, context, false);
            appendStringInfoChar(buf, b')' as _);
        }
    }
}

/*
 * get_opclass_name           - fetch name of an index operator class
 *
 * The opclass name is appended (after a space) to buf.
 *
 * Output is suppressed if the opclass is the default for the given
 * actual_datatype.  (If you don't want this behavior, just pass
 * InvalidOid for actual_datatype.)
 */
fn get_opclass_name(opclass: Oid, actual_datatype: Oid, buf: *mut StringInfoData) {
    unsafe {
        let ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
        if !HeapTupleIsValid!(ht_opc) {
            elog!(ERROR, "cache lookup failed for opclass {}", opclass);
        }
        let opcrec = GETSTRUCT!(ht_opc) as Form_pg_opclass;

        if !OidIsValid!(actual_datatype) ||
            GetDefaultOpClass(actual_datatype, (*opcrec).opcmethod) != opclass
        {
            /* Okay, we need the opclass name.  Do we need to qualify it? */
            let opcname = NameStr!((*opcrec).opcname);
            if OpclassIsVisible(opclass) {
                appendStringInfo!(buf, " {}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(opcname)).to_string_lossy());
            } else {
                let nspname = get_namespace_name_or_temp((*opcrec).opcnamespace);
                appendStringInfo!(buf, " {}.{}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
                    ::std::ffi::CStr::from_ptr(quote_identifier(opcname)).to_string_lossy());
            }
        }
        ReleaseSysCache(ht_opc);
    }
}

/*
 * generate_opclass_name
 *      Compute the name to display for an opclass specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
pub fn generate_opclass_name(opclass: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData {
            data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0,
        };
        initStringInfo(&mut buf);
        get_opclass_name(opclass, InvalidOid, &mut buf);
        buf.data.add(1) /* get_opclass_name() prepends space */
    }
}

/*
 * processIndirection - take care of array and subfield assignment
 *
 * We strip any top-level FieldStore or assignment SubscriptingRef nodes that
 * appear in the input, printing them as decoration for the base column
 * name (which we assume the caller just printed).  We might also need to
 * strip CoerceToDomain nodes, but only ones that appear above assignment
 * nodes.
 *
 * Returns the subexpression that's to be assigned.
 */
fn processIndirection(node: *mut Node, context: *mut deparse_context) -> *mut Node {
    unsafe {
        let buf = (*context).buf;
        let mut cdomain: *mut CoerceToDomain = std::ptr::null_mut();
        let mut node = node;

        loop {
            if node.is_null() { break; }
            if IsA!(node, FieldStore) {
                let fstore = node as *mut FieldStore;
                let mut typrelid: Oid = 0;

                /* lookup tuple type */
                typrelid = get_typ_typrelid((*fstore).resulttype);
                if !OidIsValid!(typrelid) {
                    elog!(ERROR,
                        "argument type {} of FieldStore is not a tuple type",
                        ::std::ffi::CStr::from_ptr(format_type_be((*fstore).resulttype)).to_string_lossy());
                }

                /*
                 * Print the field name.  There should only be one target field in
                 * stored rules.  There could be more than that in executable
                 * target lists, but this function cannot be used for that case.
                 */
                assert!(list_length((*fstore).fieldnums) == 1);
                let fieldname = get_attname(typrelid,
                    linitial_int!((*fstore).fieldnums) as _, false);
                appendStringInfo!(buf, ".{}",
                    ::std::ffi::CStr::from_ptr(quote_identifier(fieldname)).to_string_lossy());

                /*
                 * We ignore arg since it should be an uninteresting reference to
                 * the target column or subcolumn.
                 */
                node = linitial!((*fstore).newvals) as *mut Node;
            } else if IsA!(node, SubscriptingRef) {
                let sbsref = node as *mut SubscriptingRef;
                if (*sbsref).refassgnexpr.is_null() { break; }
                printSubscripts(sbsref, context);
                /*
                 * We ignore refexpr since it should be an uninteresting reference
                 * to the target column or subcolumn.
                 */
                node = (*sbsref).refassgnexpr as *mut Node;
            } else if IsA!(node, CoerceToDomain) {
                cdomain = node as *mut CoerceToDomain;
                /* If it's an explicit domain coercion, we're done */
                if (*cdomain).coercionformat != COERCE_IMPLICIT_CAST { break; }
                /* Tentatively descend past the CoerceToDomain */
                node = (*cdomain).arg as *mut Node;
            } else {
                break;
            }
        }

        /*
         * If we descended past a CoerceToDomain whose argument turned out not to
         * be a FieldStore or array assignment, back up to the CoerceToDomain.
         * (This is not enough to be fully correct if there are nested implicit
         * CoerceToDomains, but such cases shouldn't ever occur.)
         */
        if !cdomain.is_null() && node == (*cdomain).arg as *mut Node {
            node = cdomain as *mut Node;
        }

        node
    }
}

fn printSubscripts(sbsref: *mut SubscriptingRef, context: *mut deparse_context) {
    unsafe {
        let buf = (*context).buf;
        let mut lowlist_item = list_head((*sbsref).reflowerindexpr); /* could be NULL */
        let mut uplist_item = list_head((*sbsref).refupperindexpr);
        while !uplist_item.is_null() {
            appendStringInfoChar(buf, b'[' as _);
            if !lowlist_item.is_null() {
                /* If subexpression is NULL, get_rule_expr prints nothing */
                get_rule_expr(crate::current_cell!(lowlist_item) as *mut Node, context, false);
                appendStringInfoChar(buf, b':' as _);
                lowlist_item = lnext!((*sbsref).reflowerindexpr, lowlist_item);
            }
            /* If subexpression is NULL, get_rule_expr prints nothing */
            get_rule_expr(crate::current_cell!(uplist_item) as *mut Node, context, false);
            appendStringInfoChar(buf, b']' as _);
            uplist_item = lnext!((*sbsref).refupperindexpr, uplist_item);
        }
    }
}

/*
 * quote_identifier           - Quote an identifier only if needed
 *
 * When quotes are needed, we palloc the required space; slightly
 * space-wasteful but well worth it for notational simplicity.
 */
pub fn quote_identifier(ident: *const ::std::os::raw::c_char) -> *const ::std::os::raw::c_char {
    unsafe {
        /*
         * Can avoid quoting if ident starts with a lowercase letter or underscore
         * and contains only lowercase letters, digits, and underscores, *and* is
         * not any SQL keyword.  Otherwise, supply quotes.
         *
         * would like to use <ctype.h> macros here, but they might yield unwanted
         * locale-specific results...
         */
        let mut nquotes: i32 = 0;
        let mut safe: bool;
        let mut ptr: *const u8 = ident as _;
        let c0 = *ptr;
        safe = (c0 >= b'a' && c0 <= b'z') || c0 == b'_';

        while *ptr != 0 {
            let ch = *ptr;
            if !((ch >= b'a' && ch <= b'z') || (ch >= b'0' && ch <= b'9') || ch == b'_') {
                safe = false;
                if ch == b'"' { nquotes += 1; }
            }
            ptr = ptr.add(1);
        }

        if quote_all_identifiers { safe = false; }

        if safe {
            /*
             * Check for keyword.  We quote keywords except for unreserved ones.
             * (In some cases we could avoid quoting a col_name or type_func_name
             * keyword, but it seems much harder than it's worth to tell that.)
             *
             * Note: ScanKeywordLookup() does case-insensitive comparison, but
             * that's fine, since we already know we have all-lower-case.
             */
            let kwnum = ScanKeywordLookup(ident, &ScanKeywords);
            if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != UNRESERVED_KEYWORD as u8 {
                safe = false;
            }
        }

        if safe {
            return ident; /* no change needed */
        }

        let identlen = libc::strlen(ident);
        let result = palloc(identlen + nquotes as usize + 2 + 1) as *mut u8;

        let mut optr = result;
        *optr = b'"'; optr = optr.add(1);
        let mut ptr: *const u8 = ident as _;
        while *ptr != 0 {
            let ch = *ptr;
            if ch == b'"' { *optr = b'"'; optr = optr.add(1); }
            *optr = ch; optr = optr.add(1);
            ptr = ptr.add(1);
        }
        *optr = b'"'; optr = optr.add(1);
        *optr = 0;

        result as *const ::std::os::raw::c_char
    }
}

/*
 * quote_qualified_identifier  - Quote a possibly-qualified identifier
 *
 * Return a name of the form qualifier.ident, or just ident if qualifier
 * is NULL, quoting each component if necessary.  The result is palloc'd.
 */
pub fn quote_qualified_identifier(
    qualifier: *const ::std::os::raw::c_char,
    ident: *const ::std::os::raw::c_char,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData {
            data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0,
        };
        initStringInfo(&mut buf);
        if !qualifier.is_null() {
            appendStringInfo!((&mut buf), "{}.",
                ::std::ffi::CStr::from_ptr(quote_identifier(qualifier)).to_string_lossy());
        }
        appendStringInfoString(&mut buf, quote_identifier(ident));
        buf.data
    }
}

/*
 * get_relation_name
 *      Get the unqualified name of a relation specified by OID
 *
 * This differs from the underlying get_rel_name() function in that it will
 * throw error instead of silently returning NULL if the OID is bad.
 */
fn get_relation_name(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let relname = get_rel_name(relid);
        if relname.is_null() {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        relname
    }
}

/*
 * generate_relation_name
 *      Compute the name to display for a relation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 *
 * If namespaces isn't NIL, it must be a list of deparse_namespace nodes.
 * We will forcibly qualify the relation name if it equals any CTE name
 * visible in the namespace list.
 */
fn generate_relation_name(relid: Oid, namespaces: *mut List) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let reltup = GETSTRUCT!(tp) as Form_pg_class;
        let relname = NameStr!((*reltup).relname);

        /* Check for conflicting CTE name */
        let mut need_qual = false;
        let mut nslist = list_head(namespaces);
        'outer: while !nslist.is_null() {
            let dpns = crate::current_cell!(nslist) as *mut deparse_namespace;
            let mut ctlist = list_head((*dpns).ctes);
            while !ctlist.is_null() {
                let cte = crate::current_cell!(ctlist) as *mut CommonTableExpr;
                if libc::strcmp((*cte).ctename, relname) == 0 {
                    need_qual = true;
                    break 'outer;
                }
                ctlist = lnext!((*dpns).ctes, ctlist);
            }
            nslist = lnext!(namespaces, nslist);
        }

        /* Otherwise, qualify the name if not visible in search path */
        if !need_qual {
            need_qual = !RelationIsVisible(relid);
        }

        let nspname = if need_qual {
            get_namespace_name_or_temp((*reltup).relnamespace)
        } else {
            std::ptr::null_mut()
        };

        let result = quote_qualified_identifier(nspname, relname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_qualified_relation_name
 *      Compute the name to display for a relation specified by OID
 *
 * As above, but unconditionally schema-qualify the name.
 */
fn generate_qualified_relation_name(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let reltup = GETSTRUCT!(tp) as Form_pg_class;
        let relname = NameStr!((*reltup).relname);
        let nspname = get_namespace_name_or_temp((*reltup).relnamespace);
        if nspname.is_null() {
            elog!(ERROR, "cache lookup failed for namespace {}", (*reltup).relnamespace);
        }
        let result = quote_qualified_identifier(nspname, relname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_function_name
 *      Compute the name to display for a function specified by OID,
 *      given that it is being called with the specified actual arg names and
 *      types.  (Those matter because of ambiguous-function resolution rules.)
 *
 * If we're dealing with a potentially variadic function (in practice, this
 * means a FuncExpr or Aggref, not some other way of calling a function), then
 * has_variadic must specify whether variadic arguments have been merged,
 * and *use_variadic_p will be set to indicate whether to print VARIADIC in
 * the output.  For non-FuncExpr cases, has_variadic should be false and
 * use_variadic_p can be NULL.
 *
 * inGroupBy must be true if we're deparsing a GROUP BY clause.
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
fn generate_function_name(
    funcid: Oid,
    nargs: i32,
    argnames: *mut List,
    argtypes: *mut Oid,
    has_variadic: bool,
    use_variadic_p: *mut bool,
    in_group_by: bool,
) -> *mut ::std::os::raw::c_char {
    unsafe {
        let proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        if !HeapTupleIsValid!(proctup) {
            elog!(ERROR, "cache lookup failed for function {}", funcid);
        }
        let procform = GETSTRUCT!(proctup) as Form_pg_proc;
        let proname = NameStr!((*procform).proname);
        let mut force_qualify = false;

        /*
         * Due to parser hacks to avoid needing to reserve CUBE, we need to force
         * qualification of some function names within GROUP BY.
         */
        if in_group_by {
            let s = ::std::ffi::CStr::from_ptr(proname).to_bytes();
            if s == b"cube" || s == b"rollup" {
                force_qualify = true;
            }
        }

        /*
         * Determine whether VARIADIC should be printed.  We must do this first
         * since it affects the lookup rules in func_get_detail().
         *
         * We always print VARIADIC if the function has a merged variadic-array
         * argument.  Note that this is always the case for functions taking a
         * VARIADIC argument type other than VARIADIC ANY.  If we omitted VARIADIC
         * and printed the array elements as separate arguments, the call could
         * match a newer non-VARIADIC function.
         */
        let use_variadic: bool;
        if !use_variadic_p.is_null() {
            /* Parser should not have set funcvariadic unless fn is variadic */
            assert!(!has_variadic || OidIsValid!((*procform).provariadic));
            use_variadic = has_variadic;
            *use_variadic_p = use_variadic;
        } else {
            assert!(!has_variadic);
            use_variadic = false;
        }

        /*
         * The idea here is to schema-qualify only if the parser would fail to
         * resolve the correct function given the unqualified func name with the
         * specified argtypes and VARIADIC flag.  But if we already decided to
         * force qualification, then we can skip the lookup and pretend we didn't
         * find it.
         */
        let mut p_funcid: Oid = 0;
        let mut p_rettype: Oid = 0;
        let mut p_retset = false;
        let mut p_nvargs: i32 = 0;
        let mut p_vatype: Oid = 0;
        let mut p_true_typeids: *mut Oid = std::ptr::null_mut();
        let p_result: FuncDetailCode;

        if !force_qualify {
            p_result = func_get_detail(
                list_make1(makeString(proname)),
                std::ptr::null_mut(), argnames, nargs, argtypes,
                !use_variadic, true, false,
                &mut p_funcid, &mut p_rettype,
                &mut p_retset, &mut p_nvargs, &mut p_vatype,
                &mut p_true_typeids, std::ptr::null_mut());
        } else {
            p_result = FUNCDETAIL_NOTFOUND;
            p_funcid = InvalidOid;
        }

        let nspname = if (p_result == FUNCDETAIL_NORMAL ||
             p_result == FUNCDETAIL_AGGREGATE ||
             p_result == FUNCDETAIL_WINDOWFUNC) &&
            p_funcid == funcid
        {
            std::ptr::null_mut()
        } else {
            get_namespace_name_or_temp((*procform).pronamespace)
        };

        let result = quote_qualified_identifier(nspname, proname);
        ReleaseSysCache(proctup);
        result
    }
}

/*
 * generate_operator_name
 *      Compute the name to display for an operator specified by OID,
 *      given that it is being called with the specified actual arg types.
 *      (Arg types matter because of ambiguous-operator resolution rules.
 *      Pass InvalidOid for unused arg of a unary operator.)
 *
 * The result includes all necessary quoting and schema-prefixing,
 * plus the OPERATOR() decoration needed to use a qualified operator name
 * in an expression.
 */
fn generate_operator_name(operid: Oid, arg1: Oid, arg2: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut buf = StringInfoData { data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };
        initStringInfo(&mut buf);

        let opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
        if !HeapTupleIsValid!(opertup) {
            elog!(ERROR, "cache lookup failed for operator {}", operid);
        }
        let operform = GETSTRUCT!(opertup) as Form_pg_operator;
        let oprname = NameStr!((*operform).oprname);

        /*
         * The idea here is to schema-qualify only if the parser would fail to
         * resolve the correct operator given the unqualified op name with the
         * specified argtypes.
         */
        let p_result: Operator = match (*operform).oprkind as u8 {
            b'b' => oper(std::ptr::null_mut(),
                list_make1(makeString(oprname)), arg1, arg2, true, -1),
            b'l' => left_oper(std::ptr::null_mut(),
                list_make1(makeString(oprname)), arg2, true, -1),
            _ => {
                elog!(ERROR, "unrecognized oprkind: {}", (*operform).oprkind as i32);
                std::ptr::null_mut()
            }
        };

        let nspname: *mut ::std::os::raw::c_char = if !p_result.is_null() && oprid(p_result) == operid {
            std::ptr::null_mut()
        } else {
            let ns = get_namespace_name_or_temp((*operform).oprnamespace);
            appendStringInfo!((&mut buf), "OPERATOR({}.{}",
                ::std::ffi::CStr::from_ptr(quote_identifier(ns)).to_string_lossy(),
                ::std::ffi::CStr::from_ptr(oprname).to_string_lossy());
            ns /* not actually used below since we already appended */
        };

        if nspname.is_null() {
            appendStringInfoString(&mut buf, oprname);
        }

        if !nspname.is_null() {
            appendStringInfoChar(&mut buf, b')' as _);
        }

        if !p_result.is_null() { ReleaseSysCache(p_result); }
        ReleaseSysCache(opertup);

        buf.data
    }
}

/*
 * generate_operator_clause --- generate a binary-operator WHERE clause
 *
 * This is used for internally-generated-and-executed SQL queries, where
 * precision is essential and readability is secondary.  The basic
 * requirement is to append "leftop op rightop" to buf, where leftop and
 * rightop are given as strings and are assumed to yield types leftoptype
 * and rightoptype; the operator is identified by OID.  The complexity
 * comes from needing to be sure that the parser will select the desired
 * operator when the query is parsed.  We always name the operator using
 * OPERATOR(schema.op) syntax, so as to avoid search-path uncertainties.
 * We have to emit casts too, if either input isn't already the input type
 * of the operator; else we are at the mercy of the parser's heuristics for
 * ambiguous-operator resolution.  The caller must ensure that leftop and
 * rightop are suitable arguments for a cast operation; it's best to insert
 * parentheses if they aren't just variables or parameters.
 */
pub fn generate_operator_clause(
    buf: *mut StringInfoData,
    leftop: *const ::std::os::raw::c_char,
    leftoptype: Oid,
    opoid: Oid,
    rightop: *const ::std::os::raw::c_char,
    rightoptype: Oid,
) {
    unsafe {
        let opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
        if !HeapTupleIsValid!(opertup) {
            elog!(ERROR, "cache lookup failed for operator {}", opoid);
        }
        let operform = GETSTRUCT!(opertup) as Form_pg_operator;
        assert!((*operform).oprkind == b'b' as i8);
        let oprname = NameStr!((*operform).oprname);
        let nspname = get_namespace_name((*operform).oprnamespace);

        appendStringInfoString(buf, leftop);
        if leftoptype != (*operform).oprleft { add_cast_to(buf, (*operform).oprleft); }
        appendStringInfo!(buf, " OPERATOR({}.{}",
            ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(oprname).to_string_lossy());
        appendStringInfo!(buf, ") {}", ::std::ffi::CStr::from_ptr(rightop).to_string_lossy());
        if rightoptype != (*operform).oprright { add_cast_to(buf, (*operform).oprright); }

        ReleaseSysCache(opertup);
    }
}

/*
 * Add a cast specification to buf.  We spell out the type name the hard way,
 * intentionally not using format_type_be().  This is to avoid corner cases
 * for CHARACTER, BIT, and perhaps other types, where specifying the type
 * using SQL-standard syntax results in undesirable data truncation.  By
 * doing it this way we can be certain that the cast will have default (-1)
 * target typmod.
 */
fn add_cast_to(buf: *mut StringInfoData, typid: Oid) {
    unsafe {
        let typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid!(typetup) {
            elog!(ERROR, "cache lookup failed for type {}", typid);
        }
        let typform = GETSTRUCT!(typetup) as Form_pg_type;
        let typname = NameStr!((*typform).typname);
        let nspname = get_namespace_name_or_temp((*typform).typnamespace);
        appendStringInfo!(buf, "::{}.{}",
            ::std::ffi::CStr::from_ptr(quote_identifier(nspname)).to_string_lossy(),
            ::std::ffi::CStr::from_ptr(quote_identifier(typname)).to_string_lossy());
        ReleaseSysCache(typetup);
    }
}

/*
 * generate_qualified_type_name
 *      Compute the name to display for a type specified by OID
 *
 * This is different from format_type_be() in that we unconditionally
 * schema-qualify the name.  That also means no special syntax for
 * SQL-standard type names ... although in current usage, this should
 * only get used for domains, so such cases wouldn't occur anyway.
 */
fn generate_qualified_type_name(typid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for type {}", typid);
        }
        let typtup = GETSTRUCT!(tp) as Form_pg_type;
        let typname = NameStr!((*typtup).typname);
        let nspname = get_namespace_name_or_temp((*typtup).typnamespace);
        if nspname.is_null() {
            elog!(ERROR, "cache lookup failed for namespace {}", (*typtup).typnamespace);
        }
        let result = quote_qualified_identifier(nspname, typname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * generate_collation_name
 *      Compute the name to display for a collation specified by OID
 *
 * The result includes all necessary quoting and schema-prefixing.
 */
pub fn generate_collation_name(collid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid!(tp) {
            elog!(ERROR, "cache lookup failed for collation {}", collid);
        }
        let colltup = GETSTRUCT!(tp) as Form_pg_collation;
        let collname = NameStr!((*colltup).collname);
        let nspname = if !CollationIsVisible(collid) {
            get_namespace_name_or_temp((*colltup).collnamespace)
        } else {
            std::ptr::null_mut()
        };
        let result = quote_qualified_identifier(nspname, collname);
        ReleaseSysCache(tp);
        result
    }
}

/*
 * Given a C string, produce a TEXT datum.
 *
 * We assume that the input was palloc'd and may be freed.
 */
fn string_to_text(str_: *mut ::std::os::raw::c_char) -> *mut text {
    unsafe {
        let result = cstring_to_text(str_);
        pfree(str_ as *mut _);
        result
    }
}

/*
 * Generate a C string representing a relation options from text[] datum.
 */
fn get_reloptions(buf: *mut StringInfoData, reloptions: Datum) {
    unsafe {
        let mut options: *mut Datum = std::ptr::null_mut();
        let mut noptions: i32 = 0;

        deconstruct_array_builtin(DatumGetArrayTypeP!(reloptions), TEXTOID,
            &mut options, std::ptr::null_mut(), &mut noptions);

        for i in 0..noptions as usize {
            let option = TextDatumGetCString(*options.add(i));
            let name = option;
            let separator = libc::strchr(option, b'=' as i32);
            let value: *const ::std::os::raw::c_char;
            if !separator.is_null() {
                *separator = 0;
                value = separator.add(1);
            } else {
                value = b"\0".as_ptr() as _;
            }

            if i > 0 { appendStringInfoString(buf, b", \0".as_ptr() as _); }
            appendStringInfo!(buf, "{}=",
                ::std::ffi::CStr::from_ptr(quote_identifier(name)).to_string_lossy());

            /*
             * In general we need to quote the value; but to avoid unnecessary
             * clutter, do not quote if it is an identifier that would not need
             * quoting.
             */
            if quote_identifier(value) == value {
                appendStringInfoString(buf, value);
            } else {
                simple_quote_literal(buf, value);
            }

            pfree(option as *mut _);
        }
    }
}

/*
 * Generate a C string representing a relation's reloptions, or NULL if none.
 */
fn flatten_reloptions(relid: Oid) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut result: *mut ::std::os::raw::c_char = std::ptr::null_mut();
        let tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid!(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", relid);
        }
        let mut isnull = false;
        let reloptions = SysCacheGetAttr(RELOID, tuple,
            Anum_pg_class_reloptions as _, &mut isnull);
        if !isnull {
            let mut buf = StringInfoData { data: std::ptr::null_mut(), len: 0, maxlen: 0, cursor: 0 };
            initStringInfo(&mut buf);
            get_reloptions(&mut buf, reloptions);
            result = buf.data;
        }
        ReleaseSysCache(tuple);
        result
    }
}

/*
 * get_range_partbound_string
 *      A C string representation of one range partition bound
 */
pub fn get_range_partbound_string(bound_datums: *mut List) -> *mut ::std::os::raw::c_char {
    unsafe {
        let mut context: deparse_context = std::mem::zeroed();
        let buf = makeStringInfo();
        context.buf = buf;

        appendStringInfoChar(buf, b'(' as _);
        let mut sep: *const ::std::os::raw::c_char = b"\0".as_ptr() as _;
        let mut cell = list_head(bound_datums);
        while !cell.is_null() {
            let datum = crate::current_cell!(cell) as *mut PartitionRangeDatum;
            appendStringInfoString(buf, sep);
            if (*datum).kind == PARTITION_RANGE_DATUM_MINVALUE {
                appendStringInfoString(buf, b"MINVALUE\0".as_ptr() as _);
            } else if (*datum).kind == PARTITION_RANGE_DATUM_MAXVALUE {
                appendStringInfoString(buf, b"MAXVALUE\0".as_ptr() as _);
            } else {
                let val = (*datum).value as *mut Const;
                get_const_expr(val, &mut context, -1);
            }
            sep = b", \0".as_ptr() as _;
            cell = lnext!(bound_datums, cell);
        }
        appendStringInfoChar(buf, b')' as _);

        (*buf).data
    }
}
