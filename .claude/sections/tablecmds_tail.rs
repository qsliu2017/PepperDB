// section: tablecmds_tail -- C lines 14726-22113 (ATExecAlterColumnType ... GetAttributeStorage)

// ---------------------------------------------------------------------------
// ATExecAlterColumnType
// ---------------------------------------------------------------------------

pub unsafe fn ATExecAlterColumnType(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let col_name: *mut libc::c_char = (*cmd).name;
    let def: *mut ColumnDef = (*cmd).def as *mut ColumnDef;
    let type_name: *mut TypeName = (*def).typeName;
    let mut heap_tup: HeapTuple;
    let att_tup: Form_pg_attribute;
    let att_old_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let type_tuple: HeapTuple;
    let tform: Form_pg_type;
    let targettype: Oid;
    let mut targettypmod: i32 = 0;
    let targetcollid: Oid;
    let defaultexpr: *mut Node;
    let attrelation: Relation;
    let dep_rel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut dep_tup: HeapTuple;
    let address: ObjectAddress;

    /*
     * Clear all the missing values if we're rewriting the table, since this
     * renders them pointless.
     */
    if (*tab).rewrite != 0 {
        let newrel: Relation = table_open(RelationGetRelid(rel), NoLock);
        RelationClearMissing(newrel);
        relation_close(newrel, NoLock);
        /* make sure we don't conflict with later attribute modifications */
        CommandCounterIncrement();
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    /* Look up the target column */
    heap_tup = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(heap_tup) {
        /* shouldn't happen */
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }
    att_tup = GETSTRUCT(heap_tup) as Form_pg_attribute;
    attnum = (*att_tup).attnum;
    att_old_tup = TupleDescAttr((*tab).oldDesc, (attnum - 1) as usize) as Form_pg_attribute;

    /* Check for multiple ALTER TYPE on same column --- can't cope */
    if (*att_tup).atttypid != (*att_old_tup).atttypid
        || (*att_tup).atttypmod != (*att_old_tup).atttypmod
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter type of column \"{}\" twice",
                CStr::from_ptr(col_name).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Look up the target type (should not fail, since prep found it) */
    type_tuple = typenameType(core::ptr::null_mut(), type_name, &mut targettypmod);
    tform = GETSTRUCT(type_tuple) as Form_pg_type;
    targettype = (*tform).oid;
    /* And the collation */
    targetcollid = GetColumnDefCollation(core::ptr::null_mut(), def, targettype);

    /*
     * If there is a default expression for the column, get it and ensure we
     * can coerce it to the new datatype.  (We must do this before changing
     * the column type, because build_column_default itself will try to
     * coerce, and will not issue the error message we want if it fails.)
     *
     * We remove any implicit coercion steps at the top level of the old
     * default expression; this has been agreed to satisfy the principle of
     * least surprise.
     */
    if (*att_tup).atthasdef {
        let mut dexpr: *mut Node = build_column_default(rel, attnum);
        Assert!(!dexpr.is_null());
        dexpr = strip_implicit_coercions(dexpr);
        dexpr = coerce_to_target_type(
            core::ptr::null_mut(), /* no UNKNOWN params */
            dexpr,
            exprType(dexpr),
            targettype,
            targettypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if dexpr.is_null() {
            if (*att_tup).attgenerated != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "generation expression for column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "default for column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
        }
        defaultexpr = dexpr;
    } else {
        defaultexpr = core::ptr::null_mut();
    }

    /*
     * Find everything that depends on the column (constraints, indexes, etc),
     * and record enough information to let us recreate the objects.
     */
    RememberAllDependentForRebuilding(tab, AT_AlterColumnType, rel, attnum, col_name);

    /*
     * Now scan for dependencies of this column on other things. The only
     * things we should find are the dependency on the column datatype and
     * possibly a collation dependency. Those can be removed.
     */
    dep_rel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(attnum as i32),
    );

    scan = systable_beginscan(dep_rel, DependDependerIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    loop {
        dep_tup = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tup) {
            break;
        }
        let found_dep: Form_pg_depend = GETSTRUCT(dep_tup) as Form_pg_depend;
        let mut found_object: ObjectAddress = core::mem::zeroed();

        found_object.classId = (*found_dep).refclassid;
        found_object.objectId = (*found_dep).refobjid;
        found_object.objectSubId = (*found_dep).refobjsubid;

        if (*found_dep).deptype != DEPENDENCY_NORMAL as libc::c_char {
            elog!(ERROR, "found unexpected dependency type '{}'", (*found_dep).deptype as u8 as char);
        }
        if !((*found_dep).refclassid == TypeRelationId
            && (*found_dep).refobjid == (*att_tup).atttypid)
            && !((*found_dep).refclassid == CollationRelationId
                && (*found_dep).refobjid == (*att_tup).attcollation)
        {
            elog!(
                ERROR,
                "found unexpected dependency for column: {}",
                CStr::from_ptr(getObjectDescription(&found_object, false)).to_string_lossy()
            );
        }

        CatalogTupleDelete(dep_rel, &(*dep_tup).t_self);
    }

    systable_endscan(scan);
    table_close(dep_rel, RowExclusiveLock);

    /*
     * Here we go --- change the recorded column type and collation.
     * First fix up the missing value if any.
     */
    if (*att_tup).atthasmissing {
        let mut missing_val: Datum;
        let mut missing_null: bool = false;

        /* if rewrite is true the missing value should already be cleared */
        Assert!((*tab).rewrite == 0);

        /* Get the missing value datum */
        missing_val = heap_getattr(
            heap_tup,
            Anum_pg_attribute_attmissingval,
            (*attrelation).rd_att,
            &mut missing_null,
        );

        /* if it's a null array there is nothing to do */
        if !missing_null {
            /*
             * Get the datum out of the array and repack it in a new array
             * built with the new type data.
             */
            let one: i32 = 1;
            let mut is_null: bool = false;
            let mut values_att: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
            let mut nulls_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
            let mut replaces_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
            let new_tup: HeapTuple;

            missing_val = array_get_element(
                missing_val,
                1,
                &one,
                0,
                (*att_tup).attlen,
                (*att_tup).attbyval,
                (*att_tup).attalign,
                &mut is_null,
            );
            missing_val = PointerGetDatum(construct_array(
                &mut missing_val,
                1,
                targettype,
                (*tform).typlen,
                (*tform).typbyval,
                (*tform).typalign,
            ));

            values_att[Anum_pg_attribute_attmissingval - 1] = missing_val;
            replaces_att[Anum_pg_attribute_attmissingval - 1] = true;
            nulls_att[Anum_pg_attribute_attmissingval - 1] = false;

            new_tup = heap_modify_tuple(
                heap_tup,
                RelationGetDescr(attrelation),
                values_att.as_mut_ptr(),
                nulls_att.as_mut_ptr(),
                replaces_att.as_mut_ptr(),
            );
            heap_freetuple(heap_tup);
            heap_tup = new_tup;
            // re-fetch att_tup after tuple replacement
            let att_tup = GETSTRUCT(heap_tup) as Form_pg_attribute;
            let _ = att_tup; // used below via heap_tup
        }
    }

    // re-borrow att_tup for mutation
    let att_tup_mut = GETSTRUCT(heap_tup) as Form_pg_attribute;
    (*att_tup_mut).atttypid = targettype;
    (*att_tup_mut).atttypmod = targettypmod;
    (*att_tup_mut).attcollation = targetcollid;
    if list_length((*type_name).arrayBounds) > libc::INT16_MAX as i32 {
        ereport!(
            ERROR,
            errmsg!("too many array dimensions") /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
        );
    }
    (*att_tup_mut).attndims = list_length((*type_name).arrayBounds) as i16;
    (*att_tup_mut).attlen = (*tform).typlen;
    (*att_tup_mut).attbyval = (*tform).typbyval;
    (*att_tup_mut).attalign = (*tform).typalign;
    (*att_tup_mut).attstorage = (*tform).typstorage;
    (*att_tup_mut).attcompression = InvalidCompressionMethod;

    ReleaseSysCache(type_tuple);

    CatalogTupleUpdate(attrelation, &(*heap_tup).t_self, heap_tup);

    table_close(attrelation, RowExclusiveLock);

    /* Install dependencies on new datatype and collation */
    add_column_datatype_dependency(RelationGetRelid(rel), attnum, targettype);
    add_column_collation_dependency(RelationGetRelid(rel), attnum, targetcollid);

    /*
     * Drop any pg_statistic entry for the column, since it's now wrong type
     */
    RemoveStatistics(RelationGetRelid(rel), attnum);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /*
     * Update the default, if present, by brute force --- remove and re-add
     * the default.
     */
    if !defaultexpr.is_null() {
        /*
         * If it's a GENERATED default, drop its dependency records, in
         * particular its INTERNAL dependency on the column, which would
         * otherwise cause dependency.c to refuse to perform the deletion.
         */
        let att_tup_cur = GETSTRUCT(heap_tup) as Form_pg_attribute;
        if (*att_tup_cur).attgenerated != 0 {
            let attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
            if !OidIsValid(attrdefoid) {
                elog!(
                    ERROR,
                    "could not find attrdef tuple for relation {} attnum {}",
                    RelationGetRelid(rel),
                    attnum
                );
            }
            let _ = deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);
        }

        /*
         * Make updates-so-far visible, particularly the new pg_attribute row
         * which will be updated again.
         */
        CommandCounterIncrement();

        /*
         * We use RESTRICT here for safety, but at present we do not expect
         * anything to depend on the default.
         */
        RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, true, true);

        let _ = StoreAttrDefault(rel, attnum, defaultexpr, true);
    }

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);

    /* Cleanup */
    heap_freetuple(heap_tup);

    address
}

// ---------------------------------------------------------------------------
// RememberAllDependentForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType and ATExecSetExpression: Find everything
/// that depends on the column (constraints, indexes, etc), and record enough
/// information to let us recreate the objects.
unsafe fn RememberAllDependentForRebuilding(
    tab: *mut AlteredTableInfo,
    subtype: AlterTableType,
    rel: Relation,
    attnum: AttrNumber,
    col_name: *const libc::c_char,
) {
    let dep_rel: Relation;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut dep_tup: HeapTuple;

    Assert!(subtype == AT_AlterColumnType || subtype == AT_SetExpression);

    dep_rel = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_refobjsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(attnum as i32),
    );

    scan = systable_beginscan(dep_rel, DependReferenceIndexId, true, core::ptr::null_mut(), 3, key.as_mut_ptr());

    loop {
        dep_tup = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tup) {
            break;
        }
        let found_dep: Form_pg_depend = GETSTRUCT(dep_tup) as Form_pg_depend;
        let mut found_object: ObjectAddress = core::mem::zeroed();

        found_object.classId = (*found_dep).classid;
        found_object.objectId = (*found_dep).objid;
        found_object.objectSubId = (*found_dep).objsubid;

        match found_object.classId {
            RelationRelationId => {
                let rel_kind: libc::c_char = get_rel_relkind(found_object.objectId);
                if rel_kind == RELKIND_INDEX as libc::c_char
                    || rel_kind == RELKIND_PARTITIONED_INDEX as libc::c_char
                {
                    Assert!(found_object.objectSubId == 0);
                    RememberIndexForRebuilding(found_object.objectId, tab);
                } else if rel_kind == RELKIND_SEQUENCE as libc::c_char {
                    /*
                     * This must be a SERIAL column's sequence. We need
                     * not do anything to it.
                     */
                    Assert!(found_object.objectSubId == 0);
                } else {
                    /* Not expecting any other direct dependencies... */
                    elog!(
                        ERROR,
                        "unexpected object depending on column: {}",
                        CStr::from_ptr(getObjectDescription(&found_object, false))
                            .to_string_lossy()
                    );
                }
            }
            ConstraintRelationId => {
                Assert!(found_object.objectSubId == 0);
                RememberConstraintForRebuilding(found_object.objectId, tab);
            }
            ProcedureRelationId => {
                /*
                 * A new-style SQL function can depend on a column, if that
                 * column is referenced in the parsed function body. FIXME someday.
                 *
                 * This is only a problem for AT_AlterColumnType, not AT_SetExpression.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!("cannot alter type of a column used by a function or procedure")
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail("%s depends on column \"%s\"", ...) */
                    );
                }
            }
            RewriteRelationId => {
                /*
                 * View/rule bodies have pretty much the same issues as
                 * function bodies. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!("cannot alter type of a column used by a view or rule")
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            TriggerRelationId => {
                /*
                 * A trigger can depend on a column because the column is
                 * specified as an update target, or because the column is
                 * used in the trigger's WHEN condition. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used in a trigger definition"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            PolicyRelationId => {
                /*
                 * A policy can depend on a column because the column is
                 * specified in the policy's USING or WITH CHECK qual
                 * expressions. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used in a policy definition"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            AttrDefaultRelationId => {
                let col: ObjectAddress = GetAttrDefaultColumnAddress(found_object.objectId);
                if col.objectId == RelationGetRelid(rel)
                    && col.objectSubId == attnum as i32
                {
                    /*
                     * Ignore the column's own default expression. The
                     * caller deals with it.
                     */
                } else {
                    /*
                     * This must be a reference from the expression of a
                     * generated column elsewhere in the same table.
                     * Changing the type/generated expression of a column
                     * that is used by a generated column is not allowed
                     * by SQL standard, so just punt for now.
                     */
                    if subtype == AT_AlterColumnType {
                        ereport!(
                            ERROR,
                            errmsg!("cannot alter type of a column used by a generated column")
                            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errdetail("Column \"%s\" is used by generated column \"%s\".", ...) */
                        );
                    }
                }
            }
            StatisticExtRelationId => {
                /*
                 * Give the extended-stats machinery a chance to fix anything
                 * that this column type change would break.
                 */
                RememberStatisticsForRebuilding(found_object.objectId, tab);
            }
            PublicationRelRelationId => {
                /*
                 * Column reference in a PUBLICATION ... FOR TABLE ... WHERE
                 * clause. FIXME someday.
                 */
                if subtype == AT_AlterColumnType {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot alter type of a column used by a publication WHERE clause"
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    );
                }
            }
            _ => {
                /*
                 * We don't expect any other sorts of objects to depend on a
                 * column.
                 */
                elog!(
                    ERROR,
                    "unexpected object depending on column: {}",
                    CStr::from_ptr(getObjectDescription(&found_object, false)).to_string_lossy()
                );
            }
        }
    }

    systable_endscan(scan);
    table_close(dep_rel, NoLock);
}

// ---------------------------------------------------------------------------
// RememberReplicaIdentityForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a replica identity
/// needs to be reset.
unsafe fn RememberReplicaIdentityForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    if !get_index_isreplident(indoid) {
        return;
    }
    if !(*tab).replicaIdentityIndex.is_null() {
        elog!(
            ERROR,
            "relation {} has multiple indexes marked as replica identity",
            (*tab).relid
        );
    }
    (*tab).replicaIdentityIndex = get_rel_name(indoid);
}

// ---------------------------------------------------------------------------
// RememberClusterOnForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember any clustered index.
unsafe fn RememberClusterOnForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    if !get_index_isclustered(indoid) {
        return;
    }
    if !(*tab).clusterOnIndex.is_null() {
        elog!(ERROR, "relation {} has multiple clustered indexes", (*tab).relid);
    }
    (*tab).clusterOnIndex = get_rel_name(indoid);
}

// ---------------------------------------------------------------------------
// RememberConstraintForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a constraint needs
/// to be rebuilt (which we might already know).
unsafe fn RememberConstraintForRebuilding(conoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same constraint twice, and if a constraint
     * depends on more than one column whose type is to be altered, we must
     * capture its definition string before applying any of the column type
     * changes. ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedConstraintOids, conoid) {
        /* OK, capture the constraint's existing definition string */
        let defstring: *mut libc::c_char = pg_get_constraintdef_command(conoid);
        let indoid: Oid;

        /*
         * It is critical to create not-null constraints ahead of primary key
         * indexes; otherwise, the not-null constraint would be created by the
         * primary key, and the constraint name would be wrong.
         */
        if get_constraint_type(conoid) == CONSTRAINT_NOTNULL as libc::c_char {
            (*tab).changedConstraintOids =
                lcons_oid(conoid, (*tab).changedConstraintOids);
            (*tab).changedConstraintDefs =
                lcons(defstring as *mut libc::c_void, (*tab).changedConstraintDefs);
        } else {
            (*tab).changedConstraintOids =
                lappend_oid((*tab).changedConstraintOids, conoid);
            (*tab).changedConstraintDefs =
                lappend((*tab).changedConstraintDefs, defstring as *mut libc::c_void);
        }

        /*
         * For the index of a constraint, if any, remember if it is used for
         * the table's replica identity or if it is a clustered index.
         */
        indoid = get_constraint_index(conoid);
        if OidIsValid(indoid) {
            RememberReplicaIdentityForRebuilding(indoid, tab);
            RememberClusterOnForRebuilding(indoid, tab);
        }
    }
}

// ---------------------------------------------------------------------------
// RememberIndexForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that an index needs
/// to be rebuilt (which we might already know).
unsafe fn RememberIndexForRebuilding(indoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same index twice, and if an index depends
     * on more than one column whose type is to be altered, we must capture
     * its definition string before applying any of the column type changes.
     * ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedIndexOids, indoid) {
        /*
         * Before adding it as an index-to-rebuild, we'd better see if it
         * belongs to a constraint, and if so rebuild the constraint instead.
         */
        let conoid: Oid = get_index_constraint(indoid);
        if OidIsValid(conoid) {
            RememberConstraintForRebuilding(conoid, tab);
        } else {
            /* OK, capture the index's existing definition string */
            let defstring: *mut libc::c_char = pg_get_indexdef_string(indoid);

            (*tab).changedIndexOids = lappend_oid((*tab).changedIndexOids, indoid);
            (*tab).changedIndexDefs =
                lappend((*tab).changedIndexDefs, defstring as *mut libc::c_void);

            /*
             * Remember if this index is used for the table's replica identity
             * or if it is a clustered index.
             */
            RememberReplicaIdentityForRebuilding(indoid, tab);
            RememberClusterOnForRebuilding(indoid, tab);
        }
    }
}

// ---------------------------------------------------------------------------
// RememberStatisticsForRebuilding
// ---------------------------------------------------------------------------

/// Subroutine for ATExecAlterColumnType: remember that a statistics object
/// needs to be rebuilt (which we might already know).
unsafe fn RememberStatisticsForRebuilding(stxoid: Oid, tab: *mut AlteredTableInfo) {
    /*
     * This de-duplication check is critical for two independent reasons: we
     * mustn't try to recreate the same statistics object twice, and if the
     * statistics object depends on more than one column whose type is to be
     * altered, we must capture its definition string before applying any of
     * the type changes. ruleutils.c will get confused if we ask again later.
     */
    if !list_member_oid((*tab).changedStatisticsOids, stxoid) {
        /* OK, capture the statistics object's existing definition string */
        let defstring: *mut libc::c_char = pg_get_statisticsobjdef_string(stxoid);

        (*tab).changedStatisticsOids =
            lappend_oid((*tab).changedStatisticsOids, stxoid);
        (*tab).changedStatisticsDefs =
            lappend((*tab).changedStatisticsDefs, defstring as *mut libc::c_void);
    }
}

// ---------------------------------------------------------------------------
// ATPostAlterTypeCleanup
// ---------------------------------------------------------------------------

/// Cleanup after we've finished all the ALTER TYPE or SET EXPRESSION
/// operations for a particular relation. We have to drop and recreate all the
/// indexes and constraints that depend on the altered columns.
unsafe fn ATPostAlterTypeCleanup(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    lockmode: LOCKMODE,
) {
    let mut obj: ObjectAddress = core::mem::zeroed();
    let objects: *mut ObjectAddresses = new_object_addresses();
    let mut def_item: *mut ListCell;
    let mut oid_item: *mut ListCell;

    /*
     * Collect all the constraints and indexes to drop so we can process them
     * in a single call. That way we don't have to worry about dependencies
     * among them.
     */

    /*
     * Re-parse the index and constraint definitions, and attach them to the
     * appropriate work queue entries.
     */
    // forboth over changedConstraintOids / changedConstraintDefs
    oid_item = list_head((*tab).changedConstraintOids);
    def_item = list_head((*tab).changedConstraintDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let tup: HeapTuple;
        let con: Form_pg_constraint;
        let relid: Oid;
        let confrelid: Oid;
        let conislocal: bool;

        tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(old_id));
        if !HeapTupleIsValid(tup) {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for constraint {}", old_id);
        }
        con = GETSTRUCT(tup) as Form_pg_constraint;
        if OidIsValid((*con).conrelid) {
            relid = (*con).conrelid;
        } else {
            /* must be a domain constraint */
            relid = get_typ_typrelid(getBaseType((*con).contypid));
            if !OidIsValid(relid) {
                elog!(
                    ERROR,
                    "could not identify relation associated with constraint {}",
                    old_id
                );
            }
        }
        confrelid = (*con).confrelid;
        conislocal = (*con).conislocal;
        ReleaseSysCache(tup);

        ObjectAddressSet!(obj, ConstraintRelationId, old_id);
        add_exact_object_address(&obj, objects);

        /*
         * If the constraint is inherited (only), we don't want to inject a
         * new definition here; it'll get recreated when
         * ATAddCheckNNConstraint recurses from adding the parent table's
         * constraint. But we had to carry the info this far so that we can
         * drop the constraint below.
         */
        if !conislocal {
            oid_item = lnext((*tab).changedConstraintOids, oid_item);
            def_item = lnext((*tab).changedConstraintDefs, def_item);
            continue;
        }

        /*
         * When rebuilding another table's constraint that references the
         * table we're modifying, we might not yet have any lock on the other
         * table, so get one now.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, AccessExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            confrelid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        oid_item = lnext((*tab).changedConstraintOids, oid_item);
        def_item = lnext((*tab).changedConstraintDefs, def_item);
    }

    oid_item = list_head((*tab).changedIndexOids);
    def_item = list_head((*tab).changedIndexDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let relid: Oid = IndexGetRelation(old_id, false);

        /*
         * As above, make sure we have lock on the index's table if it's not
         * the same table.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, AccessExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            InvalidOid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        ObjectAddressSet!(obj, RelationRelationId, old_id);
        add_exact_object_address(&obj, objects);

        oid_item = lnext((*tab).changedIndexOids, oid_item);
        def_item = lnext((*tab).changedIndexDefs, def_item);
    }

    /* add dependencies for new statistics */
    oid_item = list_head((*tab).changedStatisticsOids);
    def_item = list_head((*tab).changedStatisticsDefs);
    while !oid_item.is_null() {
        let old_id: Oid = lfirst_oid(oid_item);
        let relid: Oid = StatisticsGetRelation(old_id, false);

        /*
         * As above, make sure we have lock on the statistics object's table
         * if it's not the same table. However, we take
         * ShareUpdateExclusiveLock here.
         *
         * CAUTION: this should be done after all cases that grab
         * AccessExclusiveLock.
         */
        if relid != (*tab).relid {
            LockRelationOid(relid, ShareUpdateExclusiveLock);
        }

        ATPostAlterTypeParse(
            old_id,
            relid,
            InvalidOid,
            lfirst(def_item) as *mut libc::c_char,
            wqueue,
            lockmode,
            (*tab).rewrite != 0,
        );

        ObjectAddressSet!(obj, StatisticExtRelationId, old_id);
        add_exact_object_address(&obj, objects);

        oid_item = lnext((*tab).changedStatisticsOids, oid_item);
        def_item = lnext((*tab).changedStatisticsDefs, def_item);
    }

    /*
     * Queue up command to restore replica identity index marking
     */
    if !(*tab).replicaIdentityIndex.is_null() {
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        let subcmd: *mut ReplicaIdentityStmt = makeNode!(ReplicaIdentityStmt, T_ReplicaIdentityStmt);

        (*subcmd).identity_type = REPLICA_IDENTITY_INDEX;
        (*subcmd).name = (*tab).replicaIdentityIndex;
        (*cmd).subtype = AT_ReplicaIdentity;
        (*cmd).def = subcmd as *mut Node;

        /* do it after indexes and constraints */
        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] =
            lappend((*tab).subcmds[AT_PASS_OLD_CONSTR as usize], cmd as *mut libc::c_void);
    }

    /*
     * Queue up command to restore marking of index used for cluster.
     */
    if !(*tab).clusterOnIndex.is_null() {
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

        (*cmd).subtype = AT_ClusterOn;
        (*cmd).name = (*tab).clusterOnIndex;

        /* do it after indexes and constraints */
        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] =
            lappend((*tab).subcmds[AT_PASS_OLD_CONSTR as usize], cmd as *mut libc::c_void);
    }

    /*
     * It should be okay to use DROP_RESTRICT here, since nothing else should
     * be depending on these objects.
     */
    performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    free_object_addresses(objects);

    /*
     * The objects will get recreated during subsequent passes over the work
     * queue.
     */
}

// ---------------------------------------------------------------------------
// ATPostAlterTypeParse
// ---------------------------------------------------------------------------

/// Parse the previously-saved definition string for a constraint, index or
/// statistics object against the newly-established column data type(s), and
/// queue up the resulting command parsetrees for execution.
unsafe fn ATPostAlterTypeParse(
    old_id: Oid,
    old_rel_id: Oid,
    ref_rel_id: Oid,
    cmd: *mut libc::c_char,
    wqueue: *mut *mut List,
    lockmode: LOCKMODE,
    rewrite: bool,
) {
    let raw_parsetree_list: *mut List;
    let mut querytree_list: *mut List = NIL;
    let mut list_item: *mut ListCell;
    let rel: Relation;

    /*
     * We expect that we will get only ALTER TABLE and CREATE INDEX
     * statements. Hence, there is no need to pass them through
     * parse_analyze_*() or the rewriter, but instead we need to pass them
     * through parse_utilcmd.c to make them ready for execution.
     */
    raw_parsetree_list = raw_parser(cmd, RAW_PARSE_DEFAULT);
    querytree_list = NIL;
    list_item = list_head(raw_parsetree_list);
    while !list_item.is_null() {
        let rs: *mut RawStmt = lfirst_node!(RawStmt, T_RawStmt, list_item);
        let stmt: *mut Node = (*rs).stmt;

        if IsA!(stmt, T_IndexStmt) {
            querytree_list = lappend(
                querytree_list,
                transformIndexStmt(old_rel_id, stmt as *mut IndexStmt, cmd) as *mut libc::c_void,
            );
        } else if IsA!(stmt, T_AlterTableStmt) {
            let mut before_stmts: *mut List = core::ptr::null_mut();
            let mut after_stmts: *mut List = core::ptr::null_mut();

            let transformed = transformAlterTableStmt(
                old_rel_id,
                stmt as *mut AlterTableStmt,
                cmd,
                &mut before_stmts,
                &mut after_stmts,
            ) as *mut Node;
            querytree_list = list_concat(querytree_list, before_stmts);
            querytree_list = lappend(querytree_list, transformed as *mut libc::c_void);
            querytree_list = list_concat(querytree_list, after_stmts);
        } else if IsA!(stmt, T_CreateStatsStmt) {
            querytree_list = lappend(
                querytree_list,
                transformStatsStmt(old_rel_id, stmt as *mut CreateStatsStmt, cmd)
                    as *mut libc::c_void,
            );
        } else {
            querytree_list = lappend(querytree_list, stmt as *mut libc::c_void);
        }

        list_item = lnext(raw_parsetree_list, list_item);
    }

    /* Caller should already have acquired whatever lock we need. */
    rel = relation_open(old_rel_id, NoLock);

    /*
     * Attach each generated command to the proper place in the work queue.
     * Note this could result in creation of entirely new work-queue entries.
     *
     * Also note that we have to tweak the command subtypes, because it turns
     * out that re-creation of indexes and constraints has to act a bit
     * differently from initial creation.
     */
    list_item = list_head(querytree_list);
    while !list_item.is_null() {
        let stm: *mut Node = lfirst(list_item) as *mut Node;
        let tab: *mut AlteredTableInfo = ATGetQueueEntry(wqueue, rel);

        if IsA!(stm, T_IndexStmt) {
            let stmt: *mut IndexStmt = stm as *mut IndexStmt;
            let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

            if !rewrite {
                TryReuseIndex(old_id, stmt);
            }
            (*stmt).reset_default_tblspc = true;
            /* keep the index's comment */
            (*stmt).idxcomment = GetComment(old_id, RelationRelationId, 0);

            (*newcmd).subtype = AT_ReAddIndex;
            (*newcmd).def = stmt as *mut Node;
            (*tab).subcmds[AT_PASS_OLD_INDEX as usize] = lappend(
                (*tab).subcmds[AT_PASS_OLD_INDEX as usize],
                newcmd as *mut libc::c_void,
            );
        } else if IsA!(stm, T_AlterTableStmt) {
            let stmt: *mut AlterTableStmt = stm as *mut AlterTableStmt;
            let mut lcmd: *mut ListCell = list_head((*stmt).cmds);
            while !lcmd.is_null() {
                let acmd: *mut AlterTableCmd =
                    lfirst_node!(AlterTableCmd, T_AlterTableCmd, lcmd);

                if (*acmd).subtype == AT_AddIndex {
                    let indstmt: *mut IndexStmt =
                        castNode!(IndexStmt, T_IndexStmt, (*acmd).def);
                    let indoid: Oid = get_constraint_index(old_id);

                    if !rewrite {
                        TryReuseIndex(indoid, indstmt);
                    }
                    /* keep any comment on the index */
                    (*indstmt).idxcomment = GetComment(indoid, RelationRelationId, 0);
                    (*indstmt).reset_default_tblspc = true;

                    (*acmd).subtype = AT_ReAddIndex;
                    (*tab).subcmds[AT_PASS_OLD_INDEX as usize] = lappend(
                        (*tab).subcmds[AT_PASS_OLD_INDEX as usize],
                        acmd as *mut libc::c_void,
                    );

                    /* recreate any comment on the constraint */
                    RebuildConstraintComment(
                        tab,
                        AT_PASS_OLD_INDEX,
                        old_id,
                        rel,
                        NIL,
                        (*indstmt).idxname,
                    );
                } else if (*acmd).subtype == AT_AddConstraint {
                    let con: *mut Constraint =
                        castNode!(Constraint, T_Constraint, (*acmd).def);

                    (*con).old_pktable_oid = ref_rel_id;
                    /* rewriting neither side of a FK */
                    if (*con).contype == CONSTR_FOREIGN
                        && !rewrite
                        && (*tab).rewrite == 0
                    {
                        TryReuseForeignKey(old_id, con);
                    }
                    (*con).reset_default_tblspc = true;
                    (*acmd).subtype = AT_ReAddConstraint;
                    (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] = lappend(
                        (*tab).subcmds[AT_PASS_OLD_CONSTR as usize],
                        acmd as *mut libc::c_void,
                    );

                    /*
                     * Recreate any comment on the constraint. If we have
                     * recreated a primary key, then transformTableConstraint
                     * has added an unnamed not-null constraint here; skip
                     * this in that case.
                     */
                    if !(*con).conname.is_null() {
                        RebuildConstraintComment(
                            tab,
                            AT_PASS_OLD_CONSTR,
                            old_id,
                            rel,
                            NIL,
                            (*con).conname,
                        );
                    } else {
                        Assert!((*con).contype == CONSTR_NOTNULL);
                    }
                } else {
                    elog!(
                        ERROR,
                        "unexpected statement subtype: {}",
                        (*acmd).subtype as i32
                    );
                }

                lcmd = lnext((*stmt).cmds, lcmd);
            }
        } else if IsA!(stm, T_AlterDomainStmt) {
            let stmt: *mut AlterDomainStmt = stm as *mut AlterDomainStmt;

            if (*stmt).subtype == b'C' as libc::c_char {
                /* ADD CONSTRAINT */
                let con: *mut Constraint =
                    castNode!(Constraint, T_Constraint, (*stmt).def);
                let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

                (*newcmd).subtype = AT_ReAddDomainConstraint;
                (*newcmd).def = stmt as *mut Node;
                (*tab).subcmds[AT_PASS_OLD_CONSTR as usize] = lappend(
                    (*tab).subcmds[AT_PASS_OLD_CONSTR as usize],
                    newcmd as *mut libc::c_void,
                );

                /* recreate any comment on the constraint */
                RebuildConstraintComment(
                    tab,
                    AT_PASS_OLD_CONSTR,
                    old_id,
                    core::ptr::null_mut(),
                    (*stmt).typeName,
                    (*con).conname,
                );
            } else {
                elog!(ERROR, "unexpected statement subtype: {}", (*stmt).subtype as i32);
            }
        } else if IsA!(stm, T_CreateStatsStmt) {
            let stmt: *mut CreateStatsStmt = stm as *mut CreateStatsStmt;
            let newcmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

            /* keep the statistics object's comment */
            (*stmt).stxcomment = GetComment(old_id, StatisticExtRelationId, 0);

            (*newcmd).subtype = AT_ReAddStatistics;
            (*newcmd).def = stmt as *mut Node;
            (*tab).subcmds[AT_PASS_MISC as usize] = lappend(
                (*tab).subcmds[AT_PASS_MISC as usize],
                newcmd as *mut libc::c_void,
            );
        } else {
            elog!(ERROR, "unexpected statement type: {}", nodeTag(stm) as i32);
        }

        list_item = lnext(querytree_list, list_item);
    }

    relation_close(rel, NoLock);
}

// ---------------------------------------------------------------------------
// RebuildConstraintComment
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse() to recreate any existing comment
/// for a table or domain constraint that is being rebuilt.
///
/// objid is the OID of the constraint.
/// Pass "rel" for a table constraint, or "domname" (domain's qualified name
/// as a string list) for a domain constraint.
unsafe fn RebuildConstraintComment(
    tab: *mut AlteredTableInfo,
    pass: AlterTablePass,
    objid: Oid,
    rel: Relation,
    domname: *mut List,
    conname: *const libc::c_char,
) {
    let cmd: *mut CommentStmt;
    let comment_str: *mut libc::c_char;
    let newcmd: *mut AlterTableCmd;

    /* Look for comment for object wanted, and leave if none */
    comment_str = GetComment(objid, ConstraintRelationId, 0);
    if comment_str.is_null() {
        return;
    }

    /* Build CommentStmt node, copying all input data for safety */
    cmd = makeNode!(CommentStmt, T_CommentStmt);
    if !rel.is_null() {
        (*cmd).objtype = OBJECT_TABCONSTRAINT;
        (*cmd).object = list_make3(
            makeString(get_namespace_name(RelationGetNamespace(rel))),
            makeString(pstrdup(RelationGetRelationName(rel))),
            makeString(pstrdup(conname)),
        ) as *mut Node;
    } else {
        (*cmd).objtype = OBJECT_DOMCONSTRAINT;
        (*cmd).object = list_make2(
            makeTypeNameFromNameList(copyObject(domname)),
            makeString(pstrdup(conname)),
        ) as *mut Node;
    }
    (*cmd).comment = comment_str;

    /* Append it to list of commands */
    newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
    (*newcmd).subtype = AT_ReAddComment;
    (*newcmd).def = cmd as *mut Node;
    (*tab).subcmds[pass as usize] =
        lappend((*tab).subcmds[pass as usize], newcmd as *mut libc::c_void);
}

// ---------------------------------------------------------------------------
// TryReuseIndex
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse(). Calls out to CheckIndexCompatible()
/// for the real analysis, then mutates the IndexStmt based on that verdict.
unsafe fn TryReuseIndex(old_id: Oid, stmt: *mut IndexStmt) {
    if CheckIndexCompatible(
        old_id,
        (*stmt).accessMethod,
        (*stmt).indexParams,
        (*stmt).excludeOpNames,
        (*stmt).iswithoutoverlaps,
    ) {
        let irel: Relation = index_open(old_id, NoLock);
        /* If it's a partitioned index, there is no storage to share. */
        if (*(*irel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX as libc::c_char {
            (*stmt).oldNumber = (*irel).rd_locator.relNumber;
            (*stmt).oldCreateSubid = (*irel).rd_createSubid;
            (*stmt).oldFirstRelfilelocatorSubid = (*irel).rd_firstRelfilelocatorSubid;
        }
        index_close(irel, NoLock);
    }
}

// ---------------------------------------------------------------------------
// TryReuseForeignKey
// ---------------------------------------------------------------------------

/// Subroutine for ATPostAlterTypeParse().
///
/// Stash the old P-F equality operator into the Constraint node, for possible
/// use by ATAddForeignKeyConstraint() in determining whether revalidation of
/// this constraint can be skipped.
unsafe fn TryReuseForeignKey(old_id: Oid, con: *mut Constraint) {
    let tup: HeapTuple;
    let adatum: Datum;
    let arr: *mut ArrayType;
    let rawarr: *mut Oid;
    let numkeys: i32;

    Assert!((*con).contype == CONSTR_FOREIGN);
    Assert!((*con).old_conpfeqop == NIL); /* already prepared this node */

    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(old_id));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for constraint {}", old_id);
    }

    adatum = SysCacheGetAttrNotNull(CONSTROID, tup, Anum_pg_constraint_conpfeqop);
    arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
    numkeys = ARR_DIMS(arr)[0];
    /* test follows the one in ri_FetchConstraintInfo() */
    if ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID {
        elog!(ERROR, "conpfeqop is not a 1-D Oid array");
    }
    rawarr = ARR_DATA_PTR(arr) as *mut Oid;

    /* stash a List of the operator Oids in our Constraint node */
    for i in 0..numkeys as usize {
        (*con).old_conpfeqop = lappend_oid((*con).old_conpfeqop, *rawarr.add(i));
    }

    ReleaseSysCache(tup);
}

// ---------------------------------------------------------------------------
// ATExecAlterColumnGenericOptions
// ---------------------------------------------------------------------------

/// ALTER COLUMN .. OPTIONS ( ... )
///
/// Returns the address of the modified column
unsafe fn ATExecAlterColumnGenericOptions(
    rel: Relation,
    col_name: *const libc::c_char,
    options: *mut List,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let ftrel: Relation;
    let attrel: Relation;
    let server: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let mut tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut repl_null: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut repl_repl: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut datum: Datum;
    let fttableform: Form_pg_foreign_table;
    let atttableform: Form_pg_attribute;
    let attnum: AttrNumber;
    let address: ObjectAddress;

    if options == NIL {
        return InvalidObjectAddress;
    }

    /* First, determine FDW validator associated to the foreign table. */
    ftrel = table_open(ForeignTableRelationId, AccessShareLock);
    tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum((*rel).rd_id));
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "foreign table \"{}\" does not exist",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }
    fttableform = GETSTRUCT(tuple) as Form_pg_foreign_table;
    server = GetForeignServer((*fttableform).ftserver);
    fdw = GetForeignDataWrapper((*server).fdwid);

    table_close(ftrel, AccessShareLock);
    ReleaseSysCache(tuple);

    attrel = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }

    /* Prevent them from altering a system attribute */
    atttableform = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*atttableform).attnum;
    if attnum <= 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter system column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Initialize buffers for new tuple values */
    libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
    libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

    /* Extract the current options */
    datum = SysCacheGetAttr(
        ATTNAME,
        tuple,
        Anum_pg_attribute_attfdwoptions,
        &mut isnull,
    );
    if isnull {
        datum = PointerGetDatum(core::ptr::null::<libc::c_void>() as *mut libc::c_void);
    }

    /* Transform the options */
    datum = transformGenericOptions(
        AttributeRelationId,
        datum,
        options,
        (*fdw).fdwvalidator,
    );

    if PointerIsValid(DatumGetPointer(datum)) {
        repl_val[Anum_pg_attribute_attfdwoptions - 1] = datum;
    } else {
        repl_null[Anum_pg_attribute_attfdwoptions - 1] = true;
    }

    repl_repl[Anum_pg_attribute_attfdwoptions - 1] = true;

    /* Everything looks good - update the tuple */
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(attrel, &(*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(
        RelationRelationId,
        RelationGetRelid(rel),
        (*atttableform).attnum as i32,
    );
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);

    ReleaseSysCache(tuple);

    table_close(attrel, RowExclusiveLock);

    heap_freetuple(newtuple);

    address
}

// ---------------------------------------------------------------------------
// ATExecChangeOwner
// ---------------------------------------------------------------------------

/// ALTER TABLE OWNER
///
/// recursing is true if we are recursing from a table to its indexes,
/// sequences, or toast table. We don't allow the ownership of those things to
/// be changed separately from the parent table.
pub unsafe fn ATExecChangeOwner(
    relation_oid: Oid,
    new_owner_id: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let target_rel: Relation;
    let class_rel: Relation;
    let mut tuple: HeapTuple;
    let tuple_class: Form_pg_class;

    /*
     * Get exclusive lock till end of transaction on the target table. Use
     * relation_open so that we can work on indexes and sequences.
     */
    target_rel = relation_open(relation_oid, lockmode);

    /* Get its pg_class tuple, too */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relation_oid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relation_oid);
    }
    tuple_class = GETSTRUCT(tuple) as Form_pg_class;

    /* Can we change the ownership of this tuple? */
    let mut new_owner_id = new_owner_id;
    match (*tuple_class).relkind as u8 {
        RELKIND_RELATION
        | RELKIND_VIEW
        | RELKIND_MATVIEW
        | RELKIND_FOREIGN_TABLE
        | RELKIND_PARTITIONED_TABLE => {
            /* ok to change owner */
        }
        RELKIND_INDEX => {
            if !recursing {
                /*
                 * Because ALTER INDEX OWNER used to be allowed, and in fact
                 * is generated by old versions of pg_dump, we give a warning
                 * and do nothing rather than erroring out.
                 */
                if (*tuple_class).relowner != new_owner_id {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "cannot change owner of index \"{}\"",
                            CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errhint("Change the ownership of the index's table instead.") */
                    );
                }
                /* quick hack to exit via the no-op path */
                new_owner_id = (*tuple_class).relowner;
            }
        }
        RELKIND_PARTITIONED_INDEX => {
            if recursing {
                /* ok */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot change owner of index \"{}\"",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errhint("Change the ownership of the index's table instead.") */
                );
            }
        }
        RELKIND_SEQUENCE => {
            if !recursing && (*tuple_class).relowner != new_owner_id {
                /* if it's an owned sequence, disallow changing it by itself */
                let mut table_id: Oid = InvalidOid;
                let mut col_id: i32 = 0;

                if sequenceIsOwned(relation_oid, DEPENDENCY_AUTO, &mut table_id, &mut col_id)
                    || sequenceIsOwned(relation_oid, DEPENDENCY_INTERNAL, &mut table_id, &mut col_id)
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot change owner of sequence \"{}\"",
                            CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errdetail("Sequence \"%s\" is linked to table \"%s\".", ...) */
                    );
                }
            }
        }
        RELKIND_COMPOSITE_TYPE => {
            if recursing {
                /* ok */
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is a composite type",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errhint("Use %s instead.", "ALTER TYPE") */
                );
            }
        }
        RELKIND_TOASTVALUE => {
            if !recursing {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot change owner of relation \"{}\"",
                        CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                    )
                    /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errdetail_relkind_not_supported(tuple_class->relkind) */
                );
            }
            /* else: fall through - same as default for recursing toast */
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change owner of relation \"{}\"",
                    CStr::from_ptr(NameStr!((*tuple_class).relname)).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errdetail_relkind_not_supported(tuple_class->relkind) */
            );
        }
    }

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded. This is for dump restoration purposes.
     */
    if (*tuple_class).relowner != new_owner_id {
        let mut repl_val: [Datum; Natts_pg_class] = [0; Natts_pg_class];
        let mut repl_null: [bool; Natts_pg_class] = [false; Natts_pg_class];
        let mut repl_repl: [bool; Natts_pg_class] = [false; Natts_pg_class];
        let new_acl: *mut Acl;
        let mut acl_datum: Datum;
        let mut is_null: bool = false;
        let newtuple: HeapTuple;

        /* skip permission checks when recursing to index or toast table */
        if !recursing {
            /* Superusers can always do it */
            if !superuser() {
                let namespace_oid: Oid = (*tuple_class).relnamespace;
                let aclresult: AclResult;

                /* Otherwise, must be owner of the existing object */
                if !object_ownercheck(RelationRelationId, relation_oid, GetUserId()) {
                    aclcheck_error(
                        ACLCHECK_NOT_OWNER,
                        get_relkind_objtype(get_rel_relkind(relation_oid)),
                        RelationGetRelationName(target_rel),
                    );
                }

                /* Must be able to become new owner */
                check_can_set_role(GetUserId(), new_owner_id);

                /* New owner must have CREATE privilege on namespace */
                aclresult = object_aclcheck(
                    NamespaceRelationId,
                    namespace_oid,
                    new_owner_id,
                    ACL_CREATE,
                );
                if aclresult != ACLCHECK_OK {
                    aclcheck_error(
                        aclresult,
                        OBJECT_SCHEMA,
                        get_namespace_name(namespace_oid),
                    );
                }
            }
        }

        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        repl_repl[Anum_pg_class_relowner - 1] = true;
        repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(new_owner_id);

        /*
         * Determine the modified ACL for the new owner. This is only
         * necessary when the ACL is non-null.
         */
        acl_datum = SysCacheGetAttr(
            RELOID,
            tuple,
            Anum_pg_class_relacl,
            &mut is_null,
        );
        if !is_null {
            new_acl = aclnewowner(
                DatumGetAclP(acl_datum),
                (*tuple_class).relowner,
                new_owner_id,
            );
            repl_repl[Anum_pg_class_relacl - 1] = true;
            repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);
        }

        newtuple = heap_modify_tuple(
            tuple,
            RelationGetDescr(class_rel),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(class_rel, &(*newtuple).t_self, newtuple);

        heap_freetuple(newtuple);

        /*
         * We must similarly update any per-column ACLs to reflect the new
         * owner; for neatness reasons that's split out as a subroutine.
         */
        change_owner_fix_column_acls(relation_oid, (*tuple_class).relowner, new_owner_id);

        /*
         * Update owner dependency reference, if any.
         */
        if (*tuple_class).relkind as u8 != RELKIND_COMPOSITE_TYPE
            && (*tuple_class).relkind as u8 != RELKIND_INDEX
            && (*tuple_class).relkind as u8 != RELKIND_PARTITIONED_INDEX
            && (*tuple_class).relkind as u8 != RELKIND_TOASTVALUE
        {
            changeDependencyOnOwner(RelationRelationId, relation_oid, new_owner_id);
        }

        /*
         * Also change the ownership of the table's row type, if it has one
         */
        if OidIsValid((*tuple_class).reltype) {
            AlterTypeOwnerInternal((*tuple_class).reltype, new_owner_id);
        }

        /*
         * If we are operating on a table or materialized view, also change
         * the ownership of any indexes and sequences that belong to the
         * relation, as well as its toast table (if it has one).
         */
        if (*tuple_class).relkind as u8 == RELKIND_RELATION
            || (*tuple_class).relkind as u8 == RELKIND_PARTITIONED_TABLE
            || (*tuple_class).relkind as u8 == RELKIND_MATVIEW
            || (*tuple_class).relkind as u8 == RELKIND_TOASTVALUE
        {
            let index_oid_list: *mut List = RelationGetIndexList(target_rel);
            let mut i: *mut ListCell = list_head(index_oid_list);
            while !i.is_null() {
                ATExecChangeOwner(lfirst_oid(i), new_owner_id, true, lockmode);
                i = lnext(index_oid_list, i);
            }
            list_free(index_oid_list);
        }

        /* If it has a toast table, recurse to change its ownership */
        if (*tuple_class).reltoastrelid != InvalidOid {
            ATExecChangeOwner((*tuple_class).reltoastrelid, new_owner_id, true, lockmode);
        }

        /* If it has dependent sequences, recurse to change them too */
        change_owner_recurse_to_sequences(relation_oid, new_owner_id, lockmode);
    }

    InvokeObjectPostAlterHook(RelationRelationId, relation_oid, 0);

    ReleaseSysCache(tuple);
    table_close(class_rel, RowExclusiveLock);
    relation_close(target_rel, NoLock);
}

// ---------------------------------------------------------------------------
// change_owner_fix_column_acls
// ---------------------------------------------------------------------------

/// Helper function for ATExecChangeOwner. Scan the columns of the table
/// and fix any non-null column ACLs to reflect the new owner.
unsafe fn change_owner_fix_column_acls(
    relation_oid: Oid,
    old_owner_id: Oid,
    new_owner_id: Oid,
) {
    let att_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let mut attribute_tuple: HeapTuple;

    att_relation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relation_oid),
    );
    scan = systable_beginscan(
        att_relation,
        AttributeRelidNumIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );
    loop {
        attribute_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(attribute_tuple) {
            break;
        }
        let att = GETSTRUCT(attribute_tuple) as Form_pg_attribute;
        let mut repl_val: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
        let mut repl_null: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
        let mut repl_repl: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
        let new_acl: *mut Acl;
        let acl_datum: Datum;
        let mut is_null: bool = false;
        let newtuple: HeapTuple;

        /* Ignore dropped columns */
        if (*att).attisdropped {
            continue;
        }

        acl_datum = heap_getattr(
            attribute_tuple,
            Anum_pg_attribute_attacl,
            RelationGetDescr(att_relation),
            &mut is_null,
        );
        /* Null ACLs do not require changes */
        if is_null {
            continue;
        }

        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        new_acl = aclnewowner(DatumGetAclP(acl_datum), old_owner_id, new_owner_id);
        repl_repl[Anum_pg_attribute_attacl - 1] = true;
        repl_val[Anum_pg_attribute_attacl - 1] = PointerGetDatum(new_acl);

        newtuple = heap_modify_tuple(
            attribute_tuple,
            RelationGetDescr(att_relation),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(att_relation, &(*newtuple).t_self, newtuple);

        heap_freetuple(newtuple);
    }
    systable_endscan(scan);
    table_close(att_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// change_owner_recurse_to_sequences
// ---------------------------------------------------------------------------

/// Helper function for ATExecChangeOwner. Examines pg_depend searching
/// for sequences that are dependent on serial columns, and changes their
/// ownership.
unsafe fn change_owner_recurse_to_sequences(
    relation_oid: Oid,
    new_owner_id: Oid,
    lockmode: LOCKMODE,
) {
    let dep_rel: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;

    /*
     * SERIAL sequences are those having an auto dependency on one of the
     * table's columns (we don't care *which* column, exactly).
     */
    dep_rel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relation_oid),
    );
    /* we leave refobjsubid unspecified */

    scan = systable_beginscan(dep_rel, DependReferenceIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let dep_form: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;
        let seq_rel: Relation;

        /* skip dependencies other than auto dependencies on columns */
        if (*dep_form).refobjsubid == 0
            || (*dep_form).classid != RelationRelationId
            || (*dep_form).objsubid != 0
            || !((*dep_form).deptype == DEPENDENCY_AUTO as libc::c_char
                || (*dep_form).deptype == DEPENDENCY_INTERNAL as libc::c_char)
        {
            continue;
        }

        /* Use relation_open just in case it's an index */
        seq_rel = relation_open((*dep_form).objid, lockmode);

        /* skip non-sequence relations */
        if (*RelationGetForm(seq_rel)).relkind as u8 != RELKIND_SEQUENCE {
            /* No need to keep the lock */
            relation_close(seq_rel, lockmode);
            continue;
        }

        /* We don't need to close the sequence while we alter it. */
        ATExecChangeOwner((*dep_form).objid, new_owner_id, true, lockmode);

        /* Now we can close it. Keep the lock till end of transaction. */
        relation_close(seq_rel, NoLock);
    }

    systable_endscan(scan);

    relation_close(dep_rel, AccessShareLock);
}

// ---------------------------------------------------------------------------
// ATExecClusterOn
// ---------------------------------------------------------------------------

/// ALTER TABLE CLUSTER ON
///
/// The only thing we have to do is to change the indisclustered bits.
/// Return the address of the new clustering index.
unsafe fn ATExecClusterOn(
    rel: Relation,
    index_name: *const libc::c_char,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let index_oid: Oid;
    let address: ObjectAddress;

    index_oid = get_relname_relid(index_name, (*(*rel).rd_rel).relnamespace);

    if !OidIsValid(index_oid) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" for table \"{}\" does not exist",
                CStr::from_ptr(index_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    /* Check index is valid to cluster on */
    check_index_is_clusterable(rel, index_oid, lockmode);

    /* And do the work */
    mark_index_clustered(rel, index_oid, false);

    ObjectAddressSet!(address, RelationRelationId, index_oid);

    address
}

// ---------------------------------------------------------------------------
// ATExecDropCluster
// ---------------------------------------------------------------------------

/// ALTER TABLE SET WITHOUT CLUSTER
///
/// We have to find any indexes on the table that have indisclustered bit
/// set and turn it off.
unsafe fn ATExecDropCluster(rel: Relation, lockmode: LOCKMODE) {
    mark_index_clustered(rel, InvalidOid, false);
}

// ---------------------------------------------------------------------------
// ATPrepSetAccessMethod
// ---------------------------------------------------------------------------

/// Preparation phase for SET ACCESS METHOD
///
/// Check that the access method exists and determine whether a change is
/// actually needed.
unsafe fn ATPrepSetAccessMethod(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    amname: *const libc::c_char,
) {
    let amoid: Oid;

    /*
     * Look up the access method name and check that it differs from the
     * table's current AM. If DEFAULT was specified for a partitioned table
     * (amname is NULL), set it to InvalidOid to reset the catalogued AM.
     */
    if !amname.is_null() {
        amoid = get_table_am_oid(amname, false);
    } else if (*(*rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        amoid = InvalidOid;
    } else {
        amoid = get_table_am_oid(default_table_access_method, false);
    }

    /* if it's a match, phase 3 doesn't need to do anything */
    if (*(*rel).rd_rel).relam == amoid {
        return;
    }

    /* Save info for Phase 3 to do the real work */
    (*tab).rewrite |= AT_REWRITE_ACCESS_METHOD;
    (*tab).newAccessMethod = amoid;
    (*tab).chgAccessMethod = true;
}

// ---------------------------------------------------------------------------
// ATExecSetAccessMethodNoStorage
// ---------------------------------------------------------------------------

/// Special handling of ALTER TABLE SET ACCESS METHOD for relations with no
/// storage that have an interest in preserving AM.
///
/// Since these have no storage, setting the access method is a catalog only
/// operation.
unsafe fn ATExecSetAccessMethodNoStorage(rel: Relation, new_access_method_id: Oid) {
    let pg_class: Relation;
    let old_access_method_id: Oid;
    let tuple: HeapTuple;
    let rd_rel: Form_pg_class;
    let reloid: Oid = RelationGetRelid(rel);

    /*
     * Shouldn't be called on relations having storage; these are processed in
     * phase 3.
     */
    Assert!(!RELKIND_HAS_STORAGE!((*(*rel).rd_rel).relkind as u8));

    /* Get a modifiable copy of the relation's pg_class row. */
    pg_class = table_open(RelationRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(reloid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", reloid);
    }
    rd_rel = GETSTRUCT(tuple) as Form_pg_class;

    /* Update the pg_class row. */
    old_access_method_id = (*rd_rel).relam;
    (*rd_rel).relam = new_access_method_id;

    /* Leave if no update required */
    if (*rd_rel).relam == old_access_method_id {
        heap_freetuple(tuple);
        table_close(pg_class, RowExclusiveLock);
        return;
    }

    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    /*
     * Update the dependency on the new access method. No dependency is added
     * if the new access method is InvalidOid (default case).
     */
    if !OidIsValid(old_access_method_id) && OidIsValid((*rd_rel).relam) {
        let mut relobj: ObjectAddress = core::mem::zeroed();
        let mut referenced: ObjectAddress = core::mem::zeroed();

        /*
         * New access method is defined and there was no dependency
         * previously, so record a new one.
         */
        ObjectAddressSet!(relobj, RelationRelationId, reloid);
        ObjectAddressSet!(referenced, AccessMethodRelationId, (*rd_rel).relam);
        recordDependencyOn(&relobj, &referenced, DEPENDENCY_NORMAL);
    } else if OidIsValid(old_access_method_id) && !OidIsValid((*rd_rel).relam) {
        /*
         * There was an access method defined, and no new one, so just remove
         * the existing dependency.
         */
        deleteDependencyRecordsForClass(
            RelationRelationId,
            reloid,
            AccessMethodRelationId,
            DEPENDENCY_NORMAL,
        );
    } else {
        Assert!(OidIsValid(old_access_method_id) && OidIsValid((*rd_rel).relam));

        /* Both are valid, so update the dependency */
        changeDependencyFor(
            RelationRelationId,
            reloid,
            AccessMethodRelationId,
            old_access_method_id,
            (*rd_rel).relam,
        );
    }

    /* make the relam and dependency changes visible */
    CommandCounterIncrement();

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    heap_freetuple(tuple);
    table_close(pg_class, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATPrepSetTableSpace
// ---------------------------------------------------------------------------

/// ALTER TABLE SET TABLESPACE
unsafe fn ATPrepSetTableSpace(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    tablespacename: *const libc::c_char,
    lockmode: LOCKMODE,
) {
    let tablespace_id: Oid;

    /* Check that the tablespace exists */
    tablespace_id = get_tablespace_oid(tablespacename, false);

    /* Check permissions except when moving to database's default */
    if OidIsValid(tablespace_id) && tablespace_id != MyDatabaseTableSpace {
        let aclresult: AclResult = object_aclcheck(
            TableSpaceRelationId,
            tablespace_id,
            GetUserId(),
            ACL_CREATE,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, tablespacename);
        }
    }

    /* Save info for Phase 3 to do the real work */
    if OidIsValid((*tab).newTableSpace) {
        ereport!(
            ERROR,
            errmsg!("cannot have multiple SET TABLESPACE subcommands")
            /* errcode(ERRCODE_SYNTAX_ERROR) */
        );
    }

    (*tab).newTableSpace = tablespace_id;
}

// ---------------------------------------------------------------------------
// ATExecSetRelOptions
// ---------------------------------------------------------------------------

/// Set, reset, or replace reloptions.
unsafe fn ATExecSetRelOptions(
    rel: Relation,
    def_list: *mut List,
    operation: AlterTableType,
    lockmode: LOCKMODE,
) {
    let relid: Oid;
    let pgclass: Relation;
    let mut tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut datum: Datum;
    let new_options: Datum;
    let mut repl_val: [Datum; Natts_pg_class] = [0; Natts_pg_class];
    let mut repl_null: [bool; Natts_pg_class] = [false; Natts_pg_class];
    let mut repl_repl: [bool; Natts_pg_class] = [false; Natts_pg_class];
    let valid_nsps: &[*const libc::c_char] = HEAP_RELOPT_NAMESPACES;

    if def_list == NIL && operation != AT_ReplaceRelOptions {
        return; /* nothing to do */
    }

    pgclass = table_open(RelationRelationId, RowExclusiveLock);

    /* Fetch heap tuple */
    relid = RelationGetRelid(rel);
    tuple = SearchSysCache1Locked(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }

    if operation == AT_ReplaceRelOptions {
        /*
         * If we're supposed to replace the reloptions list, we just pretend
         * there were none before.
         */
        datum = 0 as Datum;
    } else {
        let mut isnull: bool = false;
        /* Get the old reloptions */
        datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &mut isnull);
        if isnull {
            datum = 0 as Datum;
        }
    }

    /* Generate new proposed reloptions (text array) */
    let new_options = transformRelOptions(
        datum,
        def_list,
        core::ptr::null_mut(),
        valid_nsps.as_ptr() as *mut *const libc::c_char,
        false,
        operation == AT_ResetRelOptions,
    );

    /* Validate */
    match (*(*rel).rd_rel).relkind as u8 {
        RELKIND_RELATION | RELKIND_MATVIEW => {
            let _ = heap_reloptions((*(*rel).rd_rel).relkind, new_options, true);
        }
        RELKIND_PARTITIONED_TABLE => {
            let _ = partitioned_table_reloptions(new_options, true);
        }
        RELKIND_VIEW => {
            let _ = view_reloptions(new_options, true);
        }
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => {
            let _ = index_reloptions((*(*rel).rd_indam).amoptions, new_options, true);
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot set options for relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errdetail_relkind_not_supported(rel->rd_rel->relkind) */
            );
        }
    }

    /* Special-case validation of view options */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_VIEW {
        let view_query: *mut Query = get_view_query(rel);
        let view_options: *mut List = untransformRelOptions(new_options);
        let mut check_option: bool = false;
        let mut cell: *mut ListCell = list_head(view_options);
        while !cell.is_null() {
            let defel: *mut DefElem = lfirst(cell) as *mut DefElem;
            if libc::strcmp((*defel).defname, cstr!("check_option")) == 0 {
                check_option = true;
            }
            cell = lnext(view_options, cell);
        }

        /*
         * If the check option is specified, look to see if the view is
         * actually auto-updatable or not.
         */
        if check_option {
            let view_updatable_error: *const libc::c_char =
                view_query_is_auto_updatable(view_query, true);
            if !view_updatable_error.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("WITH CHECK OPTION is supported only on automatically updatable views")
                    /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errhint("%s", _(view_updatable_error)) */
                );
            }
        }
    }

    /*
     * All we need do here is update the pg_class row; the new options will be
     * propagated into relcaches during post-commit cache inval.
     */
    libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
    libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

    if new_options != 0 as Datum {
        repl_val[Anum_pg_class_reloptions - 1] = new_options;
    } else {
        repl_null[Anum_pg_class_reloptions - 1] = true;
    }

    repl_repl[Anum_pg_class_reloptions - 1] = true;

    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(pgclass),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(pgclass, &(*newtuple).t_self, newtuple);
    UnlockTuple(pgclass, &(*tuple).t_self, InplaceUpdateTupleLock);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    heap_freetuple(newtuple);

    ReleaseSysCache(tuple);

    /* repeat the whole exercise for the toast table, if there's one */
    if OidIsValid((*(*rel).rd_rel).reltoastrelid) {
        let toastrel: Relation;
        let toastid: Oid = (*(*rel).rd_rel).reltoastrelid;

        toastrel = table_open(toastid, lockmode);

        /* Fetch heap tuple */
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for relation {}", toastid);
        }

        if operation == AT_ReplaceRelOptions {
            datum = 0 as Datum;
        } else {
            let mut isnull: bool = false;
            datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &mut isnull);
            if isnull {
                datum = 0 as Datum;
            }
        }

        let new_options = transformRelOptions(
            datum,
            def_list,
            cstr!("toast"),
            valid_nsps.as_ptr() as *mut *const libc::c_char,
            false,
            operation == AT_ResetRelOptions,
        );

        let _ = heap_reloptions(RELKIND_TOASTVALUE as libc::c_char, new_options, true);

        libc::memset(repl_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_val));
        libc::memset(repl_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_null));
        libc::memset(repl_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&repl_repl));

        if new_options != 0 as Datum {
            repl_val[Anum_pg_class_reloptions - 1] = new_options;
        } else {
            repl_null[Anum_pg_class_reloptions - 1] = true;
        }

        repl_repl[Anum_pg_class_reloptions - 1] = true;

        let newtuple = heap_modify_tuple(
            tuple,
            RelationGetDescr(pgclass),
            repl_val.as_mut_ptr(),
            repl_null.as_mut_ptr(),
            repl_repl.as_mut_ptr(),
        );

        CatalogTupleUpdate(pgclass, &(*newtuple).t_self, newtuple);

        InvokeObjectPostAlterHookArg(
            RelationRelationId,
            RelationGetRelid(toastrel),
            0,
            InvalidOid,
            true,
        );

        heap_freetuple(newtuple);

        ReleaseSysCache(tuple);

        table_close(toastrel, NoLock);
    }

    table_close(pgclass, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecSetTableSpace
// ---------------------------------------------------------------------------

/// Execute ALTER TABLE SET TABLESPACE for cases where there is no tuple
/// rewriting to be done, so we just want to copy the data as fast as possible.
unsafe fn ATExecSetTableSpace(
    table_oid: Oid,
    new_table_space: Oid,
    lockmode: LOCKMODE,
) {
    let rel: Relation;
    let reltoastrelid: Oid;
    let newrelfilenumber: RelFileNumber;
    let mut newrlocator: RelFileLocator;
    let mut reltoastidxids: *mut List = NIL;
    let mut lc: *mut ListCell;

    /*
     * Need lock here in case we are recursing to toast table or index
     */
    rel = relation_open(table_oid, lockmode);

    /* Check first if relation can be moved to new tablespace */
    if !CheckRelationTableSpaceMove(rel, new_table_space) {
        InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
        relation_close(rel, NoLock);
        return;
    }

    reltoastrelid = (*(*rel).rd_rel).reltoastrelid;
    /* Fetch the list of indexes on toast relation if necessary */
    if OidIsValid(reltoastrelid) {
        let toast_rel: Relation = relation_open(reltoastrelid, lockmode);
        reltoastidxids = RelationGetIndexList(toast_rel);
        relation_close(toast_rel, lockmode);
    }

    /*
     * Relfilenumbers are not unique in databases across tablespaces, so we
     * need to allocate a new one in the new tablespace.
     */
    newrelfilenumber = GetNewRelFileNumber(
        new_table_space,
        core::ptr::null_mut(),
        (*(*rel).rd_rel).relpersistence,
    );

    /* Open old and new relation */
    newrlocator = (*rel).rd_locator;
    newrlocator.relNumber = newrelfilenumber;
    newrlocator.spcOid = new_table_space;

    /* hand off to AM to actually create new rel storage and copy the data */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_INDEX {
        index_copy_data(rel, newrlocator);
    } else {
        Assert!(RELKIND_HAS_TABLE_AM!((*(*rel).rd_rel).relkind as u8));
        table_relation_copy_data(rel, &newrlocator);
    }

    /*
     * Update the pg_class row.
     *
     * NB: This wouldn't work if ATExecSetTableSpace() were allowed to be
     * executed on pg_class or its indexes, but that's forbidden with
     * CheckRelationTableSpaceMove().
     */
    SetRelationTableSpace(rel, new_table_space, newrelfilenumber);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    RelationAssumeNewRelfilelocator(rel);

    relation_close(rel, NoLock);

    /* Make sure the reltablespace change is visible */
    CommandCounterIncrement();

    /* Move associated toast relation and/or indexes, too */
    if OidIsValid(reltoastrelid) {
        ATExecSetTableSpace(reltoastrelid, new_table_space, lockmode);
    }
    lc = list_head(reltoastidxids);
    while !lc.is_null() {
        ATExecSetTableSpace(lfirst_oid(lc), new_table_space, lockmode);
        lc = lnext(reltoastidxids, lc);
    }

    /* Clean up */
    list_free(reltoastidxids);
}

// ---------------------------------------------------------------------------
// ATExecSetTableSpaceNoStorage
// ---------------------------------------------------------------------------

/// Special handling of ALTER TABLE SET TABLESPACE for relations with no
/// storage that have an interest in preserving tablespace.
///
/// Since these have no storage the tablespace can be updated with a simple
/// metadata only operation to update the tablespace.
unsafe fn ATExecSetTableSpaceNoStorage(rel: Relation, new_table_space: Oid) {
    /*
     * Shouldn't be called on relations having storage; these are processed in
     * phase 3.
     */
    Assert!(!RELKIND_HAS_STORAGE!((*(*rel).rd_rel).relkind as u8));

    /* check if relation can be moved to its new tablespace */
    if !CheckRelationTableSpaceMove(rel, new_table_space) {
        InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
        return;
    }

    /* Update can be done, so change reltablespace */
    SetRelationTableSpace(rel, new_table_space, InvalidOid);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    /* Make sure the reltablespace change is visible */
    CommandCounterIncrement();
}

// ---------------------------------------------------------------------------
// AlterTableMoveAll
// ---------------------------------------------------------------------------

/// Alter Table ALL ... SET TABLESPACE
///
/// Allows a user to move all objects of some type in a given tablespace in the
/// current database to another tablespace.
pub unsafe fn AlterTableMoveAll(stmt: *mut AlterTableMoveAllStmt) -> Oid {
    let mut relations: *mut List = NIL;
    let mut l: *mut ListCell;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();
    let rel: Relation;
    let scan: TableScanDesc;
    let mut tuple: HeapTuple;
    let orig_tablespaceoid: Oid;
    let new_tablespaceoid: Oid;
    let role_oids: *mut List = roleSpecsToIds((*stmt).roles);

    /* Ensure we were not asked to move something we can't */
    if (*stmt).objtype != OBJECT_TABLE
        && (*stmt).objtype != OBJECT_INDEX
        && (*stmt).objtype != OBJECT_MATVIEW
    {
        ereport!(
            ERROR,
            errmsg!("only tables, indexes, and materialized views exist in tablespaces")
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    /* Get the orig and new tablespace OIDs */
    orig_tablespaceoid = get_tablespace_oid((*stmt).orig_tablespacename, false);
    let mut new_tablespaceoid = get_tablespace_oid((*stmt).new_tablespacename, false);

    /* Can't move shared relations in to or out of pg_global */
    if orig_tablespaceoid == GLOBALTABLESPACE_OID || new_tablespaceoid == GLOBALTABLESPACE_OID {
        ereport!(
            ERROR,
            errmsg!("cannot move relations in to or out of pg_global tablespace")
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    /*
     * Must have CREATE rights on the new tablespace, unless it is the
     * database default tablespace.
     */
    if OidIsValid(new_tablespaceoid) && new_tablespaceoid != MyDatabaseTableSpace {
        let aclresult: AclResult = object_aclcheck(
            TableSpaceRelationId,
            new_tablespaceoid,
            GetUserId(),
            ACL_CREATE,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_TABLESPACE, get_tablespace_name(new_tablespaceoid));
        }
    }

    /*
     * Now that the checks are done, check if we should set either to
     * InvalidOid because it is our database's default tablespace.
     */
    let mut orig_tablespaceoid = orig_tablespaceoid;
    if orig_tablespaceoid == MyDatabaseTableSpace {
        orig_tablespaceoid = InvalidOid;
    }
    if new_tablespaceoid == MyDatabaseTableSpace {
        new_tablespaceoid = InvalidOid;
    }

    /* no-op */
    if orig_tablespaceoid == new_tablespaceoid {
        return new_tablespaceoid;
    }

    /*
     * Walk the list of objects in the tablespace and move them. This will
     * only find objects in our database, of course.
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_reltablespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(orig_tablespaceoid),
    );

    rel = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan_catalog(rel, 1, key.as_mut_ptr());
    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let rel_form: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let rel_oid: Oid = (*rel_form).oid;

        /*
         * Do not move objects in pg_catalog as part of this.
         * Also, explicitly avoid any shared tables, temp tables, or TOAST.
         */
        if IsCatalogNamespace((*rel_form).relnamespace)
            || (*rel_form).relisshared
            || isAnyTempNamespace((*rel_form).relnamespace)
            || IsToastNamespace((*rel_form).relnamespace)
        {
            continue;
        }

        /* Only move the object type requested */
        if ((*stmt).objtype == OBJECT_TABLE
            && (*rel_form).relkind as u8 != RELKIND_RELATION
            && (*rel_form).relkind as u8 != RELKIND_PARTITIONED_TABLE)
            || ((*stmt).objtype == OBJECT_INDEX
                && (*rel_form).relkind as u8 != RELKIND_INDEX
                && (*rel_form).relkind as u8 != RELKIND_PARTITIONED_INDEX)
            || ((*stmt).objtype == OBJECT_MATVIEW
                && (*rel_form).relkind as u8 != RELKIND_MATVIEW)
        {
            continue;
        }

        /* Check if we are only moving objects owned by certain roles */
        if role_oids != NIL && !list_member_oid(role_oids, (*rel_form).relowner) {
            continue;
        }

        /*
         * Handle permissions-checking here since we are locking the tables
         * and also to avoid doing a bunch of work only to fail part-way.
         */
        if !object_ownercheck(RelationRelationId, rel_oid, GetUserId()) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_relkind_objtype(get_rel_relkind(rel_oid)),
                NameStr!((*rel_form).relname),
            );
        }

        if (*stmt).nowait && !ConditionalLockRelationOid(rel_oid, AccessExclusiveLock) {
            ereport!(
                ERROR,
                errmsg!(
                    "aborting because lock on relation \"{}.{}\" is not available",
                    CStr::from_ptr(get_namespace_name((*rel_form).relnamespace)).to_string_lossy(),
                    CStr::from_ptr(NameStr!((*rel_form).relname)).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_IN_USE) */
            );
        } else {
            LockRelationOid(rel_oid, AccessExclusiveLock);
        }

        /* Add to our list of objects to move */
        relations = lappend_oid(relations, rel_oid);
    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);

    if relations == NIL {
        ereport!(
            NOTICE,
            errmsg!(
                "no matching relations in tablespace \"{}\" found",
                if orig_tablespaceoid == InvalidOid {
                    "(database default)"
                } else {
                    CStr::from_ptr(get_tablespace_name(orig_tablespaceoid))
                        .to_str()
                        .unwrap_or("?")
                }
            ) /* errcode(ERRCODE_NO_DATA_FOUND) */
        );
    }

    /* Everything is locked, loop through and move all of the relations. */
    l = list_head(relations);
    while !l.is_null() {
        let mut cmds: *mut List = NIL;
        let cmd: *mut AlterTableCmd = makeNode!(AlterTableCmd, T_AlterTableCmd);

        (*cmd).subtype = AT_SetTableSpace;
        (*cmd).name = (*stmt).new_tablespacename;

        cmds = lappend(cmds, cmd as *mut libc::c_void);

        EventTriggerAlterTableStart(stmt as *mut Node);
        /* OID is set by AlterTableInternal */
        AlterTableInternal(lfirst_oid(l), cmds, false);
        EventTriggerAlterTableEnd();

        l = lnext(relations, l);
    }

    new_tablespaceoid
}

// ---------------------------------------------------------------------------
// index_copy_data
// ---------------------------------------------------------------------------

unsafe fn index_copy_data(rel: Relation, newrlocator: RelFileLocator) {
    let dstrel: SMgrRelation;

    /*
     * Since we copy the file directly without looking at the shared buffers,
     * we'd better first flush out any pages of the source relation that are
     * in shared buffers.
     */
    FlushRelationBuffers(rel);

    /*
     * Create and copy all forks of the relation, and schedule unlinking of
     * old physical files.
     *
     * NOTE: any conflict in relfilenumber value will be caught in
     * RelationCreateStorage().
     */
    dstrel = RelationCreateStorage(newrlocator, (*(*rel).rd_rel).relpersistence, true);

    /* copy main fork */
    RelationCopyStorage(
        RelationGetSmgr(rel),
        dstrel,
        MAIN_FORKNUM,
        (*(*rel).rd_rel).relpersistence,
    );

    /* copy those extra forks that exist */
    let mut fork_num: ForkNumber = MAIN_FORKNUM + 1;
    while fork_num <= MAX_FORKNUM {
        if smgrexists(RelationGetSmgr(rel), fork_num) {
            smgrcreate(dstrel, fork_num, false);

            /*
             * WAL log creation if the relation is persistent, or this is the
             * init fork of an unlogged relation.
             */
            if RelationIsPermanent(rel)
                || ((*(*rel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED
                    && fork_num == INIT_FORKNUM)
            {
                log_smgrcreate(&newrlocator, fork_num);
            }
            RelationCopyStorage(
                RelationGetSmgr(rel),
                dstrel,
                fork_num,
                (*(*rel).rd_rel).relpersistence,
            );
        }
        fork_num += 1;
    }

    /* drop old relation, and close new one */
    RelationDropStorage(rel);
    smgrclose(dstrel);
}

// ---------------------------------------------------------------------------
// ATExecEnableDisableTrigger
// ---------------------------------------------------------------------------

/// ALTER TABLE ENABLE/DISABLE TRIGGER
///
/// We just pass this off to trigger.c.
unsafe fn ATExecEnableDisableTrigger(
    rel: Relation,
    trigname: *const libc::c_char,
    fires_when: libc::c_char,
    skip_system: bool,
    recurse: bool,
    lockmode: LOCKMODE,
) {
    EnableDisableTrigger(
        rel,
        trigname,
        InvalidOid,
        fires_when,
        skip_system,
        recurse,
        lockmode,
    );

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
}

// ---------------------------------------------------------------------------
// ATExecEnableDisableRule
// ---------------------------------------------------------------------------

/// ALTER TABLE ENABLE/DISABLE RULE
///
/// We just pass this off to rewriteDefine.c.
unsafe fn ATExecEnableDisableRule(
    rel: Relation,
    rulename: *const libc::c_char,
    fires_when: libc::c_char,
    lockmode: LOCKMODE,
) {
    EnableDisableRule(rel, rulename, fires_when);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
}

// ---------------------------------------------------------------------------
// ATPrepAddInherit
// ---------------------------------------------------------------------------

/// ALTER TABLE INHERIT
///
/// Add a parent to the child's parents.
unsafe fn ATPrepAddInherit(child_rel: Relation) {
    if (*(*child_rel).rd_rel).reloftype != InvalidOid {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of typed table")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*child_rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*child_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of partitioned table")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
}

// ---------------------------------------------------------------------------
// ATExecAddInherit
// ---------------------------------------------------------------------------

/// Return the address of the new parent relation.
unsafe fn ATExecAddInherit(
    child_rel: Relation,
    parent: *mut RangeVar,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let parent_rel: Relation;
    let children: *mut List;
    let address: ObjectAddress;
    let trigger_name: *const libc::c_char;

    /*
     * A self-exclusive lock is needed here. See the similar case in
     * MergeAttributes() for a full explanation.
     */
    parent_rel = table_openrv(parent, ShareUpdateExclusiveLock);

    /*
     * Must be owner of both parent and child -- child was checked by
     * ATSimplePermissions call in ATPrepCmd
     */
    ATSimplePermissions(
        AT_AddInherit,
        parent_rel,
        ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
    );

    /* Permanent rels cannot inherit from temporary ones */
    if (*(*parent_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && (*(*child_rel).rd_rel).relpersistence != RELPERSISTENCE_TEMP
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot inherit from temporary relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* If parent rel is temp, it must belong to this session */
    if (*(*parent_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && !(*parent_rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot inherit from temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Ditto for the child */
    if (*(*child_rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP
        && !(*child_rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot inherit to temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Prevent partitioned tables from becoming inheritance parents */
    if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        ereport!(
            ERROR,
            errmsg!(
                "cannot inherit from partitioned table \"{}\"",
                CStr::from_ptr((*parent).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Likewise for partitions */
    if (*(*parent_rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot inherit from a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * Prevent circularity by seeing if proposed parent inherits from child.
     * (In particular, this disallows making a rel inherit from itself.)
     *
     * We use weakest lock we can on child's children, namely AccessShareLock.
     */
    children = find_all_inheritors(RelationGetRelid(child_rel), AccessShareLock, core::ptr::null_mut());

    if list_member_oid(children, RelationGetRelid(parent_rel)) {
        ereport!(
            ERROR,
            errmsg!("circular inheritance not allowed")
            /* errcode(ERRCODE_DUPLICATE_TABLE),
               errdetail("\"%s\" is already a child of \"%s\".", ...) */
        );
    }

    /*
     * If child_rel has row-level triggers with transition tables, we
     * currently don't allow it to become an inheritance child.
     */
    trigger_name = FindTriggerIncompatibleWithInheritance((*child_rel).trigdesc);
    if !trigger_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "trigger \"{}\" prevents table \"{}\" from becoming an inheritance child",
                CStr::from_ptr(trigger_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("ROW triggers with transition tables are not supported in inheritance hierarchies.") */
        );
    }

    /* OK to create inheritance */
    CreateInheritance(child_rel, parent_rel, false);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(parent_rel));

    /* keep our lock on the parent relation until commit */
    table_close(parent_rel, NoLock);

    address
}

// ---------------------------------------------------------------------------
// CreateInheritance
// ---------------------------------------------------------------------------

/// Catalog manipulation portion of creating inheritance between a child
/// table and a parent table.
///
/// Common to ATExecAddInherit() and ATExecAttachPartition().
unsafe fn CreateInheritance(child_rel: Relation, parent_rel: Relation, ispartition: bool) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut inherits_tuple: HeapTuple;
    let mut inhseqno: i32;

    /* Note: get RowExclusiveLock because we will write pg_inherits below. */
    catalog_relation = table_open(InheritsRelationId, RowExclusiveLock);

    /*
     * Check for duplicates in the list of parents, and determine the highest
     * inhseqno already present; we'll use the next one for the new parent.
     * Also, if proposed child is a partition, it cannot already be inheriting.
     *
     * Note: we do not reject the case where the child already inherits from
     * the parent indirectly; CREATE TABLE doesn't reject comparable cases.
     */
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    /* inhseqno sequences start at 1 */
    inhseqno = 0;
    loop {
        inherits_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(inherits_tuple) {
            break;
        }
        let inh: Form_pg_inherits = GETSTRUCT(inherits_tuple) as Form_pg_inherits;

        if (*inh).inhparent == RelationGetRelid(parent_rel) {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" would be inherited from more than once",
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_DUPLICATE_TABLE) */
            );
        }

        if (*inh).inhseqno > inhseqno {
            inhseqno = (*inh).inhseqno;
        }
    }
    systable_endscan(scan);

    /* Match up the columns and bump attinhcount as needed */
    MergeAttributesIntoExisting(child_rel, parent_rel, ispartition);

    /* Match up the constraints and bump coninhcount as needed */
    MergeConstraintsIntoExisting(child_rel, parent_rel);

    /*
     * OK, it looks valid. Make the catalog entries that show inheritance.
     */
    StoreCatalogInheritance1(
        RelationGetRelid(child_rel),
        RelationGetRelid(parent_rel),
        inhseqno + 1,
        catalog_relation,
        (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE,
    );

    /* Now we're done with pg_inherits */
    table_close(catalog_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// decompile_conbin
// ---------------------------------------------------------------------------

/// Obtain the source-text form of the constraint expression for a check
/// constraint, given its pg_constraint tuple
unsafe fn decompile_conbin(contup: HeapTuple, tupdesc: TupleDesc) -> *mut libc::c_char {
    let con: Form_pg_constraint;
    let mut isnull: bool = false;
    let attr: Datum;
    let expr: Datum;

    con = GETSTRUCT(contup) as Form_pg_constraint;
    attr = heap_getattr(contup, Anum_pg_constraint_conbin, tupdesc, &mut isnull);
    if isnull {
        elog!(ERROR, "null conbin for constraint {}", (*con).oid);
    }

    expr = DirectFunctionCall2(pg_get_expr, attr, ObjectIdGetDatum((*con).conrelid));
    TextDatumGetCString(expr)
}

// ---------------------------------------------------------------------------
// constraints_equivalent
// ---------------------------------------------------------------------------

/// Determine whether two check constraints are functionally equivalent
///
/// The test we apply is to see whether they reverse-compile to the same
/// source string.
///
/// Note that we ignore enforceability as there are cases where constraints
/// with differing enforceability are allowed.
unsafe fn constraints_equivalent(
    a: HeapTuple,
    b: HeapTuple,
    tuple_desc: TupleDesc,
) -> bool {
    let acon: Form_pg_constraint = GETSTRUCT(a) as Form_pg_constraint;
    let bcon: Form_pg_constraint = GETSTRUCT(b) as Form_pg_constraint;

    if (*acon).condeferrable != (*bcon).condeferrable
        || (*acon).condeferred != (*bcon).condeferred
        || libc::strcmp(
            decompile_conbin(a, tuple_desc),
            decompile_conbin(b, tuple_desc),
        ) != 0
    {
        false
    } else {
        true
    }
}

// ---------------------------------------------------------------------------
// MergeAttributesIntoExisting
// ---------------------------------------------------------------------------

/// Check columns in child table match up with columns in parent, and increment
/// their attinhcount.
///
/// Called by CreateInheritance
unsafe fn MergeAttributesIntoExisting(
    child_rel: Relation,
    parent_rel: Relation,
    ispartition: bool,
) {
    let attrrel: Relation;
    let parent_desc: TupleDesc;

    attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    parent_desc = RelationGetDescr(parent_rel);

    let mut parent_attno: AttrNumber = 1;
    while parent_attno <= (*parent_desc).natts as AttrNumber {
        let parent_att: Form_pg_attribute =
            TupleDescAttr(parent_desc, (parent_attno - 1) as usize) as Form_pg_attribute;
        let parent_attname: *const libc::c_char = NameStr!((*parent_att).attname);
        let tuple: HeapTuple;

        /* Ignore dropped columns in the parent. */
        if (*parent_att).attisdropped {
            parent_attno += 1;
            continue;
        }

        /* Find same column in child (matching on column name). */
        tuple = SearchSysCacheCopyAttName(RelationGetRelid(child_rel), parent_attname);
        if HeapTupleIsValid(tuple) {
            let child_att: Form_pg_attribute = GETSTRUCT(tuple) as Form_pg_attribute;

            if (*parent_att).atttypid != (*child_att).atttypid
                || (*parent_att).atttypmod != (*child_att).atttypmod
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different type for column \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            if (*parent_att).attcollation != (*child_att).attcollation {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different collation for column \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_COLLATION_MISMATCH) */
                );
            }

            /*
             * If the parent has a not-null constraint that's not NO INHERIT,
             * make sure the child has one too.
             */
            if (*parent_att).attnotnull && !(*child_att).attnotnull {
                let contup: HeapTuple = findNotNullConstraintAttnum(
                    RelationGetRelid(parent_rel),
                    (*parent_att).attnum,
                );
                if HeapTupleIsValid(contup)
                    && !(*(GETSTRUCT(contup) as Form_pg_constraint)).connoinherit
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                            CStr::from_ptr(parent_attname).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
            }

            /*
             * Child column must be generated if and only if parent column is.
             */
            if (*parent_att).attgenerated != 0 && (*child_att).attgenerated == 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table must be a generated column",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
            if (*child_att).attgenerated != 0 && (*parent_att).attgenerated == 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table must not be a generated column",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            if (*parent_att).attgenerated != 0
                && (*child_att).attgenerated != 0
                && (*child_att).attgenerated != (*parent_att).attgenerated
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" inherits from generated column of different kind",
                        CStr::from_ptr(parent_attname).to_string_lossy()
                    )
                    /* errcode(ERRCODE_DATATYPE_MISMATCH),
                       errdetail("Parent column is %s, child column is %s.", ...) */
                );
            }

            /*
             * Regular inheritance children are independent enough not to
             * inherit identity columns. But partitions are integral part of
             * a partitioned table and inherit identity column.
             */
            if ispartition {
                (*child_att).attidentity = (*parent_att).attidentity;
            }

            /*
             * OK, bump the child column's inheritance count.
             */
            let mut new_inhcount: i16 = 0;
            if pg_add_s16_overflow((*child_att).attinhcount, 1, &mut new_inhcount) {
                ereport!(
                    ERROR,
                    errmsg!("too many inheritance parents")
                    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
                );
            }
            (*child_att).attinhcount = new_inhcount;

            /*
             * In case of partitions, we must enforce that value of attislocal
             * is same in all partitions.
             */
            if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
                Assert!((*child_att).attinhcount == 1);
                (*child_att).attislocal = false;
            }

            CatalogTupleUpdate(attrrel, &(*tuple).t_self, tuple);
            heap_freetuple(tuple);
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "child table is missing column \"{}\"",
                    CStr::from_ptr(parent_attname).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }

        parent_attno += 1;
    }

    table_close(attrrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// MergeConstraintsIntoExisting
// ---------------------------------------------------------------------------

/// Check constraints in child table match up with constraints in parent,
/// and increment their coninhcount.
///
/// Constraints that are marked ONLY in the parent are ignored.
///
/// Called by CreateInheritance
unsafe fn MergeConstraintsIntoExisting(child_rel: Relation, parent_rel: Relation) {
    let constraintrel: Relation;
    let parent_scan: SysScanDesc;
    let mut parent_key: ScanKeyData = core::mem::zeroed();
    let mut parent_tuple: HeapTuple;
    let parent_relid: Oid = RelationGetRelid(parent_rel);
    let attmap: *mut AttrMap;

    constraintrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Outer loop scans through the parent's constraint definitions */
    ScanKeyInit(
        &mut parent_key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(parent_relid),
    );
    parent_scan = systable_beginscan(
        constraintrel,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut parent_key,
    );

    attmap = build_attrmap_by_name(
        RelationGetDescr(parent_rel),
        RelationGetDescr(child_rel),
        true,
    );

    loop {
        parent_tuple = systable_getnext(parent_scan);
        if !HeapTupleIsValid(parent_tuple) {
            break;
        }
        let parent_con: Form_pg_constraint = GETSTRUCT(parent_tuple) as Form_pg_constraint;
        let child_scan: SysScanDesc;
        let mut child_key: ScanKeyData = core::mem::zeroed();
        let mut child_tuple: HeapTuple;
        let parent_attno: AttrNumber;
        let mut found: bool = false;

        if (*parent_con).contype != CONSTRAINT_CHECK as libc::c_char
            && (*parent_con).contype != CONSTRAINT_NOTNULL as libc::c_char
        {
            continue;
        }

        /* if the parent's constraint is marked NO INHERIT, it's not inherited */
        if (*parent_con).connoinherit {
            continue;
        }

        if (*parent_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            parent_attno = extractNotNullColumn(parent_tuple);
        } else {
            parent_attno = InvalidAttrNumber;
        }

        /* Search for a child constraint matching this one */
        ScanKeyInit(
            &mut child_key,
            Anum_pg_constraint_conrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(RelationGetRelid(child_rel)),
        );
        child_scan = systable_beginscan(
            constraintrel,
            ConstraintRelidTypidNameIndexId,
            true,
            core::ptr::null_mut(),
            1,
            &mut child_key,
        );

        loop {
            child_tuple = systable_getnext(child_scan);
            if !HeapTupleIsValid(child_tuple) {
                break;
            }
            let child_con: Form_pg_constraint = GETSTRUCT(child_tuple) as Form_pg_constraint;
            let child_copy: HeapTuple;

            if (*child_con).contype != (*parent_con).contype {
                continue;
            }

            /*
             * CHECK constraints are matched by constraint name, NOT NULL ones
             * by attribute number.
             */
            if (*child_con).contype == CONSTRAINT_CHECK as libc::c_char {
                if libc::strcmp(
                    NameStr!((*parent_con).conname),
                    NameStr!((*child_con).conname),
                ) != 0
                {
                    continue;
                }
            } else if (*child_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
                let parent_attr: Form_pg_attribute =
                    TupleDescAttr((*parent_rel).rd_att, (parent_attno - 1) as usize)
                        as Form_pg_attribute;
                let child_attno: AttrNumber = extractNotNullColumn(child_tuple);
                if parent_attno != (*attmap).attnums[(child_attno - 1) as usize] {
                    continue;
                }

                let child_attr: Form_pg_attribute =
                    TupleDescAttr((*child_rel).rd_att, (child_attno - 1) as usize)
                        as Form_pg_attribute;
                /* there shouldn't be constraints on dropped columns */
                if (*parent_attr).attisdropped || (*child_attr).attisdropped {
                    elog!(ERROR, "found not-null constraint on dropped columns");
                }
            }

            if (*child_con).contype == CONSTRAINT_CHECK as libc::c_char
                && !constraints_equivalent(
                    parent_tuple,
                    child_tuple,
                    RelationGetDescr(constraintrel),
                )
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "child table \"{}\" has different definition for check constraint \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                        CStr::from_ptr(NameStr!((*parent_con).conname)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            /*
             * If the child constraint is "no inherit" then cannot merge
             */
            if (*child_con).connoinherit {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with non-inherited constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * If the child constraint is "not valid" then cannot merge with a
             * valid parent constraint
             */
            if (*parent_con).convalidated
                && (*child_con).conenforced
                && !(*child_con).convalidated
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with NOT VALID constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * A NOT ENFORCED child constraint cannot be merged with an
             * ENFORCED parent constraint.
             */
            if (*parent_con).conenforced && !(*child_con).conenforced {
                ereport!(
                    ERROR,
                    errmsg!(
                        "constraint \"{}\" conflicts with NOT ENFORCED constraint on child table \"{}\"",
                        CStr::from_ptr(NameStr!((*child_con).conname)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            /*
             * OK, bump the child constraint's inheritance count.
             */
            child_copy = heap_copytuple(child_tuple);
            let child_con_copy: Form_pg_constraint =
                GETSTRUCT(child_copy) as Form_pg_constraint;

            let mut new_inhcount: i16 = 0;
            if pg_add_s16_overflow((*child_con_copy).coninhcount, 1, &mut new_inhcount) {
                ereport!(
                    ERROR,
                    errmsg!("too many inheritance parents")
                    /* errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
                );
            }
            (*child_con_copy).coninhcount = new_inhcount;

            /*
             * In case of partitions, an inherited constraint must be
             * inherited only once since it cannot have multiple parents and
             * it is never considered local.
             */
            if (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
                Assert!((*child_con_copy).coninhcount == 1);
                (*child_con_copy).conislocal = false;
            }

            CatalogTupleUpdate(constraintrel, &(*child_copy).t_self, child_copy);
            heap_freetuple(child_copy);

            found = true;
            break;
        }

        systable_endscan(child_scan);

        if !found {
            if (*parent_con).contype == CONSTRAINT_NOTNULL as libc::c_char {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                        CStr::from_ptr(get_attname(
                            parent_relid,
                            extractNotNullColumn(parent_tuple),
                            false
                        ))
                        .to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }

            ereport!(
                ERROR,
                errmsg!(
                    "child table is missing constraint \"{}\"",
                    CStr::from_ptr(NameStr!((*parent_con).conname)).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }
    }

    systable_endscan(parent_scan);
    table_close(constraintrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecDropInherit
// ---------------------------------------------------------------------------

/// ALTER TABLE NO INHERIT
///
/// Return value is the address of the relation that is no longer parent.
unsafe fn ATExecDropInherit(
    rel: Relation,
    parent: *mut RangeVar,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let address: ObjectAddress;
    let parent_rel: Relation;

    if (*(*rel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!("cannot change inheritance of a partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * AccessShareLock on the parent is probably enough, seeing that DROP
     * TABLE doesn't lock parent tables at all.
     */
    parent_rel = table_openrv(parent, AccessShareLock);

    /*
     * We don't bother to check ownership of the parent table --- ownership of
     * the child is presumed enough rights.
     */

    /* Off to RemoveInheritance() where most of the work happens */
    RemoveInheritance(rel, parent_rel, false);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(parent_rel));

    /* keep our lock on the parent relation until commit */
    table_close(parent_rel, NoLock);

    address
}

// ---------------------------------------------------------------------------
// MarkInheritDetached
// ---------------------------------------------------------------------------

/// Set inhdetachpending for a partition, for ATExecDetachPartition
/// in concurrent mode.
unsafe fn MarkInheritDetached(child_rel: Relation, parent_rel: Relation) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut inherits_tuple: HeapTuple;
    let mut found: bool = false;

    Assert!((*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE);

    /*
     * Find pg_inherits entries by inhparent. We need to scan them all in
     * order to verify that no other partition is pending detach.
     */
    catalog_relation = table_open(InheritsRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    loop {
        inherits_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(inherits_tuple) {
            break;
        }
        let inh_form: Form_pg_inherits = GETSTRUCT(inherits_tuple) as Form_pg_inherits;
        if (*inh_form).inhdetachpending {
            ereport!(
                ERROR,
                errmsg!(
                    "partition \"{}\" already pending detach in partitioned table \"{}.{}\"",
                    CStr::from_ptr(get_rel_name((*inh_form).inhrelid)).to_string_lossy(),
                    CStr::from_ptr(get_namespace_name((*(*parent_rel).rd_rel).relnamespace))
                        .to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errhint("Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.") */
            );
        }

        if (*inh_form).inhrelid == RelationGetRelid(child_rel) {
            let newtup: HeapTuple = heap_copytuple(inherits_tuple);
            (*(GETSTRUCT(newtup) as Form_pg_inherits)).inhdetachpending = true;

            CatalogTupleUpdate(catalog_relation, &(*inherits_tuple).t_self, newtup);
            found = true;
            heap_freetuple(newtup);
            /* keep looking, to ensure we catch others pending detach */
        }
    }

    /* Done */
    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    if !found {
        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" is not a partition of relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
        );
    }
}

// ---------------------------------------------------------------------------
// RemoveInheritance
// ---------------------------------------------------------------------------

/// RemoveInheritance
///
/// Drop a parent from the child's parents. This just adjusts the attinhcount
/// and attislocal of the columns and removes the pg_inherit and pg_depend
/// entries.
///
/// Common to ATExecDropInherit() and ATExecDetachPartition().
unsafe fn RemoveInheritance(child_rel: Relation, parent_rel: Relation, expect_detached: bool) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let mut attribute_tuple: HeapTuple;
    let mut constraint_tuple: HeapTuple;
    let attmap: *mut AttrMap;
    let mut connames: *mut List = NIL;
    let mut nncolumns: *mut List = NIL;
    let mut found: bool;
    let is_partitioning: bool;

    is_partitioning =
        (*(*parent_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE;

    found = DeleteInheritsTuple(
        RelationGetRelid(child_rel),
        RelationGetRelid(parent_rel),
        expect_detached,
        RelationGetRelationName(child_rel),
    );
    if !found {
        if is_partitioning {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" is not a partition of relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" is not a parent of relation \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy()
                ) /* errcode(ERRCODE_UNDEFINED_TABLE) */
            );
        }
    }

    /*
     * Search through child columns looking for ones matching parent rel
     */
    catalog_relation = table_open(AttributeRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        AttributeRelidNumIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );
    loop {
        attribute_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(attribute_tuple) {
            break;
        }
        let att: Form_pg_attribute = GETSTRUCT(attribute_tuple) as Form_pg_attribute;

        /* Ignore if dropped or not inherited */
        if (*att).attisdropped {
            continue;
        }
        if (*att).attinhcount <= 0 {
            continue;
        }

        if SearchSysCacheExistsAttName(RelationGetRelid(parent_rel), NameStr!((*att).attname)) {
            /* Decrement inhcount and possibly set islocal to true */
            let copy_tuple: HeapTuple = heap_copytuple(attribute_tuple);
            let copy_att: Form_pg_attribute = GETSTRUCT(copy_tuple) as Form_pg_attribute;

            (*copy_att).attinhcount -= 1;
            if (*copy_att).attinhcount == 0 {
                (*copy_att).attislocal = true;
            }

            CatalogTupleUpdate(catalog_relation, &(*copy_tuple).t_self, copy_tuple);
            heap_freetuple(copy_tuple);
        }
    }
    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    /*
     * Likewise, find inherited check and not-null constraints and disinherit
     * them. First need a list of the names of the parent's check constraints.
     * For NOT NULL columns, we store column numbers to match.
     */
    attmap = build_attrmap_by_name(
        RelationGetDescr(child_rel),
        RelationGetDescr(parent_rel),
        false,
    );

    catalog_relation = table_open(ConstraintRelationId, RowExclusiveLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        constraint_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(constraint_tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(constraint_tuple) as Form_pg_constraint;

        if (*con).connoinherit {
            continue;
        }

        if (*con).contype == CONSTRAINT_CHECK as libc::c_char {
            connames = lappend(connames, pstrdup(NameStr!((*con).conname)) as *mut libc::c_void);
        }
        if (*con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            let parent_attno: AttrNumber = extractNotNullColumn(constraint_tuple);
            nncolumns = lappend_int(nncolumns, (*attmap).attnums[(parent_attno - 1) as usize] as i32);
        }
    }

    systable_endscan(scan);

    /* Now scan the child's constraints to find matches */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(child_rel)),
    );
    scan = systable_beginscan(
        catalog_relation,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    loop {
        constraint_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(constraint_tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(constraint_tuple) as Form_pg_constraint;
        let mut match_found: bool = false;

        /*
         * Match CHECK constraints by name, not-null constraints by column
         * number, and ignore all others.
         */
        if (*con).contype == CONSTRAINT_CHECK as libc::c_char {
            let mut lc: *mut ListCell = list_head(connames);
            while !lc.is_null() {
                let chkname: *const libc::c_char = lfirst(lc) as *const libc::c_char;
                if libc::strcmp(NameStr!((*con).conname), chkname) == 0 {
                    match_found = true;
                    connames = list_delete_cell(connames, lc);
                    break;
                }
                lc = lnext(connames, lc);
            }
        } else if (*con).contype == CONSTRAINT_NOTNULL as libc::c_char {
            let child_attno: AttrNumber = extractNotNullColumn(constraint_tuple);
            let mut lc: *mut ListCell = list_head(nncolumns);
            while !lc.is_null() {
                let prevattno: i32 = lfirst_int(lc);
                if prevattno == child_attno as i32 {
                    match_found = true;
                    nncolumns = list_delete_cell(nncolumns, lc);
                    break;
                }
                lc = lnext(nncolumns, lc);
            }
        } else {
            continue;
        }

        if match_found {
            /* Decrement inhcount and possibly set islocal to true */
            let copy_tuple: HeapTuple = heap_copytuple(constraint_tuple);
            let copy_con: Form_pg_constraint = GETSTRUCT(copy_tuple) as Form_pg_constraint;

            if (*copy_con).coninhcount <= 0 {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "relation {} has non-inherited constraint \"{}\"",
                    RelationGetRelid(child_rel),
                    CStr::from_ptr(NameStr!((*copy_con).conname)).to_string_lossy()
                );
            }

            (*copy_con).coninhcount -= 1;
            if (*copy_con).coninhcount == 0 {
                (*copy_con).conislocal = true;
            }

            CatalogTupleUpdate(catalog_relation, &(*copy_tuple).t_self, copy_tuple);
            heap_freetuple(copy_tuple);
        }
    }

    /* We should have matched all constraints */
    if connames != NIL || nncolumns != NIL {
        elog!(
            ERROR,
            "{} unmatched constraints while removing inheritance from \"{}\" to \"{}\"",
            list_length(connames) + list_length(nncolumns),
            CStr::from_ptr(RelationGetRelationName(child_rel)).to_string_lossy(),
            CStr::from_ptr(RelationGetRelationName(parent_rel)).to_string_lossy()
        );
    }

    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);

    drop_parent_dependency(
        RelationGetRelid(child_rel),
        RelationRelationId,
        RelationGetRelid(parent_rel),
        child_dependency_type(is_partitioning),
    );

    /*
     * Post alter hook of this inherits. Since object_access_hook doesn't take
     * multiple object identifiers, we relay oid of parent relation using
     * auxiliary_id argument.
     */
    InvokeObjectPostAlterHookArg(
        InheritsRelationId,
        RelationGetRelid(child_rel),
        0,
        RelationGetRelid(parent_rel),
        false,
    );
}

// ---------------------------------------------------------------------------
// drop_parent_dependency
// ---------------------------------------------------------------------------

/// Drop the dependency created by StoreCatalogInheritance1 or
/// heap_create_with_catalog.
unsafe fn drop_parent_dependency(
    relid: Oid,
    refclassid: Oid,
    refobjid: Oid,
    deptype: DependencyType,
) {
    let catalog_relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 3] = core::mem::zeroed();
    let mut dep_tuple: HeapTuple;

    catalog_relation = table_open(DependRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_classid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_objid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut key[2],
        Anum_pg_depend_objsubid,
        BTEqualStrategyNumber,
        F_INT4EQ,
        Int32GetDatum(0),
    );

    scan = systable_beginscan(
        catalog_relation,
        DependDependerIndexId,
        true,
        core::ptr::null_mut(),
        3,
        key.as_mut_ptr(),
    );

    loop {
        dep_tuple = systable_getnext(scan);
        if !HeapTupleIsValid(dep_tuple) {
            break;
        }
        let dep: Form_pg_depend = GETSTRUCT(dep_tuple) as Form_pg_depend;

        if (*dep).refclassid == refclassid
            && (*dep).refobjid == refobjid
            && (*dep).refobjsubid == 0
            && (*dep).deptype == deptype as libc::c_char
        {
            CatalogTupleDelete(catalog_relation, &(*dep_tuple).t_self);
        }
    }

    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecAddOf
// ---------------------------------------------------------------------------

unsafe fn ATExecAddOf(
    rel: Relation,
    of_typename: *const TypeName,
    _lockmode: LOCKMODE,
) -> ObjectAddress {
    let relid: Oid = RelationGetRelid(rel);
    let typetuple: Type;
    let typeform: Form_pg_type;
    let typeid: Oid;
    let inherits_relation: Relation;
    let relation_relation: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let tableobj: ObjectAddress;
    let typeobj: ObjectAddress;
    let classtuple: HeapTuple;

    /* Validate the type. */
    typetuple = typenameType(core::ptr::null_mut(), of_typename, core::ptr::null_mut());
    check_of_type(typetuple);
    typeform = GETSTRUCT(typetuple) as Form_pg_type;
    typeid = (*typeform).oid;

    /* Fail if the table has any inheritance parents. */
    inherits_relation = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    scan = systable_beginscan(
        inherits_relation,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );
    if HeapTupleIsValid(systable_getnext(scan)) {
        ereport!(
            ERROR,
            errmsg!("typed tables cannot inherit")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);
    table_close(inherits_relation, AccessShareLock);

    /*
     * Check the tuple descriptors for compatibility. Unlike inheritance, we
     * require that the order also match. However, attnotnull need not match.
     */
    let type_tuple_desc: TupleDesc = lookup_rowtype_tupdesc(typeid, -1);
    let table_tuple_desc: TupleDesc = RelationGetDescr(rel);
    let mut table_attno: AttrNumber = 1;
    let mut type_attno: AttrNumber = 1;
    while type_attno <= (*type_tuple_desc).natts as AttrNumber {
        let type_attr: Form_pg_attribute =
            TupleDescAttr(type_tuple_desc, (type_attno - 1) as usize) as Form_pg_attribute;
        type_attno += 1;
        if (*type_attr).attisdropped {
            continue;
        }
        let type_attname: *const libc::c_char = NameStr!((*type_attr).attname);

        /* Get the next non-dropped table attribute. */
        loop {
            if table_attno > (*table_tuple_desc).natts as AttrNumber {
                ereport!(
                    ERROR,
                    errmsg!(
                        "table is missing column \"{}\"",
                        CStr::from_ptr(type_attname).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
            let table_attr: Form_pg_attribute =
                TupleDescAttr(table_tuple_desc, (table_attno - 1) as usize) as Form_pg_attribute;
            table_attno += 1;
            if !(*table_attr).attisdropped {
                let table_attname: *const libc::c_char = NameStr!((*table_attr).attname);
                /* Compare name. */
                if libc::strncmp(table_attname, type_attname, NAMEDATALEN) != 0 {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "table has column \"{}\" where type requires \"{}\"",
                            CStr::from_ptr(table_attname).to_string_lossy(),
                            CStr::from_ptr(type_attname).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
                /* Compare type. */
                if (*table_attr).atttypid != (*type_attr).atttypid
                    || (*table_attr).atttypmod != (*type_attr).atttypmod
                    || (*table_attr).attcollation != (*type_attr).attcollation
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "table \"{}\" has different type for column \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(type_attname).to_string_lossy()
                        ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                    );
                }
                break;
            }
        }
    }
    ReleaseTupleDesc(type_tuple_desc);

    /* Any remaining columns at the end of the table had better be dropped. */
    while table_attno <= (*table_tuple_desc).natts as AttrNumber {
        let table_attr: Form_pg_attribute =
            TupleDescAttr(table_tuple_desc, (table_attno - 1) as usize) as Form_pg_attribute;
        table_attno += 1;
        if !(*table_attr).attisdropped {
            ereport!(
                ERROR,
                errmsg!(
                    "table has extra column \"{}\"",
                    CStr::from_ptr(NameStr!((*table_attr).attname)).to_string_lossy()
                ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
            );
        }
    }

    /* If the table was already typed, drop the existing dependency. */
    if (*(*rel).rd_rel).reloftype != InvalidOid {
        drop_parent_dependency(
            relid,
            TypeRelationId,
            (*(*rel).rd_rel).reloftype,
            DEPENDENCY_NORMAL,
        );
    }

    /* Record a dependency on the new type. */
    let mut tableobj_local: ObjectAddress = core::mem::zeroed();
    let mut typeobj_local: ObjectAddress = core::mem::zeroed();
    tableobj_local.classId = RelationRelationId;
    tableobj_local.objectId = relid;
    tableobj_local.objectSubId = 0;
    typeobj_local.classId = TypeRelationId;
    typeobj_local.objectId = typeid;
    typeobj_local.objectSubId = 0;
    recordDependencyOn(&tableobj_local, &typeobj_local, DEPENDENCY_NORMAL);

    /* Update pg_class.reloftype */
    relation_relation = table_open(RelationRelationId, RowExclusiveLock);
    classtuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(classtuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(classtuple) as Form_pg_class)).reloftype = typeid;
    CatalogTupleUpdate(relation_relation, &(*classtuple).t_self, classtuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

    heap_freetuple(classtuple);
    table_close(relation_relation, RowExclusiveLock);

    ReleaseSysCache(typetuple);

    let _ = tableobj;
    let _ = typeobj;
    typeobj_local
}

// ---------------------------------------------------------------------------
// ATExecDropOf
// ---------------------------------------------------------------------------

unsafe fn ATExecDropOf(rel: Relation, _lockmode: LOCKMODE) {
    let relid: Oid = RelationGetRelid(rel);
    let relation_relation: Relation;
    let tuple: HeapTuple;

    if !OidIsValid((*(*rel).rd_rel).reloftype) {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a typed table",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * We don't bother to check ownership of the type --- ownership of the
     * table is presumed enough rights. No lock required on the type, either.
     */

    drop_parent_dependency(
        relid,
        TypeRelationId,
        (*(*rel).rd_rel).reloftype,
        DEPENDENCY_NORMAL,
    );

    /* Clear pg_class.reloftype */
    relation_relation = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).reloftype = InvalidOid;
    CatalogTupleUpdate(relation_relation, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);

    heap_freetuple(tuple);
    table_close(relation_relation, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// relation_mark_replica_identity
// ---------------------------------------------------------------------------

unsafe fn relation_mark_replica_identity(
    rel: Relation,
    ri_type: libc::c_char,
    index_oid: Oid,
    is_internal: bool,
) {
    let pg_index: Relation;
    let pg_class: Relation;
    let pg_class_tuple: HeapTuple;
    let pg_class_form: Form_pg_class;
    let index_list: *mut List;

    /*
     * Check whether relreplident has changed, and update it if so.
     */
    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    pg_class_tuple =
        SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(pg_class_tuple) {
        elog!(
            ERROR,
            "cache lookup failed for relation \"{}\"",
            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }
    pg_class_form = GETSTRUCT(pg_class_tuple) as Form_pg_class;
    if (*pg_class_form).relreplident != ri_type {
        (*pg_class_form).relreplident = ri_type;
        CatalogTupleUpdate(pg_class, &(*pg_class_tuple).t_self, pg_class_tuple);
    }
    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(pg_class_tuple);

    /*
     * Update the per-index indisreplident flags correctly.
     */
    pg_index = table_open(IndexRelationId, RowExclusiveLock);
    index_list = RelationGetIndexList(rel);
    let mut lc: *mut ListCell = list_head(index_list);
    while !lc.is_null() {
        let this_index_oid: Oid = lfirst_oid(lc);
        let mut dirty: bool = false;
        let pg_index_tuple: HeapTuple;
        let pg_index_form: Form_pg_index;

        pg_index_tuple =
            SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(this_index_oid));
        if !HeapTupleIsValid(pg_index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", this_index_oid);
        }
        pg_index_form = GETSTRUCT(pg_index_tuple) as Form_pg_index;

        if this_index_oid == index_oid {
            /* Set the bit if not already set. */
            if !(*pg_index_form).indisreplident {
                dirty = true;
                (*pg_index_form).indisreplident = true;
            }
        } else {
            /* Unset the bit if set. */
            if (*pg_index_form).indisreplident {
                dirty = true;
                (*pg_index_form).indisreplident = false;
            }
        }

        if dirty {
            CatalogTupleUpdate(pg_index, &(*pg_index_tuple).t_self, pg_index_tuple);
            InvokeObjectPostAlterHookArg(
                IndexRelationId,
                this_index_oid,
                0,
                InvalidOid,
                is_internal,
            );

            /*
             * Invalidate the relcache for the table, so that after we commit
             * all sessions will refresh the table's replica identity index
             * before attempting any UPDATE or DELETE on the table.
             */
            CacheInvalidateRelcache(rel);
        }
        heap_freetuple(pg_index_tuple);

        lc = lnext(index_list, lc);
    }

    table_close(pg_index, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecReplicaIdentity
// ---------------------------------------------------------------------------

unsafe fn ATExecReplicaIdentity(rel: Relation, stmt: *mut ReplicaIdentityStmt, _lockmode: LOCKMODE) {
    let index_oid: Oid;
    let index_rel: Relation;

    if (*stmt).identity_type == REPLICA_IDENTITY_DEFAULT as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_FULL as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_NOTHING as libc::c_char {
        relation_mark_replica_identity(rel, (*stmt).identity_type, InvalidOid, true);
        return;
    } else if (*stmt).identity_type == REPLICA_IDENTITY_INDEX as libc::c_char {
        /* fallthrough */
    } else {
        elog!(ERROR, "unexpected identity type {}", (*stmt).identity_type);
    }

    /* Check that the index exists */
    index_oid = get_relname_relid((*stmt).name, (*(*rel).rd_rel).relnamespace);
    if !OidIsValid(index_oid) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" for table \"{}\" does not exist",
                CStr::from_ptr((*stmt).name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    index_rel = index_open(index_oid, ShareLock);

    /* Check that the index is on the relation we're altering. */
    if (*index_rel).rd_index.is_null()
        || (*(*index_rel).rd_index).indrelid != RelationGetRelid(rel)
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index for table \"{}\"",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /*
     * The AM must support uniqueness, and the index must in fact be unique.
     * If we have a WITHOUT OVERLAPS constraint (identified by uniqueness +
     * exclusion), we can use that too.
     */
    if (!(*(*index_rel).rd_indam).amcanunique
        || !(*(*index_rel).rd_index).indisunique)
        && !((*(*index_rel).rd_index).indisunique
            && (*(*index_rel).rd_index).indisexclusion)
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use non-unique index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    /* Deferred indexes are not guaranteed to be always unique. */
    if !(*(*index_rel).rd_index).indimmediate {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use non-immediate index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }
    /* Expression indexes aren't supported. */
    if RelationGetIndexExpressions(index_rel) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use expression index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }
    /* Predicate indexes aren't supported. */
    if RelationGetIndexPredicate(index_rel) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot use partial index \"{}\" as replica identity",
                CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* Check index for nullable columns. */
    let nkeys: i32 = IndexRelationGetNumberOfKeyAttributes(index_rel);
    for key in 0..nkeys {
        let attno: i16 = (*(*index_rel).rd_index).indkey.values[key as usize];
        let attr: Form_pg_attribute;

        if attno <= 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" cannot be used as replica identity because column {} is a system column",
                    CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                    attno
                ) /* errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
            );
        }

        attr = TupleDescAttr((*rel).rd_att, (attno - 1) as usize) as Form_pg_attribute;
        if !(*attr).attnotnull {
            ereport!(
                ERROR,
                errmsg!(
                    "index \"{}\" cannot be used as replica identity because column \"{}\" is nullable",
                    CStr::from_ptr(RelationGetRelationName(index_rel)).to_string_lossy(),
                    CStr::from_ptr(NameStr!((*attr).attname)).to_string_lossy()
                ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
            );
        }
    }

    /* This index is suitable for use as a replica identity. Mark it. */
    relation_mark_replica_identity(rel, (*stmt).identity_type, index_oid, true);

    index_close(index_rel, NoLock);
}

// ---------------------------------------------------------------------------
// ATExecSetRowSecurity
// ---------------------------------------------------------------------------

unsafe fn ATExecSetRowSecurity(rel: Relation, rls: bool) {
    let pg_class: Relation;
    let relid: Oid;
    let tuple: HeapTuple;

    relid = RelationGetRelid(rel);

    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).relrowsecurity = rls;
    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecForceNoForceRowSecurity
// ---------------------------------------------------------------------------

unsafe fn ATExecForceNoForceRowSecurity(rel: Relation, force_rls: bool) {
    let pg_class: Relation;
    let relid: Oid;
    let tuple: HeapTuple;

    relid = RelationGetRelid(rel);

    pg_class = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    (*(GETSTRUCT(tuple) as Form_pg_class)).relforcerowsecurity = force_rls;
    CatalogTupleUpdate(pg_class, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);

    table_close(pg_class, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecGenericOptions
// ---------------------------------------------------------------------------

unsafe fn ATExecGenericOptions(rel: Relation, options: *mut List) {
    let ftrel: Relation;
    let server: *mut ForeignServer;
    let fdw: *mut ForeignDataWrapper;
    let mut tuple: HeapTuple;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut repl_null: [bool; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut repl_repl: [bool; Natts_pg_foreign_table as usize] = core::mem::zeroed();
    let mut datum: Datum;
    let tableform: Form_pg_foreign_table;

    if options == NIL {
        return;
    }

    ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(FOREIGNTABLEREL, ObjectIdGetDatum((*rel).rd_id));
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "foreign table \"{}\" does not exist",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }
    tableform = GETSTRUCT(tuple) as Form_pg_foreign_table;
    server = GetForeignServer((*tableform).ftserver);
    fdw = GetForeignDataWrapper((*server).fdwid);

    libc::memset(
        repl_val.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_val),
    );
    libc::memset(
        repl_null.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_null),
    );
    libc::memset(
        repl_repl.as_mut_ptr() as *mut libc::c_void,
        0,
        core::mem::size_of_val(&repl_repl),
    );

    /* Extract the current options */
    datum = SysCacheGetAttr(
        FOREIGNTABLEREL,
        tuple,
        Anum_pg_foreign_table_ftoptions,
        &mut isnull,
    );
    if isnull {
        datum = PointerGetDatum(core::ptr::null_mut());
    }

    /* Transform the options */
    datum = transformGenericOptions(
        ForeignTableRelationId,
        datum,
        options,
        (*fdw).fdwvalidator,
    );

    if PointerIsValid(DatumGetPointer(datum)) {
        repl_val[Anum_pg_foreign_table_ftoptions as usize - 1] = datum;
    } else {
        repl_null[Anum_pg_foreign_table_ftoptions as usize - 1] = true;
    }

    repl_repl[Anum_pg_foreign_table_ftoptions as usize - 1] = true;

    tuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(ftrel),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    CatalogTupleUpdate(ftrel, &(*tuple).t_self, tuple);

    CacheInvalidateRelcache(rel);

    InvokeObjectPostAlterHook(ForeignTableRelationId, RelationGetRelid(rel), 0);

    table_close(ftrel, RowExclusiveLock);
    heap_freetuple(tuple);
}

// ---------------------------------------------------------------------------
// ATExecSetCompression
// ---------------------------------------------------------------------------

unsafe fn ATExecSetCompression(
    rel: Relation,
    column: *const libc::c_char,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrel: Relation;
    let tuple: HeapTuple;
    let atttableform: Form_pg_attribute;
    let attnum: AttrNumber;
    let compression: *mut libc::c_char;
    let cmethod: libc::c_char;
    let address: ObjectAddress;

    compression = strVal(new_value);

    attrel = table_open(AttributeRelationId, RowExclusiveLock);

    /* copy the cache entry so we can scribble on it below */
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), column);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(column).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_COLUMN) */
        );
    }

    atttableform = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*atttableform).attnum;
    if attnum <= 0 {
        ereport!(
            ERROR,
            errmsg!(
                "cannot alter system column \"{}\"",
                CStr::from_ptr(column).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    /* get the attribute compression method code */
    cmethod = GetAttributeCompression((*atttableform).atttypid, compression);

    /* update pg_attribute entry */
    (*atttableform).attcompression = cmethod;
    CatalogTupleUpdate(attrel, &(*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);

    /*
     * Apply the change to indexes as well (only for simple index columns).
     */
    SetIndexStorageProperties(
        rel,
        attrel,
        attnum,
        false,
        0,
        true,
        cmethod,
        lockmode,
    );

    heap_freetuple(tuple);
    table_close(attrel, RowExclusiveLock);

    /* make changes visible */
    CommandCounterIncrement();

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum);
    address
}

// ---------------------------------------------------------------------------
// ATPrepChangePersistence
// ---------------------------------------------------------------------------

unsafe fn ATPrepChangePersistence(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    to_logged: bool,
) {
    let pg_constraint: Relation;
    let mut tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();

    /*
     * Disallow changing status for a temp table.  Also verify whether we can
     * get away with doing nothing.
     */
    match (*(*rel).rd_rel).relpersistence as u8 {
        RELPERSISTENCE_TEMP => {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change logged status of table \"{}\" because it is temporary",
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_INVALID_TABLE_DEFINITION), errtable(rel) */
            );
        }
        RELPERSISTENCE_PERMANENT => {
            if to_logged {
                return;
            }
        }
        RELPERSISTENCE_UNLOGGED => {
            if !to_logged {
                return;
            }
        }
        _ => {}
    }

    /*
     * Check that the table is not part of any publication when changing to
     * UNLOGGED, as UNLOGGED tables can't be published.
     */
    if !to_logged && GetRelationPublications(RelationGetRelid(rel)) != NIL {
        ereport!(
            ERROR,
            errmsg!(
                "cannot change table \"{}\" to unlogged because it is part of a publication",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail("Unlogged relations cannot be replicated.") */
        );
    }

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        if to_logged {
            Anum_pg_constraint_conrelid
        } else {
            Anum_pg_constraint_confrelid
        },
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    scan = systable_beginscan(
        pg_constraint,
        if to_logged {
            ConstraintRelidTypidNameIndexId
        } else {
            InvalidOid
        },
        true,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let con: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

        if (*con).contype == CONSTRAINT_FOREIGN as libc::c_char {
            let foreign_relid: Oid;
            let foreign_rel: Relation;

            /* the opposite end of what we used as scankey */
            foreign_relid = if to_logged { (*con).confrelid } else { (*con).conrelid };

            /* ignore if self-referencing */
            if RelationGetRelid(rel) == foreign_relid {
                continue;
            }

            foreign_rel = relation_open(foreign_relid, AccessShareLock);

            if to_logged {
                if !RelationIsPermanent(foreign_rel) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not change table \"{}\" to logged because it references unlogged table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(foreign_rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errtableconstraint(rel, NameStr(con->conname)) */
                    );
                }
            } else {
                if RelationIsPermanent(foreign_rel) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not change table \"{}\" to unlogged because it references logged table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(foreign_rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errtableconstraint(rel, NameStr(con->conname)) */
                    );
                }
            }

            relation_close(foreign_rel, AccessShareLock);
        }
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    /* force rewrite if necessary; see comment in ATRewriteTables */
    (*tab).rewrite |= AT_REWRITE_ALTER_PERSISTENCE as i32;
    if to_logged {
        (*tab).newrelpersistence = RELPERSISTENCE_PERMANENT as libc::c_char;
    } else {
        (*tab).newrelpersistence = RELPERSISTENCE_UNLOGGED as libc::c_char;
    }
    (*tab).chgPersistence = true;
}

// ---------------------------------------------------------------------------
// AlterTableNamespace
// ---------------------------------------------------------------------------

pub unsafe fn AlterTableNamespace(
    stmt: *mut AlterObjectSchemaStmt,
    oldschema: *mut Oid,
) -> ObjectAddress {
    let rel: Relation;
    let relid: Oid;
    let old_nsp_oid: Oid;
    let nsp_oid: Oid;
    let newrv: *mut RangeVar;
    let objs_moved: *mut ObjectAddresses;
    let myself: ObjectAddress;

    relid = RangeVarGetRelidExtended(
        (*stmt).relation,
        AccessExclusiveLock,
        if (*stmt).missing_ok { RVR_MISSING_OK } else { 0 },
        Some(RangeVarCallbackForAlterRelation),
        stmt as *mut libc::c_void,
    );

    if !OidIsValid(relid) {
        ereport!(
            NOTICE,
            errmsg!(
                "relation \"{}\" does not exist, skipping",
                CStr::from_ptr((*(*stmt).relation).relname).to_string_lossy()
            )
        );
        return InvalidObjectAddress;
    }

    rel = relation_open(relid, NoLock);
    old_nsp_oid = RelationGetNamespace(rel);

    /* If it's an owned sequence, disallow moving it by itself. */
    if (*(*rel).rd_rel).relkind as u8 == RELKIND_SEQUENCE {
        let mut table_id: Oid = InvalidOid;
        let mut col_id: i32 = 0;

        if sequenceIsOwned(relid, DEPENDENCY_AUTO, &mut table_id, &mut col_id)
            || sequenceIsOwned(relid, DEPENDENCY_INTERNAL, &mut table_id, &mut col_id)
        {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot move an owned sequence into another schema"
                )
                /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errdetail("Sequence ... is linked to table ...") */
            );
        }
    }

    /* Get and lock schema OID and check its permissions. */
    newrv = makeRangeVar(
        (*stmt).newschema,
        RelationGetRelationName(rel) as *mut libc::c_char,
        -1,
    );
    nsp_oid = RangeVarGetAndCheckCreationNamespace(newrv, NoLock, core::ptr::null_mut());

    /* common checks on switching namespaces */
    CheckSetNamespace(old_nsp_oid, nsp_oid);

    objs_moved = new_object_addresses();
    AlterTableNamespaceInternal(rel, old_nsp_oid, nsp_oid, objs_moved);
    free_object_addresses(objs_moved);

    ObjectAddressSet!(myself, RelationRelationId, relid);

    if !oldschema.is_null() {
        *oldschema = old_nsp_oid;
    }

    /* close rel, but keep lock until commit */
    relation_close(rel, NoLock);

    myself
}

// ---------------------------------------------------------------------------
// AlterTableNamespaceInternal
// ---------------------------------------------------------------------------

pub unsafe fn AlterTableNamespaceInternal(
    rel: Relation,
    old_nsp_oid: Oid,
    nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) {
    let class_rel: Relation;

    Assert!(!objs_moved.is_null());

    /* OK, modify the pg_class row and pg_depend entry */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);

    AlterRelationNamespaceInternal(
        class_rel,
        RelationGetRelid(rel),
        old_nsp_oid,
        nsp_oid,
        true,
        objs_moved,
    );

    /* Fix the table's row type too, if it has one */
    if OidIsValid((*(*rel).rd_rel).reltype) {
        AlterTypeNamespaceInternal(
            (*(*rel).rd_rel).reltype,
            nsp_oid,
            false, /* isImplicitArray */
            false, /* ignoreDependent */
            false, /* errorOnTableType */
            objs_moved,
        );
    }

    /* Fix other dependent stuff */
    AlterIndexNamespaces(class_rel, rel, old_nsp_oid, nsp_oid, objs_moved);
    AlterSeqNamespaces(
        class_rel,
        rel,
        old_nsp_oid,
        nsp_oid,
        objs_moved,
        AccessExclusiveLock,
    );
    AlterConstraintNamespaces(RelationGetRelid(rel), old_nsp_oid, nsp_oid, false, objs_moved);

    table_close(class_rel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// AlterRelationNamespaceInternal
// ---------------------------------------------------------------------------

pub unsafe fn AlterRelationNamespaceInternal(
    class_rel: Relation,
    rel_oid: Oid,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    has_depend_entry: bool,
    objs_moved: *mut ObjectAddresses,
) {
    let class_tup: HeapTuple;
    let class_form: Form_pg_class;
    let mut thisobj: ObjectAddress = core::mem::zeroed();
    let already_done: bool;

    /* no rel lock for relkind=c so use LOCKTAG_TUPLE */
    class_tup = SearchSysCacheLockedCopy1(RELOID, ObjectIdGetDatum(rel_oid));
    if !HeapTupleIsValid(class_tup) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_oid);
    }
    class_form = GETSTRUCT(class_tup) as Form_pg_class;

    Assert!((*class_form).relnamespace == old_nsp_oid);

    thisobj.classId = RelationRelationId;
    thisobj.objectId = rel_oid;
    thisobj.objectSubId = 0;

    /*
     * If the object has already been moved, don't move it again.
     */
    already_done = object_address_present(&thisobj, objs_moved);
    if !already_done && old_nsp_oid != new_nsp_oid {
        let otid: ItemPointerData = (*class_tup).t_self;

        /* check for duplicate name */
        if get_relname_relid(NameStr!((*class_form).relname), new_nsp_oid) != InvalidOid {
            ereport!(
                ERROR,
                errmsg!(
                    "relation \"{}\" already exists in schema \"{}\"",
                    CStr::from_ptr(NameStr!((*class_form).relname)).to_string_lossy(),
                    CStr::from_ptr(get_namespace_name(new_nsp_oid)).to_string_lossy()
                ) /* errcode(ERRCODE_DUPLICATE_TABLE) */
            );
        }

        /* classTup is a copy, so OK to scribble on */
        (*class_form).relnamespace = new_nsp_oid;

        CatalogTupleUpdate(class_rel, &otid, class_tup);
        UnlockTuple(class_rel, &otid, InplaceUpdateTupleLock);

        /* Update dependency on schema if caller said so */
        if has_depend_entry
            && changeDependencyFor(
                RelationRelationId,
                rel_oid,
                NamespaceRelationId,
                old_nsp_oid,
                new_nsp_oid,
            ) != 1
        {
            elog!(
                ERROR,
                "could not change schema dependency for relation \"{}\"",
                CStr::from_ptr(NameStr!((*class_form).relname)).to_string_lossy()
            );
        }
    } else {
        UnlockTuple(class_rel, &(*class_tup).t_self, InplaceUpdateTupleLock);
    }

    if !already_done {
        add_exact_object_address(&thisobj, objs_moved);
        InvokeObjectPostAlterHook(RelationRelationId, rel_oid, 0);
    }

    heap_freetuple(class_tup);
}

// ---------------------------------------------------------------------------
// AlterIndexNamespaces (static)
// ---------------------------------------------------------------------------

unsafe fn AlterIndexNamespaces(
    class_rel: Relation,
    rel: Relation,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
) {
    let index_list: *mut List = RelationGetIndexList(rel);
    let mut lc: *mut ListCell = list_head(index_list);
    while !lc.is_null() {
        let index_oid: Oid = lfirst_oid(lc);
        let mut thisobj: ObjectAddress = core::mem::zeroed();

        thisobj.classId = RelationRelationId;
        thisobj.objectId = index_oid;
        thisobj.objectSubId = 0;

        if !object_address_present(&thisobj, objs_moved) {
            AlterRelationNamespaceInternal(
                class_rel,
                index_oid,
                old_nsp_oid,
                new_nsp_oid,
                false,
                objs_moved,
            );
            add_exact_object_address(&thisobj, objs_moved);
        }

        lc = lnext(index_list, lc);
    }

    list_free(index_list);
}

// ---------------------------------------------------------------------------
// AlterSeqNamespaces (static)
// ---------------------------------------------------------------------------

unsafe fn AlterSeqNamespaces(
    class_rel: Relation,
    rel: Relation,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: *mut ObjectAddresses,
    lockmode: LOCKMODE,
) {
    let dep_rel: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut tup: HeapTuple;

    dep_rel = table_open(DependRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_depend_refclassid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationRelationId),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_depend_refobjid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );

    scan = systable_beginscan(
        dep_rel,
        DependReferenceIndexId,
        true,
        core::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let dep_form: Form_pg_depend = GETSTRUCT(tup) as Form_pg_depend;
        let seq_rel: Relation;

        /* skip dependencies other than auto dependencies on columns */
        if (*dep_form).refobjsubid == 0
            || (*dep_form).classid != RelationRelationId
            || (*dep_form).objsubid != 0
            || !((*dep_form).deptype == DEPENDENCY_AUTO as libc::c_char
                || (*dep_form).deptype == DEPENDENCY_INTERNAL as libc::c_char)
        {
            continue;
        }

        /* Use relation_open just in case it's an index */
        seq_rel = relation_open((*dep_form).objid, lockmode);

        /* skip non-sequence relations */
        if (*RelationGetForm(seq_rel)).relkind as u8 != RELKIND_SEQUENCE {
            relation_close(seq_rel, lockmode);
            continue;
        }

        /* Fix the pg_class and pg_depend entries */
        AlterRelationNamespaceInternal(
            class_rel,
            (*dep_form).objid,
            old_nsp_oid,
            new_nsp_oid,
            true,
            objs_moved,
        );

        Assert!((*RelationGetForm(seq_rel)).reltype == InvalidOid);

        /* Now we can close it. Keep the lock till end of transaction. */
        relation_close(seq_rel, NoLock);
    }

    systable_endscan(scan);
    relation_close(dep_rel, AccessShareLock);
}

// ---------------------------------------------------------------------------
// register_on_commit_action
// ---------------------------------------------------------------------------

pub unsafe fn register_on_commit_action(relid: Oid, action: OnCommitAction) {
    let oc: *mut OnCommitItem;
    let oldcxt: MemoryContext;

    if action == ONCOMMIT_NOOP || action == ONCOMMIT_PRESERVE_ROWS {
        return;
    }

    oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

    oc = palloc(core::mem::size_of::<OnCommitItem>()) as *mut OnCommitItem;
    (*oc).relid = relid;
    (*oc).oncommit = action;
    (*oc).creating_subid = GetCurrentSubTransactionId();
    (*oc).deleting_subid = InvalidSubTransactionId;

    on_commits = lcons(oc as *mut libc::c_void, on_commits);

    MemoryContextSwitchTo(oldcxt);
}

// ---------------------------------------------------------------------------
// remove_on_commit_action
// ---------------------------------------------------------------------------

pub unsafe fn remove_on_commit_action(relid: Oid) {
    let mut lc: *mut ListCell = list_head(on_commits);
    while !lc.is_null() {
        let oc: *mut OnCommitItem = lfirst(lc) as *mut OnCommitItem;
        if (*oc).relid == relid {
            (*oc).deleting_subid = GetCurrentSubTransactionId();
            break;
        }
        lc = lnext(on_commits, lc);
    }
}

// ---------------------------------------------------------------------------
// PreCommit_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn PreCommit_on_commit_actions() {
    let mut oids_to_truncate: *mut List = NIL;
    let mut oids_to_drop: *mut List = NIL;

    let mut lc: *mut ListCell = list_head(on_commits);
    while !lc.is_null() {
        let oc: *mut OnCommitItem = lfirst(lc) as *mut OnCommitItem;
        lc = lnext(on_commits, lc);

        /* Ignore entry if already dropped in this xact */
        if (*oc).deleting_subid != InvalidSubTransactionId {
            continue;
        }

        match (*oc).oncommit {
            ONCOMMIT_NOOP | ONCOMMIT_PRESERVE_ROWS => {
                /* Do nothing */
            }
            ONCOMMIT_DELETE_ROWS => {
                if (MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE) != 0 {
                    oids_to_truncate = lappend_oid(oids_to_truncate, (*oc).relid);
                }
            }
            ONCOMMIT_DROP => {
                oids_to_drop = lappend_oid(oids_to_drop, (*oc).relid);
            }
            _ => {}
        }
    }

    if oids_to_truncate != NIL {
        heap_truncate(oids_to_truncate);
    }

    if oids_to_drop != NIL {
        let target_objects: *mut ObjectAddresses = new_object_addresses();

        let mut lc2: *mut ListCell = list_head(oids_to_drop);
        while !lc2.is_null() {
            let mut object: ObjectAddress = core::mem::zeroed();
            object.classId = RelationRelationId;
            object.objectId = lfirst_oid(lc2);
            object.objectSubId = 0;

            Assert!(!object_address_present(&object, target_objects));
            add_exact_object_address(&object, target_objects);

            lc2 = lnext(oids_to_drop, lc2);
        }

        PushActiveSnapshot(GetTransactionSnapshot());
        performMultipleDeletions(
            target_objects,
            DROP_CASCADE,
            PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY,
        );
        PopActiveSnapshot();

        /* Assert that all ON COMMIT DROP entries were deleted */
        #[cfg(debug_assertions)]
        {
            let mut lc3: *mut ListCell = list_head(on_commits);
            while !lc3.is_null() {
                let oc: *mut OnCommitItem = lfirst(lc3) as *mut OnCommitItem;
                lc3 = lnext(on_commits, lc3);
                if (*oc).oncommit != ONCOMMIT_DROP {
                    continue;
                }
                Assert!((*oc).deleting_subid != InvalidSubTransactionId);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AtEOXact_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn AtEOXact_on_commit_actions(is_commit: bool) {
    let mut cur_item: *mut ListCell = list_head(on_commits);
    while !cur_item.is_null() {
        let oc: *mut OnCommitItem = lfirst(cur_item) as *mut OnCommitItem;
        let next_item: *mut ListCell = lnext(on_commits, cur_item);

        let should_remove = if is_commit {
            (*oc).deleting_subid != InvalidSubTransactionId
        } else {
            (*oc).creating_subid != InvalidSubTransactionId
        };

        if should_remove {
            on_commits = list_delete_cell(on_commits, cur_item);
            pfree(oc as *mut libc::c_void);
        } else {
            (*oc).creating_subid = InvalidSubTransactionId;
            (*oc).deleting_subid = InvalidSubTransactionId;
        }

        cur_item = next_item;
    }
}

// ---------------------------------------------------------------------------
// AtEOSubXact_on_commit_actions
// ---------------------------------------------------------------------------

pub unsafe fn AtEOSubXact_on_commit_actions(
    is_commit: bool,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
) {
    let mut cur_item: *mut ListCell = list_head(on_commits);
    while !cur_item.is_null() {
        let oc: *mut OnCommitItem = lfirst(cur_item) as *mut OnCommitItem;
        let next_item: *mut ListCell = lnext(on_commits, cur_item);

        if !is_commit && (*oc).creating_subid == my_subid {
            on_commits = list_delete_cell(on_commits, cur_item);
            pfree(oc as *mut libc::c_void);
        } else {
            if (*oc).creating_subid == my_subid {
                (*oc).creating_subid = parent_subid;
            }
            if (*oc).deleting_subid == my_subid {
                (*oc).deleting_subid = if is_commit {
                    parent_subid
                } else {
                    InvalidSubTransactionId
                };
            }
        }

        cur_item = next_item;
    }
}

// ---------------------------------------------------------------------------
// RangeVarCallbackMaintainsTable
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn RangeVarCallbackMaintainsTable(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let relkind: libc::c_char;
    let acl_result: AclResult;

    if !OidIsValid(rel_id) {
        return;
    }

    relkind = get_rel_relkind(rel_id);
    if relkind == 0 {
        return;
    }
    if relkind as u8 != RELKIND_RELATION
        && relkind as u8 != RELKIND_TOASTVALUE
        && relkind as u8 != RELKIND_MATVIEW
        && relkind as u8 != RELKIND_PARTITIONED_TABLE
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a table or materialized view",
                CStr::from_ptr((*relation).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    acl_result = pg_class_aclcheck(rel_id, GetUserId(), ACL_MAINTAIN);
    if acl_result != ACLCHECK_OK {
        aclcheck_error(
            acl_result,
            get_relkind_objtype(get_rel_relkind(rel_id)),
            (*relation).relname,
        );
    }
}

// ---------------------------------------------------------------------------
// RangeVarCallbackForTruncate (static)
// ---------------------------------------------------------------------------

unsafe extern "C" fn RangeVarCallbackForTruncate(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let tuple: HeapTuple;

    if !OidIsValid(rel_id) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_id);
    }

    truncate_check_rel(rel_id, GETSTRUCT(tuple) as Form_pg_class);
    truncate_check_perms(rel_id, GETSTRUCT(tuple) as Form_pg_class);

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// RangeVarCallbackOwnsRelation
// ---------------------------------------------------------------------------

pub unsafe extern "C" fn RangeVarCallbackOwnsRelation(
    relation: *const RangeVar,
    rel_id: Oid,
    _old_rel_id: Oid,
    _arg: *mut libc::c_void,
) {
    let tuple: HeapTuple;

    if !OidIsValid(rel_id) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_id));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", rel_id);
    }

    if !object_ownercheck(RelationRelationId, rel_id, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(rel_id)),
            (*relation).relname,
        );
    }

    if !allowSystemTableMods
        && IsSystemClass(rel_id, GETSTRUCT(tuple) as Form_pg_class)
    {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                CStr::from_ptr((*relation).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// RangeVarCallbackForAlterRelation (static)
// ---------------------------------------------------------------------------

unsafe extern "C" fn RangeVarCallbackForAlterRelation(
    rv: *const RangeVar,
    relid: Oid,
    _oldrelid: Oid,
    arg: *mut libc::c_void,
) {
    let stmt: *mut Node = arg as *mut Node;
    let reltype: ObjectType;
    let tuple: HeapTuple;
    let classform: Form_pg_class;
    let acl_result: AclResult;
    let relkind: libc::c_char;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    classform = GETSTRUCT(tuple) as Form_pg_class;
    relkind = (*classform).relkind;

    /* Must own relation. */
    if !object_ownercheck(RelationRelationId, relid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            get_relkind_objtype(get_rel_relkind(relid)),
            (*rv).relname,
        );
    }

    /* No system table modifications unless explicitly allowed. */
    if !allowSystemTableMods && IsSystemClass(relid, classform) {
        ereport!(
            ERROR,
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
        );
    }

    if IsA!(stmt, T_RenameStmt) {
        acl_result = object_aclcheck(
            NamespaceRelationId,
            (*classform).relnamespace,
            GetUserId(),
            ACL_CREATE,
        );
        if acl_result != ACLCHECK_OK {
            aclcheck_error(acl_result, OBJECT_SCHEMA,
                           get_namespace_name((*classform).relnamespace));
        }
        reltype = (*(castNode!(RenameStmt, T_RenameStmt, stmt))).renameType;
    } else if IsA!(stmt, T_AlterObjectSchemaStmt) {
        reltype = (*(castNode!(AlterObjectSchemaStmt, T_AlterObjectSchemaStmt, stmt))).objectType;
    } else if IsA!(stmt, T_AlterTableStmt) {
        reltype = (*(castNode!(AlterTableStmt, T_AlterTableStmt, stmt))).objtype;
    } else {
        elog!(ERROR, "unrecognized node type: {}", nodeTag(stmt) as u32);
        reltype = OBJECT_TABLE; /* placate compiler */
    }

    if reltype == OBJECT_SEQUENCE && relkind as u8 != RELKIND_SEQUENCE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a sequence",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_VIEW && relkind as u8 != RELKIND_VIEW {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a view",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_MATVIEW && relkind as u8 != RELKIND_MATVIEW {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a materialized view",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_FOREIGN_TABLE && relkind as u8 != RELKIND_FOREIGN_TABLE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a foreign table",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_TYPE && relkind as u8 != RELKIND_COMPOSITE_TYPE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a composite type",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype == OBJECT_INDEX
        && relkind as u8 != RELKIND_INDEX
        && relkind as u8 != RELKIND_PARTITIONED_INDEX
        && !IsA!(stmt, T_RenameStmt)
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if reltype != OBJECT_TYPE && relkind as u8 == RELKIND_COMPOSITE_TYPE {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is a composite type",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            )
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
               errhint("Use ALTER TYPE instead.") */
        );
    }

    if IsA!(stmt, T_AlterObjectSchemaStmt) {
        if relkind as u8 == RELKIND_INDEX || relkind as u8 == RELKIND_PARTITIONED_INDEX {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of index \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Change the schema of the table instead.") */
            );
        } else if relkind as u8 == RELKIND_COMPOSITE_TYPE {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of composite type \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Use ALTER TYPE instead.") */
            );
        } else if relkind as u8 == RELKIND_TOASTVALUE {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot change schema of TOAST table \"{}\"",
                    CStr::from_ptr((*rv).relname).to_string_lossy()
                )
                /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                   errhint("Change the schema of the table instead.") */
            );
        }
    }

    ReleaseSysCache(tuple);
}

// ---------------------------------------------------------------------------
// transformPartitionSpec (static)
// ---------------------------------------------------------------------------

unsafe fn transformPartitionSpec(
    rel: Relation,
    partspec: *mut PartitionSpec,
) -> *mut PartitionSpec {
    let newspec: *mut PartitionSpec;
    let pstate: *mut ParseState;
    let nsitem: *mut ParseNamespaceItem;

    newspec = makeNode!(PartitionSpec, T_PartitionSpec) as *mut PartitionSpec;

    (*newspec).strategy = (*partspec).strategy;
    (*newspec).partParams = NIL;
    (*newspec).location = (*partspec).location;

    /* Check valid number of columns for strategy */
    if (*partspec).strategy == PARTITION_STRATEGY_LIST
        && list_length((*partspec).partParams) != 1
    {
        ereport!(
            ERROR,
            errmsg!("cannot use \"list\" partition strategy with more than one column")
            /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        );
    }

    pstate = make_parsestate(core::ptr::null_mut());
    nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                           core::ptr::null_mut(), false, true);
    addNSItemToQuery(pstate, nsitem, true, true, true);

    /* take care of any partition expressions */
    let mut lc: *mut ListCell = list_head((*partspec).partParams);
    while !lc.is_null() {
        let mut pelem: *mut PartitionElem =
            lfirst_node!(PartitionElem, T_PartitionElem, lc) as *mut PartitionElem;
        lc = lnext((*partspec).partParams, lc);

        if !(*pelem).expr.is_null() {
            /* Copy, to avoid scribbling on the input */
            pelem = copyObject(pelem as *mut libc::c_void) as *mut PartitionElem;

            /* Now do parse transformation of the expression */
            (*pelem).expr = transformExpr(pstate, (*pelem).expr,
                                          EXPR_KIND_PARTITION_EXPRESSION);

            /* we have to fix its collations too */
            assign_expr_collations(pstate, (*pelem).expr);
        }

        (*newspec).partParams = lappend((*newspec).partParams, pelem as *mut libc::c_void);
    }

    newspec
}

// ---------------------------------------------------------------------------
// ComputePartitionAttrs (static)
// ---------------------------------------------------------------------------

unsafe fn ComputePartitionAttrs(
    pstate: *mut ParseState,
    rel: Relation,
    part_params: *mut List,
    partattrs: *mut AttrNumber,
    partexprs: *mut *mut List,
    partopclass: *mut Oid,
    partcollation: *mut Oid,
    strategy: PartitionStrategy,
) {
    let mut attn: i32 = 0;
    let am_oid: Oid;

    let mut lc: *mut ListCell = list_head(part_params);
    while !lc.is_null() {
        let pelem: *mut PartitionElem =
            lfirst_node!(PartitionElem, T_PartitionElem, lc) as *mut PartitionElem;
        lc = lnext(part_params, lc);
        let atttype: Oid;
        let mut attcollation: Oid;

        if !(*pelem).name.is_null() {
            /* Simple attribute reference */
            let atttuple: HeapTuple;
            let attform: Form_pg_attribute;

            atttuple = SearchSysCacheAttName(RelationGetRelid(rel), (*pelem).name);
            if !HeapTupleIsValid(atttuple) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "column \"{}\" named in partition key does not exist",
                        CStr::from_ptr((*pelem).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_UNDEFINED_COLUMN),
                       parser_errposition(pstate, pelem->location) */
                );
            }
            attform = GETSTRUCT(atttuple) as Form_pg_attribute;

            if (*attform).attnum <= 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot use system column \"{}\" in partition key",
                        CStr::from_ptr((*pelem).name).to_string_lossy()
                    )
                    /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                );
            }

            if (*attform).attgenerated != 0 {
                ereport!(
                    ERROR,
                    errmsg!("cannot use generated column in partition key")
                    /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                       errdetail("Column ... is a generated column.") */
                );
            }

            *partattrs.add(attn as usize) = (*attform).attnum;
            atttype = (*attform).atttypid;
            attcollation = (*attform).attcollation;
            ReleaseSysCache(atttuple);
        } else {
            /* Expression */
            let mut expr: *mut Node = (*pelem).expr;
            let mut partattname: [libc::c_char; 16] = core::mem::zeroed();
            let mut expr_attrs: *mut Bitmapset = core::ptr::null_mut();

            Assert!(!expr.is_null());
            atttype = exprType(expr);
            attcollation = exprCollation(expr);

            libc::snprintf(
                partattname.as_mut_ptr(),
                partattname.len(),
                c"%d".as_ptr(),
                attn + 1,
            );
            CheckAttributeType(
                partattname.as_ptr(),
                atttype,
                attcollation,
                NIL,
                CHKATYPE_IS_PARTKEY as i32,
            );

            /* Strip any top-level COLLATE clause. */
            while IsA!(expr, T_CollateExpr) {
                expr = (*(expr as *mut CollateExpr)).arg as *mut Node;
            }

            pull_varattnos(expr, 1, &mut expr_attrs);
            if bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, expr_attrs) {
                expr_attrs = bms_add_range(
                    expr_attrs,
                    1 - FirstLowInvalidHeapAttributeNumber,
                    RelationGetNumberOfAttributes(rel) - FirstLowInvalidHeapAttributeNumber,
                );
                expr_attrs = bms_del_member(
                    expr_attrs,
                    0 - FirstLowInvalidHeapAttributeNumber,
                );
            }

            let mut i: i32 = -1;
            loop {
                i = bms_next_member(expr_attrs, i);
                if i < 0 {
                    break;
                }
                let attno: AttrNumber = (i + FirstLowInvalidHeapAttributeNumber) as AttrNumber;
                Assert!(attno != 0);

                if attno < 0 {
                    ereport!(
                        ERROR,
                        errmsg!("partition key expressions cannot contain system column references")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }

                if (*(TupleDescAttr(RelationGetDescr(rel), (attno - 1) as usize)
                    as Form_pg_attribute))
                    .attgenerated
                    != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!("cannot use generated column in partition key")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }
            }

            if IsA!(expr, T_Var) && (*(expr as *mut Var)).varattno > 0 {
                *partattrs.add(attn as usize) = (*(expr as *mut Var)).varattno;
            } else {
                *partattrs.add(attn as usize) = 0;
                *partexprs = lappend(*partexprs, expr as *mut libc::c_void);

                expr = expression_planner(expr as *mut Expr) as *mut Node;

                if contain_mutable_functions(expr) {
                    ereport!(
                        ERROR,
                        errmsg!("functions in partition key expression must be marked IMMUTABLE")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }

                if IsA!(expr, T_Const) {
                    ereport!(
                        ERROR,
                        errmsg!("cannot use constant expression as partition key")
                        /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
                    );
                }
            }
        }

        /* Apply collation override if any */
        if !(*pelem).collation.is_null() {
            attcollation = get_collation_oid((*pelem).collation, false);
        }

        if type_is_collatable(atttype) {
            if !OidIsValid(attcollation) {
                ereport!(
                    ERROR,
                    errmsg!("could not determine which collation to use for partition expression")
                    /* errcode(ERRCODE_INDETERMINATE_COLLATION),
                       errhint("Use the COLLATE clause to set the collation explicitly.") */
                );
            }
        } else {
            if OidIsValid(attcollation) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "collations are not supported by type {}",
                        CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                    ) /* errcode(ERRCODE_DATATYPE_MISMATCH) */
                );
            }
        }

        *partcollation.add(attn as usize) = attcollation;

        if strategy == PARTITION_STRATEGY_HASH {
            am_oid = HASH_AM_OID;
        } else {
            am_oid = BTREE_AM_OID;
        }

        if (*pelem).opclass.is_null() {
            *partopclass.add(attn as usize) = GetDefaultOpClass(atttype, am_oid);

            if !OidIsValid(*partopclass.add(attn as usize)) {
                if strategy == PARTITION_STRATEGY_HASH {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "data type {} has no default operator class for access method \"hash\"",
                            CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "data type {} has no default operator class for access method \"btree\"",
                            CStr::from_ptr(format_type_be(atttype)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_UNDEFINED_OBJECT) */
                    );
                }
            }
        } else {
            *partopclass.add(attn as usize) = ResolveOpClass(
                (*pelem).opclass,
                atttype,
                if am_oid == HASH_AM_OID {
                    c"hash".as_ptr()
                } else {
                    c"btree".as_ptr()
                },
                am_oid,
            );
        }

        attn += 1;
    }
}

// ---------------------------------------------------------------------------
// PartConstraintImpliedByRelConstraint
// ---------------------------------------------------------------------------

pub unsafe fn PartConstraintImpliedByRelConstraint(
    scanrel: Relation,
    part_constraint: *mut List,
) -> bool {
    let mut exist_constraint: *mut List = NIL;
    let constr: *mut TupleConstr = (*RelationGetDescr(scanrel)).constr;

    if !constr.is_null() && (*constr).has_not_null {
        let natts: i32 = (*(*scanrel).rd_att).natts as i32;

        for i in 1..=natts {
            let att: *mut CompactAttribute =
                TupleDescCompactAttr((*scanrel).rd_att, (i - 1) as usize);

            /* invalid not-null constraint must be ignored here */
            if (*att).attnullability == ATTNULLABLE_VALID && !(*att).attisdropped {
                let whole_att: Form_pg_attribute =
                    TupleDescAttr((*scanrel).rd_att, (i - 1) as usize) as Form_pg_attribute;
                let ntest: *mut NullTest = makeNode!(NullTest, T_NullTest) as *mut NullTest;

                (*ntest).arg = makeVar(1, i as AttrNumber,
                                       (*whole_att).atttypid,
                                       (*whole_att).atttypmod,
                                       (*whole_att).attcollation,
                                       0) as *mut Expr;
                (*ntest).nulltesttype = IS_NOT_NULL;
                (*ntest).argisrow = false;
                (*ntest).location = -1;
                exist_constraint = lappend(exist_constraint, ntest as *mut libc::c_void);
            }
        }
    }

    ConstraintImpliedByRelConstraint(scanrel, part_constraint, exist_constraint)
}

// ---------------------------------------------------------------------------
// ConstraintImpliedByRelConstraint
// ---------------------------------------------------------------------------

pub unsafe fn ConstraintImpliedByRelConstraint(
    scanrel: Relation,
    test_constraint: *mut List,
    proven_constraint: *mut List,
) -> bool {
    let mut exist_constraint: *mut List = list_copy(proven_constraint);
    let constr: *mut TupleConstr = (*RelationGetDescr(scanrel)).constr;
    let num_check: i32 = if !constr.is_null() { (*constr).num_check as i32 } else { 0 };

    for i in 0..num_check {
        let mut cexpr: *mut Node;

        if !(*(*constr).check.add(i as usize)).ccvalid {
            continue;
        }

        Assert!((*(*constr).check.add(i as usize)).ccenforced);

        cexpr = stringToNode((*(*constr).check.add(i as usize)).ccbin) as *mut Node;

        cexpr = eval_const_expressions(core::ptr::null_mut(), cexpr);
        cexpr = canonicalize_qual(cexpr as *mut Expr, true) as *mut Node;

        exist_constraint = list_concat(
            exist_constraint,
            make_ands_implicit(cexpr as *mut Expr),
        );
    }

    predicate_implied_by(test_constraint, exist_constraint, true)
}

// ---------------------------------------------------------------------------
// QueuePartitionConstraintValidation (static)
// ---------------------------------------------------------------------------

unsafe fn QueuePartitionConstraintValidation(
    wqueue: *mut *mut List,
    scanrel: Relation,
    part_constraint: *mut List,
    validate_default: bool,
) {
    if PartConstraintImpliedByRelConstraint(scanrel, part_constraint) {
        if !validate_default {
            ereport!(
                DEBUG1,
                errmsg_internal!(
                    "partition constraint for table \"{}\" is implied by existing constraints",
                    CStr::from_ptr(RelationGetRelationName(scanrel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                DEBUG1,
                errmsg_internal!(
                    "updated partition constraint for default partition \"{}\" is implied by existing constraints",
                    CStr::from_ptr(RelationGetRelationName(scanrel)).to_string_lossy()
                )
            );
        }
        return;
    }

    if (*(*scanrel).rd_rel).relkind as u8 == RELKIND_RELATION {
        let tab: *mut AlteredTableInfo;

        tab = ATGetQueueEntry(wqueue, scanrel);
        Assert!((*tab).partition_constraint.is_null());
        (*tab).partition_constraint =
            linitial(part_constraint) as *mut Expr;
        (*tab).validate_default = validate_default;
    } else if (*(*scanrel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let partdesc: PartitionDesc = RelationGetPartitionDesc(scanrel, true);

        for i in 0..(*partdesc).nparts {
            let part_rel: Relation;
            let this_part_constraint: *mut List;

            part_rel = table_open(*(*partdesc).oids.add(i as usize), AccessExclusiveLock);

            this_part_constraint =
                map_partition_varattnos(part_constraint, 1, part_rel, scanrel);

            QueuePartitionConstraintValidation(
                wqueue,
                part_rel,
                this_part_constraint,
                validate_default,
            );
            table_close(part_rel, NoLock);
        }
    }
}

// ---------------------------------------------------------------------------
// ATExecAttachPartition (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecAttachPartition(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut PartitionCmd,
    context: *mut AlterTableUtilityContext,
) -> ObjectAddress {
    let attachrel: Relation;
    let catalog: Relation;
    let attachrel_children: *mut List;
    let mut part_constraint: *mut List;
    let scan: SysScanDesc;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let address: ObjectAddress;
    let trigger_name: *const libc::c_char;
    let default_part_oid: Oid;
    let part_bound_constraint: *mut List;
    let pstate: *mut ParseState = make_parsestate(core::ptr::null_mut());

    (*pstate).p_sourcetext = (*context).queryString;

    /*
     * We must lock the default partition if one exists, because attaching a
     * new partition will change its partition constraint.
     */
    default_part_oid =
        get_default_oid_from_partdesc(RelationGetPartitionDesc(rel, true));
    if OidIsValid(default_part_oid) {
        LockRelationOid(default_part_oid, AccessExclusiveLock);
    }

    attachrel = table_openrv((*cmd).name, AccessExclusiveLock);

    ATSimplePermissions(
        AT_AttachPartition,
        attachrel,
        ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
    );

    /* A partition can only have one parent */
    if (*(*attachrel).rd_rel).relispartition {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is already a partition",
                CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if OidIsValid((*(*attachrel).rd_rel).reloftype) {
        ereport!(
            ERROR,
            errmsg!("cannot attach a typed table as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Table being attached should not already be part of inheritance: child */
    catalog = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut skey,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(attachrel)),
    );
    scan = systable_beginscan(
        catalog,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );
    if HeapTupleIsValid(systable_getnext(scan)) {
        ereport!(
            ERROR,
            errmsg!("cannot attach inheritance child as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);

    /* ...or as a parent table (except when it is partitioned) */
    ScanKeyInit(
        &mut skey,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(attachrel)),
    );
    scan = systable_beginscan(
        catalog,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );
    if HeapTupleIsValid(systable_getnext(scan))
        && (*(*attachrel).rd_rel).relkind as u8 == RELKIND_RELATION
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach inheritance parent as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }
    systable_endscan(scan);
    table_close(catalog, AccessShareLock);

    attachrel_children = find_all_inheritors(
        RelationGetRelid(attachrel),
        AccessExclusiveLock,
        core::ptr::null_mut(),
    );
    if list_member_oid(attachrel_children, RelationGetRelid(rel)) {
        ereport!(
            ERROR,
            errmsg!("circular inheritance not allowed")
            /* errcode(ERRCODE_DUPLICATE_TABLE),
               errdetail("... is already a child of ...") */
        );
    }

    if (*(*rel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as libc::c_char
        && (*(*attachrel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach a temporary relation as partition of permanent relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && (*(*attachrel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as libc::c_char
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach a permanent relation as partition of temporary relation \"{}\"",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            ) /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*rel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && !(*rel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach as partition of temporary relation of another session")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    if (*(*attachrel).rd_rel).relpersistence == RELPERSISTENCE_TEMP as libc::c_char
        && !(*attachrel).rd_islocaltemp
    {
        ereport!(
            ERROR,
            errmsg!("cannot attach temporary relation of another session as partition")
            /* errcode(ERRCODE_WRONG_OBJECT_TYPE) */
        );
    }

    /* Check for identity columns or columns not in parent */
    let tuple_desc: TupleDesc = RelationGetDescr(attachrel);
    let natts: i32 = (*tuple_desc).natts as i32;
    for attno in 1..=natts {
        let attribute: Form_pg_attribute =
            TupleDescAttr(tuple_desc, (attno - 1) as usize) as Form_pg_attribute;
        let attribute_name: *const libc::c_char = NameStr!((*attribute).attname);

        if (*attribute).attisdropped {
            continue;
        }

        if (*attribute).attidentity != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "table \"{}\" being attached contains an identity column \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                    CStr::from_ptr(attribute_name).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errdetail("The new partition may not contain an identity column.") */
            );
        }

        if !SearchSysCacheExists2(
            ATTNAME,
            ObjectIdGetDatum(RelationGetRelid(rel)),
            CStringGetDatum(attribute_name),
        ) {
            ereport!(
                ERROR,
                errmsg!(
                    "table \"{}\" contains column \"{}\" not found in parent \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                    CStr::from_ptr(attribute_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errcode(ERRCODE_DATATYPE_MISMATCH),
                   errdetail("The new partition may contain only the columns present in parent.") */
            );
        }
    }

    trigger_name = FindTriggerIncompatibleWithInheritance((*attachrel).trigdesc);
    if !trigger_name.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "trigger \"{}\" prevents table \"{}\" from becoming a partition",
                CStr::from_ptr(trigger_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy()
            )
            /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("ROW triggers with transition tables are not supported on partitions.") */
        );
    }

    check_new_partition_bound(
        RelationGetRelationName(attachrel) as *mut libc::c_char,
        rel,
        (*cmd).bound,
        pstate,
    );

    /* OK to create inheritance. Rest of the checks performed there */
    CreateInheritance(attachrel, rel, true);

    /* Update the pg_class entry. */
    StorePartitionBound(attachrel, rel, (*cmd).bound);

    /* Ensure there exists a correct set of indexes in the partition. */
    AttachPartitionEnsureIndexes(wqueue, rel, attachrel);

    /* and triggers */
    CloneRowTriggersToPartition(rel, attachrel);

    /* Clone foreign key constraints. */
    CloneForeignKeyConstraints(wqueue, rel, attachrel);

    part_bound_constraint = get_qual_from_partbound(rel, (*cmd).bound);
    part_constraint = list_concat_copy(part_bound_constraint, RelationGetPartitionQual(rel));

    if !part_constraint.is_null() {
        part_constraint =
            eval_const_expressions(core::ptr::null_mut(), part_constraint as *mut Node)
                as *mut List;
        part_constraint = list_make1(make_ands_explicit(part_constraint) as *mut libc::c_void);
        part_constraint =
            map_partition_varattnos(part_constraint, 1, attachrel, rel);

        QueuePartitionConstraintValidation(wqueue, attachrel, part_constraint, false);
    }

    if OidIsValid(default_part_oid) {
        let default_rel: Relation;
        let def_part_constraint: *mut List;

        Assert!(!(*(*cmd).bound).is_default);

        default_rel = table_open(default_part_oid, NoLock);
        def_part_constraint = get_proposed_default_constraint(part_bound_constraint);
        let def_part_constraint = map_partition_varattnos(
            def_part_constraint,
            1,
            default_rel,
            rel,
        );
        QueuePartitionConstraintValidation(wqueue, default_rel, def_part_constraint, true);

        table_close(default_rel, NoLock);
    }

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(attachrel));

    if (*(*attachrel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let mut lc: *mut ListCell = list_head(attachrel_children);
        while !lc.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(lc));
            lc = lnext(attachrel_children, lc);
        }
    }

    table_close(attachrel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// AttachPartitionEnsureIndexes (static)
// ---------------------------------------------------------------------------

unsafe fn AttachPartitionEnsureIndexes(
    wqueue: *mut *mut List,
    rel: Relation,
    attachrel: Relation,
) {
    let idxes: *mut List;
    let attach_rel_idxs: *mut List;
    let attach_rel_idx_rels: *mut Relation;
    let attach_infos: *mut *mut IndexInfo;
    let cxt: MemoryContext;
    let oldcxt: MemoryContext;

    cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        c"AttachPartitionEnsureIndexes".as_ptr(),
        ALLOCSET_DEFAULT_SIZES!(),
    );
    oldcxt = MemoryContextSwitchTo(cxt);

    idxes = RelationGetIndexList(rel);
    attach_rel_idxs = RelationGetIndexList(attachrel);
    let n_attach_idxs = list_length(attach_rel_idxs) as usize;
    attach_rel_idx_rels =
        palloc(core::mem::size_of::<Relation>() * n_attach_idxs) as *mut Relation;
    attach_infos =
        palloc(core::mem::size_of::<*mut IndexInfo>() * n_attach_idxs) as *mut *mut IndexInfo;

    /* Build arrays of all existing indexes and their IndexInfos */
    {
        let mut i: usize = 0;
        let mut lc: *mut ListCell = list_head(attach_rel_idxs);
        while !lc.is_null() {
            let cld_idx_id: Oid = lfirst_oid(lc);
            *attach_rel_idx_rels.add(i) = index_open(cld_idx_id, AccessShareLock);
            *attach_infos.add(i) = BuildIndexInfo(*attach_rel_idx_rels.add(i));
            i += 1;
            lc = lnext(attach_rel_idxs, lc);
        }
    }

    /* goto out target -- use a labeled block */
    'out: {
        /*
         * If attaching a foreign table, fail if any constraint index exists.
         */
        if (*(*attachrel).rd_rel).relkind as u8 == RELKIND_FOREIGN_TABLE {
            let mut cell: *mut ListCell = list_head(idxes);
            while !cell.is_null() {
                let idx: Oid = lfirst_oid(cell);
                let idx_rel: Relation = index_open(idx, AccessShareLock);

                if (*(*idx_rel).rd_index).indisunique || (*(*idx_rel).rd_index).indisprimary {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "cannot attach foreign table \"{}\" as partition of partitioned table \"{}\"",
                            CStr::from_ptr(RelationGetRelationName(attachrel)).to_string_lossy(),
                            CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        )
                        /* errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errdetail("Partitioned table ... contains unique indexes.") */
                    );
                }
                index_close(idx_rel, AccessShareLock);
                cell = lnext(idxes, cell);
            }

            break 'out;
        }

        /* For each index on partitioned table, find or create matching one. */
        let mut cell: *mut ListCell = list_head(idxes);
        while !cell.is_null() {
            let idx: Oid = lfirst_oid(cell);
            let idx_rel: Relation = index_open(idx, AccessShareLock);
            let info: *mut IndexInfo;
            let attmap: *mut AttrMap;
            let mut found: bool = false;
            let constraint_oid: Oid;

            /* Ignore non-partitioned indexes in the partitioned table */
            if (*(*idx_rel).rd_rel).relkind as u8 != RELKIND_PARTITIONED_INDEX {
                index_close(idx_rel, AccessShareLock);
                cell = lnext(idxes, cell);
                continue;
            }

            info = BuildIndexInfo(idx_rel);
            attmap = build_attrmap_by_name(
                RelationGetDescr(attachrel),
                RelationGetDescr(rel),
                false,
            );
            constraint_oid =
                get_relation_idx_constraint_oid(RelationGetRelid(rel), idx);

            for i in 0..n_attach_idxs {
                let cld_idx_id: Oid = RelationGetRelid(*attach_rel_idx_rels.add(i));
                let mut cld_constr_oid: Oid = InvalidOid;

                /* does this index have a parent?  if so, can't use it */
                if (*(*attach_rel_idx_rels.add(i)).rd_rel).relispartition {
                    continue;
                }

                /* If this index is invalid, can't use it */
                if !(*(*(*attach_rel_idx_rels.add(i)).rd_index)).indisvalid {
                    continue;
                }

                if CompareIndexInfo(
                    *attach_infos.add(i),
                    info,
                    (*(*attach_rel_idx_rels.add(i))).rd_indcollation,
                    (*idx_rel).rd_indcollation,
                    (*(*attach_rel_idx_rels.add(i))).rd_opfamily,
                    (*idx_rel).rd_opfamily,
                    attmap,
                ) {
                    if OidIsValid(constraint_oid) {
                        cld_constr_oid = get_relation_idx_constraint_oid(
                            RelationGetRelid(attachrel),
                            cld_idx_id,
                        );
                        if !OidIsValid(cld_constr_oid) {
                            continue;
                        }

                        if get_constraint_type(constraint_oid)
                            != get_constraint_type(cld_constr_oid)
                        {
                            continue;
                        }
                    }

                    /* bingo. */
                    IndexSetParentIndex(*attach_rel_idx_rels.add(i), idx);
                    if OidIsValid(constraint_oid) {
                        ConstraintSetParentConstraint(
                            cld_constr_oid,
                            constraint_oid,
                            RelationGetRelid(attachrel),
                        );
                    }
                    found = true;
                    CommandCounterIncrement();
                    break;
                }
            }

            if !found {
                let stmt: *mut IndexStmt;
                let con_oid: Oid;

                stmt = generateClonedIndexStmt(
                    core::ptr::null_mut(),
                    idx_rel,
                    attmap,
                    &mut (con_oid as Oid) as *mut Oid,
                );
                DefineIndex(
                    RelationGetRelid(attachrel),
                    stmt,
                    InvalidOid,
                    RelationGetRelid(idx_rel),
                    con_oid,
                    -1,
                    true,
                    false,
                    false,
                    false,
                    false,
                );
            }

            index_close(idx_rel, AccessShareLock);
            cell = lnext(idxes, cell);
        }
    } // 'out

    /* Clean up. */
    for i in 0..n_attach_idxs {
        index_close(*attach_rel_idx_rels.add(i), AccessShareLock);
    }
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);

    let _ = wqueue;
}

// ---------------------------------------------------------------------------
// CloneRowTriggersToPartition (static)
// ---------------------------------------------------------------------------

unsafe fn CloneRowTriggersToPartition(parent: Relation, partition: Relation) {
    let pg_trigger: Relation;
    let mut key: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tuple: HeapTuple;
    let per_tup_cxt: MemoryContext;

    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent)),
    );
    pg_trigger = table_open(TriggerRelationId, RowExclusiveLock);
    scan = systable_beginscan(
        pg_trigger,
        TriggerRelidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    per_tup_cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        c"clone trig".as_ptr(),
        ALLOCSET_SMALL_SIZES!(),
    );

    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let trig_form: Form_pg_trigger = GETSTRUCT(tuple) as Form_pg_trigger;
        let trig_stmt: *mut CreateTrigStmt;
        let mut qual: *mut Node = core::ptr::null_mut();
        let mut value: Datum;
        let mut isnull: bool = false;
        let mut cols: *mut List = NIL;
        let mut trigargs: *mut List = NIL;
        let oldcxt: MemoryContext;

        /* Ignore statement-level triggers; those are not cloned. */
        if !TRIGGER_FOR_ROW!((*trig_form).tgtype as u32) {
            continue;
        }

        /* Don't clone internal triggers */
        if (*trig_form).tgisinternal {
            continue;
        }

        /* Complain if we find an unexpected trigger type. */
        if !TRIGGER_FOR_BEFORE!((*trig_form).tgtype as u32)
            && !TRIGGER_FOR_AFTER!((*trig_form).tgtype as u32)
        {
            elog!(
                ERROR,
                "unexpected trigger \"{}\" found",
                CStr::from_ptr(NameStr!((*trig_form).tgname)).to_string_lossy()
            );
        }

        oldcxt = MemoryContextSwitchTo(per_tup_cxt);

        /* If there is a WHEN clause, generate a 'cooked' version of it. */
        value = heap_getattr(
            tuple,
            Anum_pg_trigger_tgqual,
            RelationGetDescr(pg_trigger),
            &mut isnull,
        );
        if !isnull {
            qual = stringToNode(TextDatumGetCString(value)) as *mut Node;
            qual = map_partition_varattnos(
                qual as *mut List,
                PRS2_OLD_VARNO as i32,
                partition,
                parent,
            ) as *mut Node;
            qual = map_partition_varattnos(
                qual as *mut List,
                PRS2_NEW_VARNO as i32,
                partition,
                parent,
            ) as *mut Node;
        }

        /* If there is a column list, transform it. */
        if (*trig_form).tgattr.dim1 > 0 {
            for i in 0..(*trig_form).tgattr.dim1 {
                let col: Form_pg_attribute = TupleDescAttr(
                    (*parent).rd_att,
                    (*trig_form).tgattr.values[i as usize] as usize - 1,
                ) as Form_pg_attribute;
                cols = lappend(
                    cols,
                    makeString(pstrdup(NameStr!((*col).attname)) as *mut libc::c_char)
                        as *mut libc::c_void,
                );
            }
        }

        /* Reconstruct trigger arguments list. */
        if (*trig_form).tgnargs > 0 {
            let mut p: *mut libc::c_char;

            value = heap_getattr(
                tuple,
                Anum_pg_trigger_tgargs,
                RelationGetDescr(pg_trigger),
                &mut isnull,
            );
            if isnull {
                elog!(
                    ERROR,
                    "tgargs is null for trigger \"{}\" in partition \"{}\"",
                    CStr::from_ptr(NameStr!((*trig_form).tgname)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(partition)).to_string_lossy()
                );
            }

            p = VARDATA_ANY!(DatumGetByteaPP(value)) as *mut libc::c_char;

            for _ in 0..(*trig_form).tgnargs {
                trigargs = lappend(
                    trigargs,
                    makeString(pstrdup(p) as *mut libc::c_char) as *mut libc::c_void,
                );
                p = p.add(libc::strlen(p) + 1);
            }
        }

        trig_stmt = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
        (*trig_stmt).replace = false;
        (*trig_stmt).isconstraint = OidIsValid((*trig_form).tgconstraint);
        (*trig_stmt).trigname = NameStr!((*trig_form).tgname) as *mut libc::c_char;
        (*trig_stmt).relation = core::ptr::null_mut();
        (*trig_stmt).funcname = core::ptr::null_mut(); /* passed separately */
        (*trig_stmt).args = trigargs;
        (*trig_stmt).row = true;
        (*trig_stmt).timing =
            ((*trig_form).tgtype & TRIGGER_TYPE_TIMING_MASK as i16) as i16;
        (*trig_stmt).events =
            ((*trig_form).tgtype & TRIGGER_TYPE_EVENT_MASK as i16) as i16;
        (*trig_stmt).columns = cols;
        (*trig_stmt).whenClause = core::ptr::null_mut(); /* passed separately */
        (*trig_stmt).transitionRels = NIL;
        (*trig_stmt).deferrable = (*trig_form).tgdeferrable;
        (*trig_stmt).initdeferred = (*trig_form).tginitdeferred;
        (*trig_stmt).constrrel = core::ptr::null_mut();

        CreateTriggerFiringOn(
            trig_stmt,
            core::ptr::null_mut(),
            RelationGetRelid(partition),
            (*trig_form).tgconstrrelid,
            InvalidOid,
            InvalidOid,
            (*trig_form).tgfoid,
            (*trig_form).oid,
            qual,
            false,
            true,
            (*trig_form).tgenabled,
        );

        MemoryContextSwitchTo(oldcxt);
        MemoryContextReset(per_tup_cxt);
    }

    MemoryContextDelete(per_tup_cxt);
    systable_endscan(scan);
    table_close(pg_trigger, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// ATExecDetachPartition (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecDetachPartition(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    name: *mut RangeVar,
    concurrent: bool,
) -> ObjectAddress {
    let mut part_rel: Relation;
    let address: ObjectAddress;
    let default_part_oid: Oid;
    let partdesc: PartitionDesc;

    partdesc = RelationGetPartitionDesc(rel, true);
    default_part_oid = get_default_oid_from_partdesc(partdesc);
    if OidIsValid(default_part_oid) {
        if concurrent {
            ereport!(
                ERROR,
                errmsg!("cannot detach partitions concurrently when a default partition exists")
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }
        LockRelationOid(default_part_oid, AccessExclusiveLock);
    }

    part_rel = table_openrv(
        name,
        if concurrent {
            ShareUpdateExclusiveLock
        } else {
            AccessExclusiveLock
        },
    );

    if !concurrent {
        RemoveInheritance(part_rel, rel, false);
    } else {
        MarkInheritDetached(part_rel, rel);
    }

    ATDetachCheckNoForeignKeyRefs(part_rel);

    if concurrent {
        let part_relid: Oid = RelationGetRelid(part_rel);
        let parent_relid: Oid = RelationGetRelid(rel);
        let mut tag: LOCKTAG = core::mem::zeroed();
        let parent_relname: *mut libc::c_char = MemoryContextStrdup(
            PortalContext,
            RelationGetRelationName(rel),
        );
        let part_relname: *mut libc::c_char = MemoryContextStrdup(
            PortalContext,
            RelationGetRelationName(part_rel),
        );

        if (*partdesc).boundinfo != core::ptr::null_mut()
            && (*(*partdesc).boundinfo).strategy != PARTITION_STRATEGY_HASH as libc::c_char
        {
            DetachAddConstraintIfNeeded(wqueue, part_rel);
        }

        CacheInvalidateRelcache(rel);

        table_close(part_rel, NoLock);
        table_close(rel, NoLock);
        (*tab).rel = core::ptr::null_mut();

        PopActiveSnapshot();
        CommitTransactionCommand();

        StartTransactionCommand();

        SET_LOCKTAG_RELATION!(tag, MyDatabaseId, parent_relid);
        let tag_list: *mut List = list_make1(&mut tag as *mut LOCKTAG as *mut libc::c_void);
        WaitForLockersMultiple(tag_list, AccessExclusiveLock, false);

        let rel_new = try_relation_open(parent_relid, ShareUpdateExclusiveLock);
        part_rel = try_relation_open(part_relid, AccessExclusiveLock);

        if rel_new.is_null() {
            if !part_rel.is_null() {
                elog!(
                    WARNING,
                    "dangling partition \"{}\" remains, can't fix",
                    CStr::from_ptr(part_relname).to_string_lossy()
                );
            }
            ereport!(
                ERROR,
                errmsg!(
                    "partitioned table \"{}\" was removed concurrently",
                    CStr::from_ptr(parent_relname).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }
        if part_rel.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "partition \"{}\" was removed concurrently",
                    CStr::from_ptr(part_relname).to_string_lossy()
                ) /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        (*tab).rel = rel_new;
        // re-bind rel to rel_new for remaining use (we must use rel_new going forward)
        let rel = rel_new;
        let _ = rel;
    }

    PushActiveSnapshot(GetTransactionSnapshot());
    DetachPartitionFinalize(rel, part_rel, concurrent, default_part_oid);
    PopActiveSnapshot();

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_rel));
    table_close(part_rel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// DetachPartitionFinalize (static)
// ---------------------------------------------------------------------------

unsafe fn DetachPartitionFinalize(
    rel: Relation,
    part_rel: Relation,
    concurrent: bool,
    default_part_oid: Oid,
) {
    let class_rel: Relation;
    let fks: *mut List;
    let mut cell: *mut ListCell;
    let indexes: *mut List;
    let mut new_val: [Datum; Natts_pg_class as usize] = core::mem::zeroed();
    let mut new_null: [bool; Natts_pg_class as usize] = core::mem::zeroed();
    let mut new_repl: [bool; Natts_pg_class as usize] = core::mem::zeroed();
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let mut trigrel: Relation = core::ptr::null_mut();
    let mut fkoids: *mut List = NIL;

    if concurrent {
        RemoveInheritance(part_rel, rel, true);
    }

    /* Drop any triggers that were cloned on creation/attach. */
    DropClonedTriggersFromPartition(RelationGetRelid(part_rel));

    /* Detach any foreign keys that are inherited. */
    fks = copyObject(RelationGetFKeyList(part_rel)) as *mut List;
    if fks != NIL {
        trigrel = table_open(TriggerRelationId, RowExclusiveLock);
    }

    /* Collect all FK OIDs first, to detect parent/child relationships */
    cell = list_head(fks);
    while !cell.is_null() {
        let fk: *mut ForeignKeyCacheInfo = lfirst(cell) as *mut ForeignKeyCacheInfo;
        fkoids = lappend_oid(fkoids, (*fk).conoid);
        cell = lnext(fks, cell);
    }

    cell = list_head(fks);
    while !cell.is_null() {
        let fk: *mut ForeignKeyCacheInfo = lfirst(cell) as *mut ForeignKeyCacheInfo;
        cell = lnext(fks, cell);
        let contup: HeapTuple;
        let conform: Form_pg_constraint;

        contup = SearchSysCache1(CONSTROID, ObjectIdGetDatum((*fk).conoid));
        if !HeapTupleIsValid(contup) {
            elog!(ERROR, "cache lookup failed for constraint {}", (*fk).conoid);
        }
        conform = GETSTRUCT(contup) as Form_pg_constraint;

        /* Consider only inherited foreign keys, and only if parent not in list */
        if (*conform).contype != CONSTRAINT_FOREIGN as libc::c_char
            || !OidIsValid((*conform).conparentid)
            || list_member_oid(fkoids, (*conform).conparentid)
        {
            ReleaseSysCache(contup);
            continue;
        }

        ConstraintSetParentConstraint((*fk).conoid, InvalidOid, InvalidOid);

        if (*fk).conenforced {
            let mut insert_trigger_oid: Oid = InvalidOid;
            let mut update_trigger_oid: Oid = InvalidOid;

            GetForeignKeyCheckTriggers(
                trigrel,
                (*fk).conoid,
                (*fk).confrelid,
                (*fk).conrelid,
                &mut insert_trigger_oid,
                &mut update_trigger_oid,
            );
            Assert!(OidIsValid(insert_trigger_oid));
            TriggerSetParentTrigger(
                trigrel,
                insert_trigger_oid,
                InvalidOid,
                RelationGetRelid(part_rel),
            );
            Assert!(OidIsValid(update_trigger_oid));
            TriggerSetParentTrigger(
                trigrel,
                update_trigger_oid,
                InvalidOid,
                RelationGetRelid(part_rel),
            );
        }

        {
            let fkconstraint: *mut Constraint =
                makeNode!(Constraint, T_Constraint) as *mut Constraint;
            let mut numfks: i32 = 0;
            let mut conkey: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut confkey: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conpfeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conppeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut conffeqop: [Oid; INDEX_MAX_KEYS] = core::mem::zeroed();
            let mut numfkdelsetcols: i32 = 0;
            let mut confdelsetcols: [AttrNumber; INDEX_MAX_KEYS] = core::mem::zeroed();
            let refd_rel: Relation;

            DeconstructFkConstraintRow(
                contup,
                &mut numfks,
                conkey.as_mut_ptr(),
                confkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                conppeqop.as_mut_ptr(),
                conffeqop.as_mut_ptr(),
                &mut numfkdelsetcols,
                confdelsetcols.as_mut_ptr(),
            );

            (*fkconstraint).contype = CONSTRAINT_FOREIGN;
            (*fkconstraint).conname = pstrdup(NameStr!((*conform).conname));
            (*fkconstraint).deferrable = (*conform).condeferrable;
            (*fkconstraint).initdeferred = (*conform).condeferred;
            (*fkconstraint).is_enforced = (*conform).conenforced;
            (*fkconstraint).skip_validation = true;
            (*fkconstraint).initially_valid = (*conform).convalidated;
            (*fkconstraint).pktable = core::ptr::null_mut();
            (*fkconstraint).fk_attrs = NIL;
            (*fkconstraint).pk_attrs = NIL;
            (*fkconstraint).fk_matchtype = (*conform).confmatchtype;
            (*fkconstraint).fk_upd_action = (*conform).confupdtype;
            (*fkconstraint).fk_del_action = (*conform).confdeltype;
            (*fkconstraint).fk_del_set_cols = NIL;
            (*fkconstraint).old_conpfeqop = NIL;
            (*fkconstraint).old_pktable_oid = InvalidOid;
            (*fkconstraint).location = -1;

            for i in 0..numfks as usize {
                let att: Form_pg_attribute = TupleDescAttr(
                    RelationGetDescr(part_rel),
                    conkey[i] as usize - 1,
                ) as Form_pg_attribute;
                (*fkconstraint).fk_attrs = lappend(
                    (*fkconstraint).fk_attrs,
                    makeString(NameStr!((*att).attname) as *mut libc::c_char) as *mut libc::c_void,
                );
            }

            refd_rel = table_open((*fk).confrelid, ShareRowExclusiveLock);

            addFkRecurseReferenced(
                fkconstraint,
                part_rel,
                refd_rel,
                (*conform).conindid,
                (*fk).conoid,
                numfks,
                confkey.as_mut_ptr(),
                conkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                conppeqop.as_mut_ptr(),
                conffeqop.as_mut_ptr(),
                numfkdelsetcols,
                confdelsetcols.as_mut_ptr(),
                true,
                InvalidOid,
                InvalidOid,
                (*conform).conperiod,
            );
            table_close(refd_rel, NoLock);
        }

        ReleaseSysCache(contup);
    }
    list_free_deep(fks);
    if !trigrel.is_null() {
        table_close(trigrel, RowExclusiveLock);
    }

    /* Remove sub-constraints that are in the referenced-side of a larger constraint */
    let parent_fk_refs: *mut List = GetParentedForeignKeyRefs(part_rel);
    cell = list_head(parent_fk_refs);
    while !cell.is_null() {
        let constr_oid: Oid = lfirst_oid(cell);
        let mut constraint: ObjectAddress = core::mem::zeroed();
        cell = lnext(parent_fk_refs, cell);

        ConstraintSetParentConstraint(constr_oid, InvalidOid, InvalidOid);
        deleteDependencyRecordsForClass(
            ConstraintRelationId,
            constr_oid,
            ConstraintRelationId,
            DEPENDENCY_INTERNAL,
        );
        CommandCounterIncrement();

        ObjectAddressSet!(constraint, ConstraintRelationId, constr_oid);
        performDeletion(&constraint, DROP_RESTRICT, 0);
    }

    /* Now we can detach indexes */
    indexes = RelationGetIndexList(part_rel);
    cell = list_head(indexes);
    while !cell.is_null() {
        let idxid: Oid = lfirst_oid(cell);
        cell = lnext(indexes, cell);
        let parent_idx: Oid;
        let idx: Relation;
        let constr_oid: Oid;
        let parent_constr_oid: Oid;

        if !has_superclass(idxid) {
            continue;
        }

        parent_idx = get_partition_parent(idxid, false);
        Assert!(IndexGetRelation(parent_idx, false) == RelationGetRelid(rel));

        idx = index_open(idxid, AccessExclusiveLock);
        IndexSetParentIndex(idx, InvalidOid);

        constr_oid =
            get_relation_idx_constraint_oid(RelationGetRelid(part_rel), idxid);
        parent_constr_oid =
            get_relation_idx_constraint_oid(RelationGetRelid(rel), parent_idx);
        if OidIsValid(parent_constr_oid) && OidIsValid(constr_oid) {
            ConstraintSetParentConstraint(constr_oid, InvalidOid, InvalidOid);
        }

        index_close(idx, NoLock);
    }

    /* Update pg_class tuple */
    class_rel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(part_rel)));
    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for relation {}",
            RelationGetRelid(part_rel)
        );
    }
    Assert!((*(GETSTRUCT(tuple) as Form_pg_class)).relispartition);

    libc::memset(new_val.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_val));
    libc::memset(new_null.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_null));
    libc::memset(new_repl.as_mut_ptr() as *mut libc::c_void, 0, core::mem::size_of_val(&new_repl));
    new_val[Anum_pg_class_relpartbound as usize - 1] = 0;
    new_null[Anum_pg_class_relpartbound as usize - 1] = true;
    new_repl[Anum_pg_class_relpartbound as usize - 1] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(class_rel),
        new_val.as_mut_ptr(),
        new_null.as_mut_ptr(),
        new_repl.as_mut_ptr(),
    );

    (*(GETSTRUCT(newtuple) as Form_pg_class)).relispartition = false;
    CatalogTupleUpdate(class_rel, &(*newtuple).t_self, newtuple);
    heap_freetuple(newtuple);
    table_close(class_rel, RowExclusiveLock);

    /* Drop identity property from all identity columns of partition. */
    for attno in 0..RelationGetNumberOfAttributes(part_rel) {
        let attr: Form_pg_attribute =
            TupleDescAttr((*part_rel).rd_att, attno as usize) as Form_pg_attribute;
        if !(*attr).attisdropped && (*attr).attidentity != 0 {
            ATExecDropIdentity(
                part_rel,
                NameStr!((*attr).attname),
                false,
                AccessExclusiveLock,
                true,
                true,
            );
        }
    }

    if OidIsValid(default_part_oid) {
        if RelationGetRelid(part_rel) == default_part_oid {
            update_default_partition_oid(RelationGetRelid(rel), InvalidOid);
        } else {
            CacheInvalidateRelcacheByRelid(default_part_oid);
        }
    }

    CacheInvalidateRelcache(rel);

    if (*(*part_rel).rd_rel).relkind as u8 == RELKIND_PARTITIONED_TABLE {
        let children: *mut List = find_all_inheritors(
            RelationGetRelid(part_rel),
            AccessExclusiveLock,
            core::ptr::null_mut(),
        );
        cell = list_head(children);
        while !cell.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(cell));
            cell = lnext(children, cell);
        }
    }
}

// ---------------------------------------------------------------------------
// ATExecDetachPartitionFinalize (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecDetachPartitionFinalize(rel: Relation, name: *mut RangeVar) -> ObjectAddress {
    let part_rel: Relation;
    let address: ObjectAddress;
    let snap: Snapshot = GetActiveSnapshot();

    part_rel = table_openrv(name, AccessExclusiveLock);

    WaitForOlderSnapshots((*snap).xmin, false);

    DetachPartitionFinalize(rel, part_rel, true, InvalidOid);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_rel));
    table_close(part_rel, NoLock);
    address
}

// ---------------------------------------------------------------------------
// DetachAddConstraintIfNeeded (static)
// ---------------------------------------------------------------------------

unsafe fn DetachAddConstraintIfNeeded(wqueue: *mut *mut List, part_rel: Relation) {
    let mut constraint_expr: *mut List;

    constraint_expr = RelationGetPartitionQual(part_rel);
    constraint_expr =
        eval_const_expressions(core::ptr::null_mut(), constraint_expr as *mut Node)
            as *mut List;

    if !PartConstraintImpliedByRelConstraint(part_rel, constraint_expr) {
        let tab: *mut AlteredTableInfo;
        let n: *mut Constraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;

        tab = ATGetQueueEntry(wqueue, part_rel);

        (*n).contype = CONSTR_CHECK;
        (*n).conname = core::ptr::null_mut();
        (*n).location = -1;
        (*n).is_no_inherit = false;
        (*n).raw_expr = core::ptr::null_mut();
        (*n).cooked_expr = nodeToString(make_ands_explicit(constraint_expr) as *mut libc::c_void);
        (*n).is_enforced = true;
        (*n).initially_valid = true;
        (*n).skip_validation = true;

        ATAddCheckNNConstraint(
            wqueue,
            tab,
            part_rel,
            n,
            true,
            false,
            true,
            ShareUpdateExclusiveLock,
        );
    }
}

// ---------------------------------------------------------------------------
// DropClonedTriggersFromPartition (static)
// ---------------------------------------------------------------------------

unsafe fn DropClonedTriggersFromPartition(partition_id: Oid) {
    let mut skey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;
    let tgrel: Relation;
    let objects: *mut ObjectAddresses;

    objects = new_object_addresses();

    ScanKeyInit(
        &mut skey,
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(partition_id),
    );
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    scan = systable_beginscan(
        tgrel,
        TriggerRelidNameIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut skey,
    );

    loop {
        trigtup = systable_getnext(scan);
        if !HeapTupleIsValid(trigtup) {
            break;
        }
        let pg_trigger: Form_pg_trigger = GETSTRUCT(trigtup) as Form_pg_trigger;
        let mut trig: ObjectAddress = core::mem::zeroed();

        /* Ignore triggers that weren't cloned */
        if !OidIsValid((*pg_trigger).tgparentid) {
            continue;
        }

        /*
         * Ignore internal triggers that are implementation objects of foreign
         * keys.
         */
        if OidIsValid((*pg_trigger).tgconstrrelid) {
            continue;
        }

        deleteDependencyRecordsForClass(
            TriggerRelationId,
            (*pg_trigger).oid,
            TriggerRelationId,
            DEPENDENCY_PARTITION_PRI,
        );
        deleteDependencyRecordsForClass(
            TriggerRelationId,
            (*pg_trigger).oid,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC,
        );

        ObjectAddressSet!(trig, TriggerRelationId, (*pg_trigger).oid);
        add_exact_object_address(&trig, objects);
    }

    CommandCounterIncrement();
    performMultipleDeletions(objects, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

    free_object_addresses(objects);
    systable_endscan(scan);
    table_close(tgrel, RowExclusiveLock);
}

// ---------------------------------------------------------------------------
// AttachIndexCallbackState (struct) and RangeVarCallbackForAttachIndex
// ---------------------------------------------------------------------------

#[repr(C)]
struct AttachIndexCallbackState {
    partition_oid: Oid,
    parent_tbl_oid: Oid,
    locked_parent_tbl: bool,
}

unsafe extern "C" fn RangeVarCallbackForAttachIndex(
    rv: *const RangeVar,
    rel_oid: Oid,
    old_rel_oid: Oid,
    arg: *mut libc::c_void,
) {
    let state: *mut AttachIndexCallbackState = arg as *mut AttachIndexCallbackState;
    let classform: Form_pg_class;
    let tuple: HeapTuple;

    if !(*state).locked_parent_tbl {
        LockRelationOid((*state).parent_tbl_oid, AccessShareLock);
        (*state).locked_parent_tbl = true;
    }

    if rel_oid != old_rel_oid && OidIsValid((*state).partition_oid) {
        UnlockRelationOid((*state).partition_oid, AccessShareLock);
        (*state).partition_oid = InvalidOid;
    }

    if !OidIsValid(rel_oid) {
        return;
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
    if !HeapTupleIsValid(tuple) {
        return; /* concurrently dropped */
    }
    classform = GETSTRUCT(tuple) as Form_pg_class;
    if (*classform).relkind as u8 != RELKIND_PARTITIONED_INDEX
        && (*classform).relkind as u8 != RELKIND_INDEX
    {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not an index",
                CStr::from_ptr((*rv).relname).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
        );
    }
    ReleaseSysCache(tuple);

    (*state).partition_oid = IndexGetRelation(rel_oid, false);
    LockRelationOid((*state).partition_oid, AccessShareLock);
}

// ---------------------------------------------------------------------------
// ATExecAttachPartitionIdx (static)
// ---------------------------------------------------------------------------

unsafe fn ATExecAttachPartitionIdx(
    wqueue: *mut *mut List,
    parent_idx: Relation,
    name: *mut RangeVar,
) -> ObjectAddress {
    let part_idx: Relation;
    let part_tbl: Relation;
    let parent_tbl: Relation;
    let address: ObjectAddress;
    let part_idx_id: Oid;
    let curr_parent: Oid;
    let mut state: AttachIndexCallbackState = AttachIndexCallbackState {
        partition_oid: InvalidOid,
        parent_tbl_oid: (*(*parent_idx).rd_index).indrelid,
        locked_parent_tbl: false,
    };

    part_idx_id = RangeVarGetRelidExtended(
        name,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForAttachIndex),
        &mut state as *mut AttachIndexCallbackState as *mut libc::c_void,
    );

    if !OidIsValid(part_idx_id) {
        ereport!(
            ERROR,
            errmsg!(
                "index \"{}\" does not exist",
                CStr::from_ptr((*name).relname).to_string_lossy()
            ) /* errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    part_idx = relation_open(part_idx_id, AccessExclusiveLock);
    parent_tbl = relation_open((*(*parent_idx).rd_index).indrelid, AccessShareLock);
    part_tbl = relation_open((*(*part_idx).rd_index).indrelid, NoLock);

    ObjectAddressSet!(address, RelationRelationId, RelationGetRelid(part_idx));

    /* Silently do nothing if already in the right state */
    curr_parent = if (*(*part_idx).rd_rel).relispartition {
        get_partition_parent(part_idx_id, false)
    } else {
        InvalidOid
    };

    if curr_parent != RelationGetRelid(parent_idx) {
        let child_info: *mut IndexInfo;
        let parent_info: *mut IndexInfo;
        let attmap: *mut AttrMap;
        let mut found: bool;
        let part_desc: PartitionDesc;
        let constraint_oid: Oid;
        let mut cld_constr_id: Oid = InvalidOid;

        refuseDupeIndexAttach(parent_idx, part_idx, part_tbl);

        if OidIsValid(curr_parent) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                   errdetail("Index ... is already attached to another index.") */
            );
        }

        /* Make sure it indexes a partition of the other index's table */
        part_desc = RelationGetPartitionDesc(parent_tbl, true);
        found = false;
        for i in 0..(*part_desc).nparts {
            if *(*part_desc).oids.add(i as usize) == state.partition_oid {
                found = true;
                break;
            }
        }
        if !found {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errdetail("Index ... is not an index on any partition of table ...") */
            );
        }

        /* Ensure the indexes are compatible */
        child_info = BuildIndexInfo(part_idx);
        parent_info = BuildIndexInfo(parent_idx);
        attmap = build_attrmap_by_name(
            RelationGetDescr(part_tbl),
            RelationGetDescr(parent_tbl),
            false,
        );
        if !CompareIndexInfo(
            child_info,
            parent_info,
            (*part_idx).rd_indcollation,
            (*parent_idx).rd_indcollation,
            (*part_idx).rd_opfamily,
            (*parent_idx).rd_opfamily,
            attmap,
        ) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                )
                /* errdetail("The index definitions do not match.") */
            );
        }

        constraint_oid = get_relation_idx_constraint_oid(
            RelationGetRelid(parent_tbl),
            RelationGetRelid(parent_idx),
        );

        if OidIsValid(constraint_oid) {
            cld_constr_id =
                get_relation_idx_constraint_oid(RelationGetRelid(part_tbl), part_idx_id);
            if !OidIsValid(cld_constr_id) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
                    )
                    /* errdetail("The index ... belongs to a constraint...") */
                );
            }
        }

        if (*(*parent_idx).rd_index).indisprimary {
            verifyPartitionIndexNotNull(child_info, part_tbl);
        }

        IndexSetParentIndex(part_idx, RelationGetRelid(parent_idx));
        if OidIsValid(constraint_oid) {
            ConstraintSetParentConstraint(
                cld_constr_id,
                constraint_oid,
                RelationGetRelid(part_tbl),
            );
        }

        free_attrmap(attmap);
        validatePartitionedIndex(parent_idx, parent_tbl);
    }

    relation_close(parent_tbl, AccessShareLock);
    relation_close(part_tbl, NoLock);
    relation_close(part_idx, NoLock);

    let _ = wqueue;
    address
}

// ---------------------------------------------------------------------------
// refuseDupeIndexAttach (static)
// ---------------------------------------------------------------------------

unsafe fn refuseDupeIndexAttach(
    parent_idx: Relation,
    part_idx: Relation,
    partition_tbl: Relation,
) {
    let existing_idx: Oid;

    existing_idx = index_get_partition(partition_tbl, RelationGetRelid(parent_idx));
    if OidIsValid(existing_idx) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot attach index \"{}\" as a partition of index \"{}\"",
                CStr::from_ptr(RelationGetRelationName(part_idx)).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(parent_idx)).to_string_lossy()
            )
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail("Another index is already attached for partition ...") */
        );
    }
}

// ---------------------------------------------------------------------------
// validatePartitionedIndex (static)
// ---------------------------------------------------------------------------

unsafe fn validatePartitionedIndex(parted_idx: Relation, parted_tbl: Relation) {
    let inherits_rel: Relation;
    let scan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut tuples: i32 = 0;
    let mut inh_tup: HeapTuple;
    let mut updated: bool = false;

    Assert!((*(*parted_idx).rd_rel).relkind as u8 == RELKIND_PARTITIONED_INDEX);

    inherits_rel = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhparent,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parted_idx)),
    );
    scan = systable_beginscan(
        inherits_rel,
        InheritsParentIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    loop {
        inh_tup = systable_getnext(scan);
        if inh_tup.is_null() {
            break;
        }
        let inh_form: Form_pg_inherits = GETSTRUCT(inh_tup) as Form_pg_inherits;
        let ind_tup: HeapTuple;
        let index_form: Form_pg_index;

        ind_tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum((*inh_form).inhrelid));
        if !HeapTupleIsValid(ind_tup) {
            elog!(ERROR, "cache lookup failed for index {}", (*inh_form).inhrelid);
        }
        index_form = GETSTRUCT(ind_tup) as Form_pg_index;
        if (*index_form).indisvalid {
            tuples += 1;
        }
        ReleaseSysCache(ind_tup);
    }

    systable_endscan(scan);
    table_close(inherits_rel, AccessShareLock);

    if tuples == (*RelationGetPartitionDesc(parted_tbl, true)).nparts {
        let idx_rel: Relation;
        let ind_tup: HeapTuple;
        let index_form: Form_pg_index;

        idx_rel = table_open(IndexRelationId, RowExclusiveLock);
        ind_tup = SearchSysCacheCopy1(
            INDEXRELID,
            ObjectIdGetDatum(RelationGetRelid(parted_idx)),
        );
        if !HeapTupleIsValid(ind_tup) {
            elog!(
                ERROR,
                "cache lookup failed for index {}",
                RelationGetRelid(parted_idx)
            );
        }
        index_form = GETSTRUCT(ind_tup) as Form_pg_index;
        (*index_form).indisvalid = true;
        updated = true;
        CatalogTupleUpdate(idx_rel, &(*ind_tup).t_self, ind_tup);
        table_close(idx_rel, RowExclusiveLock);
        heap_freetuple(ind_tup);
    }

    if updated && (*(*parted_idx).rd_rel).relispartition {
        let parent_idx_id: Oid;
        let parent_tbl_id: Oid;
        let parent_idx: Relation;
        let parent_tbl: Relation;

        CommandCounterIncrement();

        parent_idx_id = get_partition_parent(RelationGetRelid(parted_idx), false);
        parent_tbl_id = get_partition_parent(RelationGetRelid(parted_tbl), false);
        parent_idx = relation_open(parent_idx_id, AccessExclusiveLock);
        parent_tbl = relation_open(parent_tbl_id, AccessExclusiveLock);
        Assert!(!(*(*parent_idx).rd_index).indisvalid);

        validatePartitionedIndex(parent_idx, parent_tbl);

        relation_close(parent_idx, AccessExclusiveLock);
        relation_close(parent_tbl, AccessExclusiveLock);
    }
}

// ---------------------------------------------------------------------------
// verifyPartitionIndexNotNull (static)
// ---------------------------------------------------------------------------

unsafe fn verifyPartitionIndexNotNull(iinfo: *mut IndexInfo, partition: Relation) {
    for i in 0..(*iinfo).ii_NumIndexKeyAttrs as usize {
        let att: Form_pg_attribute = TupleDescAttr(
            RelationGetDescr(partition),
            (*iinfo).ii_IndexAttrNumbers[i] as usize - 1,
        ) as Form_pg_attribute;

        if !(*att).attnotnull {
            ereport!(
                ERROR,
                errmsg!("invalid primary key definition")
                /* errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                   errdetail("Column ... of relation ... is not marked NOT NULL.") */
            );
        }
    }
}

// ---------------------------------------------------------------------------
// GetParentedForeignKeyRefs (static)
// ---------------------------------------------------------------------------

unsafe fn GetParentedForeignKeyRefs(partition: Relation) -> *mut List {
    let pg_constraint: Relation;
    let mut tuple: HeapTuple;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let mut constraints: *mut List = NIL;

    if RelationGetIndexList(partition) == NIL
        || bms_is_empty(RelationGetIndexAttrBitmap(partition, INDEX_ATTR_BITMAP_KEY))
    {
        return NIL;
    }

    pg_constraint = table_open(ConstraintRelationId, AccessShareLock);
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_confrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(partition)),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_constraint_contype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(CONSTRAINT_FOREIGN as libc::c_char as Datum),
    );

    scan = systable_beginscan(
        pg_constraint,
        InvalidOid,
        true,
        core::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );
    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let constr_form: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

        if !OidIsValid((*constr_form).conparentid) {
            continue;
        }

        constraints = lappend_oid(constraints, (*constr_form).oid);
    }

    systable_endscan(scan);
    table_close(pg_constraint, AccessShareLock);

    constraints
}

// ---------------------------------------------------------------------------
// ATDetachCheckNoForeignKeyRefs (static)
// ---------------------------------------------------------------------------

unsafe fn ATDetachCheckNoForeignKeyRefs(partition: Relation) {
    let constraints: *mut List;
    let mut cell: *mut ListCell;

    constraints = GetParentedForeignKeyRefs(partition);

    cell = list_head(constraints);
    while !cell.is_null() {
        let constr_oid: Oid = lfirst_oid(cell);
        cell = lnext(constraints, cell);
        let tuple: HeapTuple;
        let constr_form: Form_pg_constraint;
        let rel: Relation;
        let mut trig: Trigger = core::mem::zeroed();

        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constr_oid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", constr_oid);
        }
        constr_form = GETSTRUCT(tuple) as Form_pg_constraint;

        Assert!(OidIsValid((*constr_form).conparentid));
        Assert!((*constr_form).confrelid == RelationGetRelid(partition));

        rel = table_open((*constr_form).conrelid, ShareLock);

        trig.tgoid = InvalidOid;
        trig.tgname = NameStr!((*constr_form).conname) as *mut libc::c_char;
        trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
        trig.tgisinternal = true;
        trig.tgconstrrelid = RelationGetRelid(partition);
        trig.tgconstrindid = (*constr_form).conindid;
        trig.tgconstraint = (*constr_form).oid;
        trig.tgdeferrable = false;
        trig.tginitdeferred = false;

        RI_PartitionRemove_Check(&trig, rel, partition);

        ReleaseSysCache(tuple);
        table_close(rel, NoLock);
    }
}

// ---------------------------------------------------------------------------
// GetAttributeCompression
// ---------------------------------------------------------------------------

unsafe fn GetAttributeCompression(
    atttypid: Oid,
    compression: *const libc::c_char,
) -> libc::c_char {
    let cmethod: libc::c_char;

    if compression.is_null()
        || libc::strcmp(compression, c"default".as_ptr()) == 0
    {
        return InvalidCompressionMethod as libc::c_char;
    }

    if !TypeIsToastable(atttypid) {
        ereport!(
            ERROR,
            errmsg!(
                "column data type {} does not support compression",
                CStr::from_ptr(format_type_be(atttypid)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    cmethod = CompressionNameToMethod(compression);
    if !CompressionMethodIsValid(cmethod) {
        ereport!(
            ERROR,
            errmsg!(
                "invalid compression method \"{}\"",
                CStr::from_ptr(compression).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    cmethod
}

// ---------------------------------------------------------------------------
// GetAttributeStorage
// ---------------------------------------------------------------------------

unsafe fn GetAttributeStorage(
    atttypid: Oid,
    storagemode: *const libc::c_char,
) -> libc::c_char {
    let cstorage: u8;

    if pg_strcasecmp(storagemode, c"plain".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_PLAIN;
    } else if pg_strcasecmp(storagemode, c"external".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_EXTERNAL;
    } else if pg_strcasecmp(storagemode, c"extended".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_EXTENDED;
    } else if pg_strcasecmp(storagemode, c"main".as_ptr()) == 0 {
        cstorage = TYPSTORAGE_MAIN;
    } else if pg_strcasecmp(storagemode, c"default".as_ptr()) == 0 {
        cstorage = get_typstorage(atttypid);
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "invalid storage type \"{}\"",
                CStr::from_ptr(storagemode).to_string_lossy()
            ) /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
        unreachable!();
    }

    if !(cstorage == TYPSTORAGE_PLAIN || TypeIsToastable(atttypid)) {
        ereport!(
            ERROR,
            errmsg!(
                "column data type {} can only have storage PLAIN",
                CStr::from_ptr(format_type_be(atttypid)).to_string_lossy()
            ) /* errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    cstorage as libc::c_char
}

// ---------------------------------------------------------------------------
// TODO(pg-port) stubs for new dependencies not in head/earlier sections
// ---------------------------------------------------------------------------

// TODO(pg-port): StoreCatalogInheritance1 -- adds a single pg_inherits row
// TODO(pg-port): child_dependency_type -- returns DEPENDENCY_AUTO for partitioned, else DEPENDENCY_NORMAL
// TODO(pg-port): DeleteInheritsTuple -- removes pg_inherits row for (child, parent)
// TODO(pg-port): extractNotNullColumn -- returns attnum from a not-null constraint tuple
// TODO(pg-port): findNotNullConstraintAttnum -- finds not-null constraint tuple by attnum
// TODO(pg-port): build_attrmap_by_name -- builds AttrMap mapping attrs by name
// TODO(pg-port): pg_add_s16_overflow -- wrapping addition with overflow check
// TODO(pg-port): check_new_partition_bound -- validates new partition bound
// TODO(pg-port): StorePartitionBound -- stores partition bound in pg_class
// TODO(pg-port): get_qual_from_partbound -- generates partition constraint from bound
// TODO(pg-port): RelationGetPartitionQual -- gets partition constraint qual list
// TODO(pg-port): map_partition_varattnos -- remaps varattnos for partition
// TODO(pg-port): get_proposed_default_constraint -- gets default partition constraint
// TODO(pg-port): list_concat_copy -- concatenates two lists (copy)
// TODO(pg-port): make_ands_explicit -- converts implicit-AND list to explicit AND node
// TODO(pg-port): make_ands_implicit -- converts AND expr to implicit-AND list
// TODO(pg-port): eval_const_expressions -- simplifies constant expressions
// TODO(pg-port): canonicalize_qual -- canonicalizes qual expression
// TODO(pg-port): predicate_implied_by -- tests if constraints imply a predicate
// TODO(pg-port): stringToNode -- deserializes a node from its string representation
// TODO(pg-port): nodeToString -- serializes a node to string representation
// TODO(pg-port): list_make1 -- creates a one-element list
// TODO(pg-port): list_copy -- shallow-copies a list
// TODO(pg-port): WaitForOlderSnapshots -- waits for snapshots older than given xmin
// TODO(pg-port): WaitForLockersMultiple -- waits for all lockers of given lock tags
// TODO(pg-port): SET_LOCKTAG_RELATION macro -- initializes a LOCKTAG for a relation
// TODO(pg-port): StartTransactionCommand / CommitTransactionCommand -- xact boundaries
// TODO(pg-port): PushActiveSnapshot / PopActiveSnapshot -- snapshot stack
// TODO(pg-port): GetTransactionSnapshot -- returns current transaction snapshot
// TODO(pg-port): GetCurrentSubTransactionId -- returns current subtransaction ID
// TODO(pg-port): InvalidSubTransactionId -- sentinel for no subtransaction
// TODO(pg-port): MyXactFlags / XACT_FLAGS_ACCESSEDTEMPNAMESPACE -- xact flags
// TODO(pg-port): PortalContext -- memory context for portal
// TODO(pg-port): CacheMemoryContext -- memory context for caches
// TODO(pg-port): lcons -- prepend element to list
// TODO(pg-port): heap_truncate -- truncates given relations
// TODO(pg-port): performMultipleDeletions -- performs cascaded object deletions
// TODO(pg-port): PERFORM_DELETION_INTERNAL / PERFORM_DELETION_QUIETLY -- flags
// TODO(pg-port): new_object_addresses / free_object_addresses / add_exact_object_address -- object-address sets
// TODO(pg-port): object_address_present -- checks if address is in set
// TODO(pg-port): CloneForeignKeyConstraints -- clones FK constraints to partition
// TODO(pg-port): addFkRecurseReferenced -- adds FK referenced-side triggers
// TODO(pg-port): DeconstructFkConstraintRow -- deconstructs FK constraint row
// TODO(pg-port): GetForeignKeyCheckTriggers -- finds FK check triggers by constraint
// TODO(pg-port): TriggerSetParentTrigger -- sets parent trigger on a trigger
// TODO(pg-port): ConstraintSetParentConstraint -- sets parent constraint
// TODO(pg-port): IndexSetParentIndex -- sets parent index on partition index
// TODO(pg-port): CompareIndexInfo -- compares two IndexInfo structures
// TODO(pg-port): BuildIndexInfo -- builds IndexInfo for an index relation
// TODO(pg-port): generateClonedIndexStmt -- generates IndexStmt clone for partition
// TODO(pg-port): DefineIndex -- creates an index
// TODO(pg-port): index_get_partition -- finds partition index for a given parent index
// TODO(pg-port): get_partition_parent -- gets parent of a partition
// TODO(pg-port): has_superclass -- checks if relation has a superclass
// TODO(pg-port): IndexGetRelation -- gets relation OID for an index
// TODO(pg-port): ATAddCheckNNConstraint -- adds CHECK/NOT NULL constraint
// TODO(pg-port): RI_PartitionRemove_Check -- validates RI when removing partition
// TODO(pg-port): deleteDependencyRecordsForClass -- deletes dependency records
// TODO(pg-port): changeDependencyFor -- changes a dependency entry
// TODO(pg-port): update_default_partition_oid -- updates default partition in pg_partitioned_table
// TODO(pg-port): AlterTypeNamespaceInternal -- moves type to new namespace
// TODO(pg-port): AlterConstraintNamespaces -- moves constraints to new namespace
// TODO(pg-port): CheckSetNamespace -- validates namespace change
// TODO(pg-port): RangeVarGetAndCheckCreationNamespace -- gets/validates namespace OID
// TODO(pg-port): sequenceIsOwned -- checks if sequence is owned by a column
// TODO(pg-port): GetRelationPublications -- gets publications for a relation
// TODO(pg-port): RelationIsPermanent -- checks if relation is permanent
// TODO(pg-port): typenameType -- looks up type by TypeName
// TODO(pg-port): check_of_type -- validates type for OF TABLE
// TODO(pg-port): lookup_rowtype_tupdesc -- gets TupleDesc for a row type
// TODO(pg-port): recordDependencyOn -- records a dependency
// TODO(pg-port): GetForeignServer / GetForeignDataWrapper -- FDW metadata access
// TODO(pg-port): transformGenericOptions -- transforms generic FDW options
// TODO(pg-port): CompressionNameToMethod / CompressionMethodIsValid -- compression utilities
// TODO(pg-port): InvalidCompressionMethod -- sentinel compression method value
// TODO(pg-port): TypeIsToastable -- checks if type can be toasted
// TODO(pg-port): get_typstorage -- gets default storage for a type
// TODO(pg-port): pg_strcasecmp -- case-insensitive strcmp
// TODO(pg-port): SetIndexStorageProperties -- applies storage properties to index columns
// TODO(pg-port): FindTriggerIncompatibleWithInheritance -- finds incompatible triggers
// TODO(pg-port): CreateTriggerFiringOn -- creates trigger with given firing conditions
// TODO(pg-port): GetActiveSnapshot -- returns active snapshot
// TODO(pg-port): on_commits static -- list of OnCommitItem entries
// TODO(pg-port): ATGetQueueEntry -- gets/creates AlteredTableInfo entry in work queue
// TODO(pg-port): RelationGetFKeyList -- returns foreign key list for relation
// TODO(pg-port): list_free_deep -- frees list and all elements
// TODO(pg-port): IndexRelationGetNumberOfKeyAttributes -- returns number of key attrs
// TODO(pg-port): RelationGetIndexExpressions / RelationGetIndexPredicate -- index metadata
// TODO(pg-port): CacheInvalidateRelcache / CacheInvalidateRelcacheByRelid -- cache invalidation
