// section: tablecmds_mid  (C lines 7217-14726)

// ---------------------------------------------------------------------------
// ATExecAddColumn  (continued from head section -- function starts at 7217
// which is the opening of the function body; signature is in head)
// ---------------------------------------------------------------------------

unsafe fn at_exec_add_column(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    cmd: *mut *mut AlterTableCmd,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: *mut AlterTableUtilityContext,
) -> ObjectAddress {
    let myrelid = RelationGetRelid(rel);
    let col_def = castNode!(ColumnDef, T_ColumnDef, (*(*cmd)).def);
    let if_not_exists = (*(*cmd)).missing_ok;
    let pgclass: Relation;
    let attrdesc: Relation;
    let reltup: HeapTuple;
    let relform: Form_pg_class;
    let attribute: Form_pg_attribute;
    let newattnum: i32;
    let relkind: i8;
    let mut defval: *mut Expr = std::ptr::null_mut();
    let children: *mut List;
    let address: ObjectAddress;
    let mut tupdesc: TupleDesc = std::ptr::null_mut();

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            (*(*cmd)).subtype,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("cannot add column to a partition")
        );
    }

    attrdesc = table_open(AttributeRelationId, RowExclusiveLock);

    /*
     * Are we adding the column to a recursion child?  If so, check whether to
     * merge with an existing definition for the column.  If we do merge, we
     * must not recurse.  Children will already have the column, and recursing
     * into them would mess up attinhcount.
     */
    if (*col_def).inhcount > 0 {
        let tuple: HeapTuple;
        /* Does child already have a column by this name? */
        tuple = SearchSysCacheCopyAttName(myrelid, (*col_def).colname);
        if HeapTupleIsValid(tuple) {
            let childatt = GETSTRUCT(tuple) as Form_pg_attribute;
            let mut ctypeid: Oid = InvalidOid;
            let mut ctypmod: i32 = 0;
            let ccollid: Oid;

            /* Child column must match on type, typmod, and collation */
            typenameTypeIdAndMod(
                std::ptr::null_mut(),
                (*col_def).typeName,
                &mut ctypeid,
                &mut ctypmod,
            );
            if ctypeid != (*childatt).atttypid || ctypmod != (*childatt).atttypmod {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg!(
                        "child table \"{}\" has different type for column \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy()
                    )
                );
            }
            ccollid = GetColumnDefCollation(std::ptr::null_mut(), col_def, ctypeid);
            if ccollid != (*childatt).attcollation {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_COLLATION_MISMATCH),
                    errmsg!(
                        "child table \"{}\" has different collation for column \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy()
                    )
                    /* errdetail: "%s" versus "%s" */
                );
            }

            /* Bump the existing child att's inhcount */
            if pg_add_s16_overflow(
                (*childatt).attinhcount,
                1,
                &mut (*childatt).attinhcount,
            ) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg!("too many inheritance parents")
                );
            }
            CatalogTupleUpdate(attrdesc, &mut (*tuple).t_self, tuple);
            heap_freetuple(tuple);

            /* Inform the user about the merge */
            ereport!(
                NOTICE,
                errmsg!(
                    "merging definition of column \"{}\" for child \"{}\"",
                    std::ffi::CStr::from_ptr((*col_def).colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );

            table_close(attrdesc, RowExclusiveLock);
            /* Make the child column change visible */
            CommandCounterIncrement();
            return InvalidObjectAddress;
        }
    }

    /* skip if the name already exists and if_not_exists is true */
    if !check_for_column_name_collision(rel, (*col_def).colname, if_not_exists) {
        table_close(attrdesc, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    /*
     * Okay, we need to add the column, so go ahead and do parse transformation.
     * When recursing, the command was already transformed.
     */
    if !context.is_null() && !recursing {
        *cmd = ATParseTransformCmd(
            wqueue, tab, rel, *cmd, recurse, lockmode, cur_pass, context,
        );
        Assert!(!(*cmd).is_null());
        // col_def re-cast after transform
        let _ = castNode!(ColumnDef, T_ColumnDef, (**cmd).def);
    }

    /*
     * Regular inheritance children are independent enough not to inherit the
     * identity column from parent hence cannot recursively add identity column
     * if the table has inheritance children.
     */
    if !(*col_def).identity.is_null()
        && (*col_def).identity != 0 as _
        && recurse
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
        && !find_inheritance_children(myrelid, NoLock).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot recursively add identity column to table that has child tables"
            )
        );
    }

    pgclass = table_open(RelationRelationId, RowExclusiveLock);
    reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(myrelid));
    if !HeapTupleIsValid(reltup) {
        elog!(ERROR, "cache lookup failed for relation {}", myrelid);
    }
    relform = GETSTRUCT(reltup) as Form_pg_class;
    relkind = (*relform).relkind;

    /* Determine the new attribute's number */
    newattnum = (*relform).relnatts as i32 + 1;
    if newattnum > MaxHeapAttributeNumber as i32 {
        ereport!(
            ERROR,
            errcode(ERRCODE_TOO_MANY_COLUMNS),
            errmsg!(
                "tables can have at most {} columns",
                MaxHeapAttributeNumber
            )
        );
    }

    /* Construct new attribute's pg_attribute entry. */
    tupdesc = BuildDescForRelation(list_make1(col_def as *mut _));
    attribute = TupleDescAttr(tupdesc, 0);

    /* Fix up attribute number */
    (*attribute).attnum = newattnum as AttrNumber;

    /* make sure datatype is legal for a column */
    CheckAttributeType(
        NameStr!((*attribute).attname),
        (*attribute).atttypid,
        (*attribute).attcollation,
        list_make1_oid((*(*rel).rd_rel).reltype),
        if (*attribute).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            CHKATYPE_IS_VIRTUAL
        } else {
            0
        },
    );

    InsertPgAttributeTuples(attrdesc, tupdesc, myrelid, std::ptr::null_mut(), std::ptr::null_mut());
    table_close(attrdesc, RowExclusiveLock);

    /* Update pg_class tuple as appropriate */
    (*relform).relnatts = newattnum as i16;
    CatalogTupleUpdate(pgclass, &mut (*reltup).t_self, reltup);
    heap_freetuple(reltup);

    /* Post creation hook for new attribute */
    InvokeObjectPostCreateHook(RelationRelationId, myrelid, newattnum);
    table_close(pgclass, RowExclusiveLock);

    /* Make the attribute's catalog entry visible */
    CommandCounterIncrement();

    /* Store the DEFAULT, if any, in the catalogs */
    if !(*col_def).raw_default.is_null() {
        let raw_ent = palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
        (*raw_ent).attnum = (*attribute).attnum;
        (*raw_ent).raw_default = copyObject((*col_def).raw_default as *mut _) as *mut _;
        (*raw_ent).generated = (*col_def).generated;

        /*
         * This function is intended for CREATE TABLE, so it processes a
         * _list_ of defaults, but we just do one.
         */
        AddRelationNewConstraints(
            rel,
            list_make1(raw_ent as *mut _),
            std::ptr::null_mut(),
            false,
            true,
            false,
            std::ptr::null_mut(),
        );
        /* Make the additional catalog changes visible */
        CommandCounterIncrement();
    }

    /*
     * Tell Phase 3 to fill in the default expression, if there is one.
     *
     * An exception occurs when the new column is of a domain type.
     */
    if RELKIND_HAS_STORAGE(relkind) {
        let has_domain_constraints: bool;
        let mut has_missing = false;

        /*
         * For an identity column, we can't use build_column_default(),
         * because the sequence ownership isn't set yet.
         */
        if (*col_def).identity != 0 as _ {
            let nve = makeNode!(NextValueExpr, T_NextValueExpr) as *mut NextValueExpr;
            (*nve).seqid =
                RangeVarGetRelid((*col_def).identitySequence, NoLock, false);
            (*nve).typeId = (*attribute).atttypid;
            defval = nve as *mut Expr;
        } else {
            defval = build_column_default(rel, (*attribute).attnum) as *mut Expr;
        }

        /* Build CoerceToDomain(NULL) expression if needed */
        has_domain_constraints = DomainHasConstraints((*attribute).atttypid);
        if defval.is_null() && has_domain_constraints {
            let mut base_type_mod = (*attribute).atttypmod;
            let base_type_id =
                getBaseTypeAndTypmod((*attribute).atttypid, &mut base_type_mod);
            let base_type_coll = get_typcollation(base_type_id);
            defval =
                makeNullConst(base_type_id, base_type_mod, base_type_coll) as *mut Expr;
            defval = coerce_to_target_type(
                std::ptr::null_mut(),
                defval as *mut Node,
                base_type_id,
                (*attribute).atttypid,
                (*attribute).atttypmod,
                COERCION_ASSIGNMENT,
                COERCE_IMPLICIT_CAST,
                -1,
            ) as *mut Expr;
            if defval.is_null() {
                /* should not happen */
                elog!(ERROR, "failed to coerce base type to domain");
            }
        }

        if !defval.is_null() {
            let newval =
                palloc0(core::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;

            /* Prepare defval for execution, either here or in Phase 3 */
            defval = expression_planner(defval);

            /* Add the new default to the newvals list */
            (*newval).attnum = (*attribute).attnum;
            (*newval).expr = defval;
            (*newval).is_generated = (*col_def).generated != 0 as _;

            (*tab).newvals = lappend((*tab).newvals, newval as *mut _);

            /*
             * Attempt to skip a complete table rewrite by storing the
             * specified DEFAULT value outside of the heap.
             */
            if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8
                && (*col_def).generated == 0 as _
                && !has_domain_constraints
                && !contain_volatile_functions(defval as *mut Node)
            {
                let estate = CreateExecutorState();
                let expr_state = ExecPrepareExpr(defval, estate);
                let mut missing_is_null = false;
                let missingval = ExecEvalExpr(
                    expr_state,
                    GetPerTupleExprContext(estate),
                    &mut missing_is_null,
                );
                /* If it turns out NULL, nothing to do; else store it */
                if !missing_is_null {
                    StoreAttrMissingVal(rel, (*attribute).attnum, missingval);
                    /* Make the additional catalog change visible */
                    CommandCounterIncrement();
                    has_missing = true;
                }
                FreeExecutorState(estate);
            } else {
                /*
                 * Failed to use missing mode.  We have to do a table rewrite
                 * to install the value --- unless it's a virtual generated column.
                 */
                if (*col_def).generated != ATTRIBUTE_GENERATED_VIRTUAL as i8 {
                    (*tab).rewrite |= AT_REWRITE_DEFAULT_VAL;
                }
            }
        }

        if !has_missing {
            /*
             * If the new column is NOT NULL, and there is no missing value,
             * tell Phase 3 it needs to check for NULLs.
             */
            (*tab).verify_new_notnull |= (*col_def).is_not_null;
        }
    }

    /* Add needed dependency entries for the new column. */
    add_column_datatype_dependency(myrelid, newattnum, (*attribute).atttypid);
    add_column_collation_dependency(myrelid, newattnum, (*attribute).attcollation);

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    /*
     * If we are told not to recurse, there had better not be any child tables.
     */
    if !children.is_null() && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("column must be added to child tables too")
        );
    }

    /* Children should see column as singly inherited */
    let childcmd: *mut AlterTableCmd;
    if !recursing {
        childcmd = copyObject(*cmd as *mut _) as *mut AlterTableCmd;
        let child_coldef = castNode!(ColumnDef, T_ColumnDef, (*childcmd).def);
        (*child_coldef).inhcount = 1;
        (*child_coldef).is_local = false;
    } else {
        childcmd = *cmd; /* no need to copy again */
    }

    let mut lc = list_head(children);
    while !lc.is_null() {
        let childrelid = lfirst_oid(lc);
        let childrel: Relation;
        let childtab: *mut AlteredTableInfo;

        /* find_inheritance_children already got lock */
        childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /* Find or create work queue entry for this table */
        childtab = ATGetQueueEntry(wqueue, childrel);

        /* Recurse to child; return value is ignored */
        at_exec_add_column(
            wqueue, childtab, childrel, &mut (childcmd as *mut AlterTableCmd),
            recurse, true, lockmode, cur_pass, context,
        );

        table_close(childrel, NoLock);
        lc = lnext(children, lc);
    }

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    ObjectAddressSubSet!(address, RelationRelationId, myrelid, newattnum);
    address
}

/*
 * If a new or renamed column will collide with the name of an existing
 * column and if_not_exists is false then error out, else do nothing.
 */
unsafe fn check_for_column_name_collision(
    rel: Relation,
    colname: *const i8,
    if_not_exists: bool,
) -> bool {
    let att_tuple: HeapTuple;
    let attnum: i32;

    /*
     * this test is deliberately not attisdropped-aware, since if one tries to
     * add a column matching a dropped column name, it's gonna fail anyway.
     */
    att_tuple = SearchSysCache2(
        ATTNAME,
        ObjectIdGetDatum(RelationGetRelid(rel)),
        PointerGetDatum(colname as *mut _),
    );
    if !HeapTupleIsValid(att_tuple) {
        return true;
    }

    attnum = (*(GETSTRUCT(att_tuple) as Form_pg_attribute)).attnum as i32;
    ReleaseSysCache(att_tuple);

    /*
     * We throw a different error message for conflicts with system column names.
     */
    if attnum <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_DUPLICATE_COLUMN),
            errmsg!(
                "column name \"{}\" conflicts with a system column name",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
        );
    } else {
        if if_not_exists {
            ereport!(
                NOTICE,
                errcode(ERRCODE_DUPLICATE_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" already exists, skipping",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            return false;
        }
        ereport!(
            ERROR,
            errcode(ERRCODE_DUPLICATE_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" already exists",
                std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    true
}

/* Install a column's dependency on its datatype. */
unsafe fn add_column_datatype_dependency(relid: Oid, attnum: i32, typid: Oid) {
    let mut myself = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    myself.classId = RelationRelationId;
    myself.objectId = relid;
    myself.objectSubId = attnum;
    referenced.classId = TypeRelationId;
    referenced.objectId = typid;
    referenced.objectSubId = 0;
    recordDependencyOn(&mut myself, &mut referenced, DEPENDENCY_NORMAL);
}

/* Install a column's dependency on its collation. */
unsafe fn add_column_collation_dependency(relid: Oid, attnum: i32, collid: Oid) {
    let mut myself = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    /* We know the default collation is pinned, so don't bother recording it */
    if OidIsValid(collid) && collid != DEFAULT_COLLATION_OID {
        myself.classId = RelationRelationId;
        myself.objectId = relid;
        myself.objectSubId = attnum;
        referenced.classId = CollationRelationId;
        referenced.objectId = collid;
        referenced.objectSubId = 0;
        recordDependencyOn(&mut myself, &mut referenced, DEPENDENCY_NORMAL);
    }
}

/*
 * ALTER TABLE ALTER COLUMN DROP NOT NULL
 *
 * Return the address of the modified column.  If the column was already
 * nullable, InvalidObjectAddress is returned.
 */
unsafe fn at_exec_drop_not_null(
    rel: Relation,
    col_name: *const i8,
    recurse: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let con_tup: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attr_rel: Relation;
    let mut address = InvalidObjectAddress;

    /* lookup the attribute */
    attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /* If the column is already nullable there's nothing to do. */
    if !(*att_tup).attnotnull {
        table_close(attr_rel, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*att_tup).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * If rel is partition, shouldn't drop NOT NULL if parent has the same.
     */
    if (*(*rel).rd_rel).relispartition {
        let parent_id = get_partition_parent(RelationGetRelid(rel), false);
        let parent = table_open(parent_id, AccessShareLock);
        let tup_desc = RelationGetDescr(parent);
        let parent_attnum = get_attnum(parent_id, col_name);
        if (*TupleDescAttr(tup_desc, (parent_attnum as i32 - 1) as usize)).attnotnull {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!(
                    "column \"{}\" is marked NOT NULL in parent table",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy()
                )
            );
        }
        table_close(parent, AccessShareLock);
    }

    /*
     * Find the constraint that makes this column NOT NULL, and drop it.
     * dropconstraint_internal() resets attnotnull.
     */
    con_tup = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
    if con_tup.is_null() {
        elog!(
            ERROR,
            "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
            std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
        );
    }

    /* The normal case: we have a pg_constraint row, remove it */
    dropconstraint_internal(
        rel, con_tup, DROP_RESTRICT, recurse, false, false, lockmode,
    );
    heap_freetuple(con_tup);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    table_close(attr_rel, RowExclusiveLock);

    address
}

/*
 * set_attnotnull
 *   Helper to update/validate the pg_attribute status of a not-null constraint
 */
unsafe fn set_attnotnull(
    wqueue: *mut *mut List,
    rel: Relation,
    attnum: AttrNumber,
    is_valid: bool,
    queue_validation: bool,
) {
    let attr: Form_pg_attribute;
    let thisatt: *mut CompactAttribute;

    Assert!(!queue_validation || !wqueue.is_null());

    CheckAlterTableIsSafe(rel);

    /*
     * Exit quickly by testing attnotnull from the tupledesc's copy of the attribute.
     */
    attr = TupleDescAttr(RelationGetDescr(rel), (attnum as i32 - 1) as usize);
    if (*attr).attisdropped {
        return;
    }

    if !(*attr).attnotnull {
        let attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        let tuple = SearchSysCacheCopyAttNum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(tuple) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                RelationGetRelid(rel)
            );
        }

        thisatt = TupleDescCompactAttr(RelationGetDescr(rel), (attnum as i32 - 1) as usize);
        (*thisatt).attnullability = ATTNULLABLE_VALID as u8;

        let attr_form = GETSTRUCT(tuple) as Form_pg_attribute;
        (*attr_form).attnotnull = true;
        CatalogTupleUpdate(attr_rel, &mut (*tuple).t_self, tuple);

        /*
         * If the nullness isn't already proven by validated constraints, have
         * ALTER TABLE phase 3 test for it.
         */
        if queue_validation && !wqueue.is_null()
            && !NotNullImpliedByRelConstraints(rel, attr_form)
        {
            let tab = ATGetQueueEntry(wqueue, rel);
            (*tab).verify_new_notnull = true;
        }

        CommandCounterIncrement();
        table_close(attr_rel, RowExclusiveLock);
        heap_freetuple(tuple);
    } else {
        CacheInvalidateRelcache(rel);
    }
}

/*
 * ALTER TABLE ALTER COLUMN SET NOT NULL
 *
 * Add a not-null constraint to a single table and its children.
 */
unsafe fn at_exec_set_not_null(
    wqueue: *mut *mut List,
    rel: Relation,
    con_name: *mut i8,
    col_name: *mut i8,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let constraint: *mut Constraint;
    let ccon: *mut CookedConstraint;
    let cooked: *mut List;
    let mut is_no_inherit = false;

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_AddConstraint,
            rel,
            ATT_PARTITIONED_TABLE | ATT_TABLE | ATT_FOREIGN_TABLE,
        );
        Assert!(!con_name.is_null());
    }

    attnum = get_attnum(RelationGetRelid(rel), col_name);
    if attnum == InvalidAttrNumber {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /* See if there's already a constraint */
    tuple = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
    if HeapTupleIsValid(tuple) {
        let con_form = GETSTRUCT(tuple) as Form_pg_constraint;
        let mut changed = false;

        /*
         * Don't let a NO INHERIT constraint be changed into inherit.
         */
        if (*con_form).connoinherit && recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*con_form).conname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }

        /*
         * If we find an appropriate constraint, increment coninhcount if recursing,
         * set conislocal if not, or validate if not already validated.
         */
        if recursing {
            if pg_add_s16_overflow(
                (*con_form).coninhcount,
                1,
                &mut (*con_form).coninhcount,
            ) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg!("too many inheritance parents")
                );
            }
            changed = true;
        } else if !(*con_form).conislocal {
            (*con_form).conislocal = true;
            changed = true;
        } else if !(*con_form).convalidated {
            /*
             * Flip attnotnull and convalidated, and also validate the constraint.
             */
            return ATExecValidateConstraint(
                wqueue,
                rel,
                NameStr!((*con_form).conname) as *mut i8,
                recurse,
                recursing,
                lockmode,
            );
        }

        if changed {
            let constr_rel = table_open(ConstraintRelationId, RowExclusiveLock);
            CatalogTupleUpdate(constr_rel, &mut (*tuple).t_self, tuple);
            ObjectAddressSet!(address, ConstraintRelationId, (*con_form).oid);
            table_close(constr_rel, RowExclusiveLock);
        }

        if changed {
            return address;
        } else {
            return InvalidObjectAddress;
        }
    }

    /*
     * If we're asked not to recurse, and children exist, raise an error for
     * partitioned tables.
     */
    if !recurse
        && !find_inheritance_children(RelationGetRelid(rel), NoLock).is_null()
    {
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be added to child tables too")
                /* errhint: "Do not specify the ONLY keyword." */
            );
        } else {
            is_no_inherit = true;
        }
    }

    /*
     * No constraint exists; we must add one.  Determine a name to use.
     */
    let con_name_used: *mut i8;
    if !recursing {
        Assert!(con_name.is_null());
        con_name_used = ChooseConstraintName(
            RelationGetRelationName(rel),
            col_name,
            b"not_null\0".as_ptr() as *const i8,
            RelationGetNamespace(rel),
            std::ptr::null_mut(),
        );
    } else {
        con_name_used = con_name;
    }

    constraint = makeNotNullConstraint(makeString(col_name));
    (*constraint).is_no_inherit = is_no_inherit;
    (*constraint).conname = con_name_used;

    /* and do it */
    cooked = AddRelationNewConstraints(
        rel,
        std::ptr::null_mut(),
        list_make1(constraint as *mut _),
        false,
        !recursing,
        false,
        std::ptr::null_mut(),
    );
    ccon = linitial(cooked) as *mut CookedConstraint;
    ObjectAddressSet!(address, ConstraintRelationId, (*ccon).conoid);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    /* Mark pg_attribute.attnotnull for the column and queue validation */
    set_attnotnull(wqueue, rel, attnum, true, true);

    /* Recurse to propagate the constraint to children that don't have one. */
    if recurse {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childoid = lfirst_oid(lc);
            let childrel = table_open(childoid, NoLock);
            CommandCounterIncrement();
            at_exec_set_not_null(wqueue, childrel, con_name_used, col_name, recurse, true, lockmode);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * NotNullImpliedByRelConstraints
 *   Does rel's existing constraints imply NOT NULL for the given attribute?
 */
unsafe fn not_null_implied_by_rel_constraints(
    rel: Relation,
    attr: Form_pg_attribute,
) -> bool {
    let nnulltest = makeNode!(NullTest, T_NullTest) as *mut NullTest;

    (*nnulltest).arg = makeVar(
        1,
        (*attr).attnum,
        (*attr).atttypid,
        (*attr).atttypmod,
        (*attr).attcollation,
        0,
    ) as *mut Expr;
    (*nnulltest).nulltesttype = IS_NOT_NULL;

    /*
     * argisrow = false is correct even for a composite column.
     */
    (*nnulltest).argisrow = false;
    (*nnulltest).location = -1;

    if ConstraintImpliedByRelConstraint(
        rel,
        list_make1(nnulltest as *mut _),
        std::ptr::null_mut(),
    ) {
        ereport!(
            DEBUG1,
            errmsg_internal!(
                "existing constraints on column \"{}.{}\" are sufficient to prove that it does not contain nulls",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                std::ffi::CStr::from_ptr(NameStr!((*attr).attname) as *const i8).to_string_lossy()
            )
        );
        return true;
    }

    false
}

/*
 * ALTER TABLE ALTER COLUMN SET/DROP DEFAULT
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_column_default(
    rel: Relation,
    col_name: *const i8,
    new_default: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tupdesc = RelationGetDescr(rel);
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;

    /* get the number of the attribute */
    attnum = get_attnum(RelationGetRelid(rel), col_name);
    if attnum == InvalidAttrNumber {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Prevent them from altering a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*TupleDescAttr(tupdesc, (attnum as i32 - 1) as usize)).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errhint depends on new_default */
        );
    }

    if (*TupleDescAttr(tupdesc, (attnum as i32 - 1) as usize)).attgenerated != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_SYNTAX_ERROR),
            errmsg!(
                "column \"{}\" of relation \"{}\" is a generated column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * Remove any old default for the column.
     */
    RemoveAttrDefault(
        RelationGetRelid(rel),
        attnum,
        DROP_RESTRICT,
        false,
        !new_default.is_null(),
    );

    if !new_default.is_null() {
        /* SET DEFAULT */
        let raw_ent =
            palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
        (*raw_ent).attnum = attnum;
        (*raw_ent).raw_default = new_default;
        (*raw_ent).generated = 0 as _;

        AddRelationNewConstraints(
            rel,
            list_make1(raw_ent as *mut _),
            std::ptr::null_mut(),
            false,
            true,
            false,
            std::ptr::null_mut(),
        );
    }

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * Add a pre-cooked default expression.
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_cooked_column_default(
    rel: Relation,
    attnum: AttrNumber,
    new_default: *mut Node,
) -> ObjectAddress {
    let mut address = InvalidObjectAddress;

    /* We assume no checking is required */

    /*
     * Remove any old default for the column.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, true);

    StoreAttrDefault(rel, attnum, new_default, true);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN ADD IDENTITY
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_add_identity(
    rel: Relation,
    col_name: *const i8,
    def: *mut Node,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let cdef = castNode!(ColumnDef, T_ColumnDef, def);
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot add identity to a column of only the partitioned table")
            /* errhint: "Do not specify the ONLY keyword." */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot add identity to a column of a partition")
        );
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    /* Can't alter a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Creating a column as identity implies NOT NULL, so adding the identity
     * to an existing column that is not NOT NULL would create a state that
     * cannot be reproduced without contortions.
     */
    if !(*att_tup).attnotnull {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" must be declared NOT NULL before identity can be added",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* If a not-null constraint exists, verify it's compatible. */
    if (*att_tup).attnotnull {
        let contup = findNotNullConstraintAttnum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(contup) {
            elog!(
                ERROR,
                "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            );
        }
        let con_form = GETSTRUCT(contup) as Form_pg_constraint;
        if !(*con_form).convalidated {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*con_form).conname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errhint: "You might need to validate it using ..." */
            );
        }
    }

    if (*att_tup).attidentity != 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is already an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    if (*att_tup).atthasdef {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" already has a default value",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    (*att_tup).attidentity = (*cdef).identity;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*att_tup).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to propagate the identity column to partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            at_exec_add_identity(childrel, col_name, def, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN SET { GENERATED or sequence options }
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_set_identity(
    rel: Relation,
    col_name: *const i8,
    def: *mut Node,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let mut generated_el: *mut DefElem = std::ptr::null_mut();
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let mut address = InvalidObjectAddress;
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot change identity column of only the partitioned table")
            /* errhint: "Do not specify the ONLY keyword." */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot change identity column of a partition")
        );
    }

    {
        let option_list = castNode!(List, T_List, def);
        let mut lc = list_head(option_list);
        while !lc.is_null() {
            let defel = lfirst_node!(DefElem, T_DefElem, current_cell!(lc));
            if libc::strcmp((*defel).defname, b"generated\0".as_ptr() as *const i8) == 0 {
                if !generated_el.is_null() {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg!("conflicting or redundant options")
                    );
                }
                generated_el = defel;
            } else {
                elog!(
                    ERROR,
                    "option \"{}\" not recognized",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()
                );
            }
            lc = lnext(option_list, lc);
        }
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if !(*att_tup).attidentity != false {
        // attidentity == 0
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is not an identity column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    if !generated_el.is_null() {
        (*att_tup).attidentity = defGetInt32(generated_el) as i8;
        CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

        InvokeObjectPostAlterHook(
            RelationRelationId,
            RelationGetRelid(rel),
            (*att_tup).attnum as i32,
        );
        ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    } else {
        address = InvalidObjectAddress;
    }

    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to propagate the identity change to partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if !generated_el.is_null() && recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            at_exec_set_identity(childrel, col_name, def, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN DROP IDENTITY
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_drop_identity(
    rel: Relation,
    col_name: *const i8,
    missing_ok: bool,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let mut address = InvalidObjectAddress;
    let seqid: Oid;
    let mut seqaddress: ObjectAddress = InvalidObjectAddress;
    let ispartitioned = (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8;

    if ispartitioned && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot drop identity from a column of only the partitioned table")
            /* errhint */
        );
    }

    if (*(*rel).rd_rel).relispartition && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("cannot drop identity from a column of a partition")
        );
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*att_tup).attidentity == 0 as _ {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not an identity column, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
    }

    (*att_tup).attidentity = 0 as _;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*att_tup).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Recurse to drop the identity from column in partitions.
     * Identity is not inherited in regular inheritance children.
     */
    if recurse && ispartitioned {
        let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrel = table_open(lfirst_oid(lc), NoLock);
            at_exec_drop_identity(childrel, col_name, false, lockmode, recurse, true);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
    }

    if !recursing {
        /* drop the internal sequence */
        seqid = getIdentitySequence(rel, attnum, false);
        deleteDependencyRecordsForClass(
            RelationRelationId,
            seqid,
            RelationRelationId,
            DEPENDENCY_INTERNAL,
        );
        CommandCounterIncrement();
        seqaddress.classId = RelationRelationId;
        seqaddress.objectId = seqid;
        seqaddress.objectSubId = 0;
        performDeletion(&mut seqaddress, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    }

    address
}

/*
 * ALTER TABLE ALTER COLUMN SET EXPRESSION
 *
 * Return the address of the affected column.
 */
unsafe fn at_exec_set_expression(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    col_name: *const i8,
    new_expr: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attgenerated: i8;
    let rewrite: bool;
    let attrdefoid: Oid;
    let mut address = InvalidObjectAddress;
    let defval: *mut Expr;
    let newval: *mut NewColumnValue;
    let raw_ent: *mut RawColumnDefault;

    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    attgenerated = (*att_tup).attgenerated;
    if attgenerated == 0 as _ {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "column \"{}\" of relation \"{}\" is not a generated column",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * TODO: This could be done, just need to recheck any constraints afterwards.
     */
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
        && !(*(*rel).rd_att).constr.is_null()
        && (*(*(*rel).rd_att).constr).num_check > 0
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables with check constraints"
            )
            /* errdetail */
        );
    }

    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 && (*att_tup).attnotnull {
        (*tab).verify_new_notnull = true;
    }

    /*
     * We need to prevent this because a change of expression could affect a row filter.
     */
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
        && !GetRelationPublications(RelationGetRelid(rel)).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in tables that are part of a publication"
            )
        );
    }

    rewrite = attgenerated == ATTRIBUTE_GENERATED_STORED as i8;

    ReleaseSysCache(tuple);

    if rewrite {
        /*
         * Clear all the missing values if we're rewriting the table.
         */
        RelationClearMissing(rel);
        /* make sure we don't conflict with later attribute modifications */
        CommandCounterIncrement();

        /*
         * Find everything that depends on the column and record enough information
         * to let us recreate the objects after rewrite.
         */
        RememberAllDependentForRebuilding(tab, AT_SetExpression, rel, attnum, col_name);
    }

    /*
     * Drop the dependency records of the GENERATED expression.
     */
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if !OidIsValid(attrdefoid) {
        elog!(
            ERROR,
            "could not find attrdef tuple for relation {} attnum {}",
            RelationGetRelid(rel),
            attnum
        );
    }
    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);

    /* Make above changes visible */
    CommandCounterIncrement();

    /*
     * Get rid of the GENERATED expression itself.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    /* Prepare to store the new expression, in the catalogs */
    raw_ent = palloc(core::mem::size_of::<RawColumnDefault>()) as *mut RawColumnDefault;
    (*raw_ent).attnum = attnum;
    (*raw_ent).raw_default = new_expr;
    (*raw_ent).generated = attgenerated;

    /* Store the generated expression */
    AddRelationNewConstraints(
        rel,
        list_make1(raw_ent as *mut _),
        std::ptr::null_mut(),
        false,
        true,
        false,
        std::ptr::null_mut(),
    );

    /* Make above new expression visible */
    CommandCounterIncrement();

    if rewrite {
        /* Prepare for table rewrite */
        defval = build_column_default(rel, attnum) as *mut Expr;
        newval = palloc0(core::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;
        (*newval).attnum = attnum;
        (*newval).expr = expression_planner(defval);
        (*newval).is_generated = true;

        (*tab).newvals = lappend((*tab).newvals, newval as *mut _);
        (*tab).rewrite |= AT_REWRITE_DEFAULT_VAL;
    }

    /* Drop any pg_statistic entry for the column */
    RemoveStatistics(RelationGetRelid(rel), attnum);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN DROP EXPRESSION
 */
unsafe fn at_prep_drop_expression(
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    /*
     * Reject ONLY if there are child tables.
     */
    if !recurse && !find_inheritance_children(RelationGetRelid(rel), lockmode).is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!("ALTER TABLE / DROP EXPRESSION must be applied to child tables too")
        );
    }

    /*
     * Cannot drop generation expression from inherited columns.
     */
    if !recursing {
        let tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), (*cmd).name);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr((*cmd).name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
        let att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
        if (*att_tup).attinhcount > 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("cannot drop generation expression from inherited column")
            );
        }
    }
}

/* Return the address of the affected column. */
unsafe fn at_exec_drop_expression(
    rel: Relation,
    col_name: *const i8,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let attrelation: Relation;
    let attrdefoid: Oid;
    let mut address = InvalidObjectAddress;

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * TODO: This could be done, but it would need a table rewrite to materialize the generated values.
     */
    if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
            )
            /* errdetail */
        );
    }

    if (*att_tup).attgenerated == 0 as _ {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not a generated column",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" is not a generated column, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
    }

    /*
     * Mark the column as no longer generated.
     */
    (*att_tup).attgenerated = 0 as _;
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum as i32);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    /*
     * Drop the dependency records of the GENERATED expression.
     */
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if !OidIsValid(attrdefoid) {
        elog!(
            ERROR,
            "could not find attrdef tuple for relation {} attnum {}",
            RelationGetRelid(rel),
            attnum
        );
    }
    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);

    /* Make above changes visible */
    CommandCounterIncrement();

    /*
     * Get rid of the GENERATED expression itself.
     */
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE ALTER COLUMN SET STATISTICS
 *
 * Return value is the address of the modified column
 */
unsafe fn at_exec_set_statistics(
    rel: Relation,
    col_name: *const i8,
    col_num: i16,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let mut newtarget: i32 = 0;
    let newtarget_default: bool;
    let attrelation: Relation;
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;
    let mut repl_val = [Datum::from(0usize); Natts_pg_attribute as usize];
    let mut repl_null = [false; Natts_pg_attribute as usize];
    let mut repl_repl = [false; Natts_pg_attribute as usize];

    /*
     * We allow referencing columns by numbers only for indexes.
     */
    if (*(*rel).rd_rel).relkind != RELKIND_INDEX as i8
        && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_INDEX as i8
        && col_name.is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!("cannot refer to non-index column by number")
        );
    }

    /* -1 was used in previous versions for the default setting */
    if !new_value.is_null() && intVal(new_value) != -1 {
        newtarget = intVal(new_value);
        newtarget_default = false;
    } else {
        newtarget_default = true;
    }

    if !newtarget_default {
        if newtarget < 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg!("statistics target {} is too low", newtarget)
            );
        } else if newtarget > MAX_STATISTICS_TARGET as i32 {
            newtarget = MAX_STATISTICS_TARGET as i32;
            ereport!(
                WARNING,
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg!("lowering statistics target to {}", newtarget)
            );
        }
    }

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    if !col_name.is_null() {
        tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
    } else {
        tuple = SearchSysCacheAttNum(RelationGetRelid(rel), col_num);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column number {} of relation \"{}\" does not exist",
                    col_num,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        }
    }

    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Prevent this as long as the ANALYZE code skips virtual generated columns.
     */
    if (*attrtuple).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter statistics on virtual generated column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_INDEX as i8
        || (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_INDEX as i8
    {
        if (attnum as i32) > (*(*rel).rd_index).indnkeyatts as i32 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot alter statistics on included column \"{}\" of index \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*attrtuple).attname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else if (*(*rel).rd_index).indkey.values[(attnum as usize) - 1] != 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                    std::ffi::CStr::from_ptr(NameStr!((*attrtuple).attname) as *const i8).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
                /* errhint: "Alter statistics on table column instead." */
            );
        }
    }

    /* Build new tuple. */
    libc::memset(repl_null.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_repl));
    if !newtarget_default {
        repl_val[(Anum_pg_attribute_attstattarget - 1) as usize] = newtarget as Datum;
    } else {
        repl_null[(Anum_pg_attribute_attstattarget - 1) as usize] = true;
    }
    repl_repl[(Anum_pg_attribute_attstattarget - 1) as usize] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrelation),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );
    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);
    table_close(attrelation, RowExclusiveLock);

    address
}

/* Return value is the address of the modified column */
unsafe fn at_exec_set_options(
    rel: Relation,
    col_name: *const i8,
    options: *mut Node,
    is_reset: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let newtuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let datum: Datum;
    let new_options: Datum;
    let mut isnull = false;
    let mut address = InvalidObjectAddress;
    let mut repl_val = [Datum::from(0usize); Natts_pg_attribute as usize];
    let mut repl_null = [false; Natts_pg_attribute as usize];
    let mut repl_repl = [false; Natts_pg_attribute as usize];

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);

    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /* Generate new proposed attoptions (text array) */
    datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &mut isnull);
    new_options = transformRelOptions(
        if isnull { 0 as Datum } else { datum },
        castNode!(List, T_List, options),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        false,
        is_reset,
    );
    /* Validate new options */
    attribute_reloptions(new_options, true);

    /* Build new tuple. */
    libc::memset(repl_null.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_null));
    libc::memset(repl_repl.as_mut_ptr() as *mut _, 0, core::mem::size_of_val(&repl_repl));
    if new_options != 0 as Datum {
        repl_val[(Anum_pg_attribute_attoptions - 1) as usize] = new_options;
    } else {
        repl_null[(Anum_pg_attribute_attoptions - 1) as usize] = true;
    }
    repl_repl[(Anum_pg_attribute_attoptions - 1) as usize] = true;
    newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attrelation),
        repl_val.as_mut_ptr(),
        repl_null.as_mut_ptr(),
        repl_repl.as_mut_ptr(),
    );

    /* Update system catalog. */
    CatalogTupleUpdate(attrelation, &mut (*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);

    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);
    table_close(attrelation, RowExclusiveLock);

    address
}

/*
 * Helper function for ATExecSetStorage and ATExecSetCompression
 *
 * Set the attstorage and/or attcompression fields for index columns
 * associated with the specified table column.
 */
unsafe fn set_index_storage_properties(
    rel: Relation,
    attrelation: Relation,
    attnum: AttrNumber,
    setstorage: bool,
    newstorage: i8,
    setcompression: bool,
    newcompression: i8,
    lockmode: LOCKMODE,
) {
    let index_list = RelationGetIndexList(rel);
    let mut lc = list_head(index_list);
    while !lc.is_null() {
        let indexoid = lfirst_oid(lc);
        let indrel = index_open(indexoid, lockmode);
        let mut indattnum: AttrNumber = 0;

        let nk = (*(*indrel).rd_index).indnatts as usize;
        for i in 0..nk {
            if (*(*indrel).rd_index).indkey.values[i] == attnum as i16 {
                indattnum = (i + 1) as AttrNumber;
                break;
            }
        }

        if indattnum == 0 {
            index_close(indrel, lockmode);
            lc = lnext(index_list, lc);
            continue;
        }

        let tuple = SearchSysCacheCopyAttNum(RelationGetRelid(indrel), indattnum);
        if HeapTupleIsValid(tuple) {
            let attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;

            if setstorage {
                (*attrtuple).attstorage = newstorage;
            }
            if setcompression {
                (*attrtuple).attcompression = newcompression;
            }

            CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);
            InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);
            heap_freetuple(tuple);
        }

        index_close(indrel, lockmode);
        lc = lnext(index_list, lc);
    }
}

/*
 * ALTER TABLE ALTER COLUMN SET STORAGE
 *
 * Return value is the address of the modified column
 */
unsafe fn at_exec_set_storage(
    rel: Relation,
    col_name: *const i8,
    new_value: *mut Node,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let attrelation: Relation;
    let tuple: HeapTuple;
    let attrtuple: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut address = InvalidObjectAddress;

    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), col_name);

    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg!(
                "column \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    attrtuple = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*attrtuple).attnum;

    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot alter system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    (*attrtuple).attstorage = GetAttributeStorage((*attrtuple).atttypid, strVal(new_value));

    CatalogTupleUpdate(attrelation, &mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), (*attrtuple).attnum as i32);

    /*
     * Apply the change to indexes as well (only for simple index columns).
     */
    set_index_storage_properties(
        rel, attrelation, attnum,
        true, (*attrtuple).attstorage,
        false, 0,
        lockmode,
    );

    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    ObjectAddressSubSet!(address, RelationRelationId, RelationGetRelid(rel), attnum as i32);
    address
}

/*
 * ALTER TABLE DROP COLUMN
 *
 * DROP COLUMN cannot use the normal ALTER TABLE recursion mechanism.
 */
unsafe fn at_prep_drop_column(
    wqueue: *mut *mut List,
    rel: Relation,
    recurse: bool,
    recursing: bool,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    if (*(*rel).rd_rel).reloftype && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("cannot drop column from typed table")
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
    }

    if recurse {
        (*cmd).recurse = true;
    }
}

/*
 * Drops column 'colName' from relation 'rel' and returns the address of the
 * dropped column.
 */
unsafe fn at_exec_drop_column(
    wqueue: *mut *mut List,
    rel: Relation,
    col_name: *const i8,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
    addrs: *mut ObjectAddresses,
) -> ObjectAddress {
    let tuple: HeapTuple;
    let targetatt: Form_pg_attribute;
    let attnum: AttrNumber;
    let children: *mut List;
    let mut object = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let is_expr: bool;
    // mut addrs - we may reassign from param
    let mut addrs = addrs;

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_DropColumn,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    /* Initialize addrs on the first invocation */
    Assert!(!recursing || !addrs.is_null());

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if !recursing {
        addrs = new_object_addresses();
    }

    /* get the number of the attribute */
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                NOTICE,
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist, skipping",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                )
            );
            return InvalidObjectAddress;
        }
    }
    targetatt = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*targetatt).attnum;

    /* Can't drop a system attribute */
    if (attnum as i32) <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "cannot drop system column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Don't drop inherited columns, unless recursing.
     */
    if (*targetatt).attinhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot drop inherited column \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy()
            )
        );
    }

    /*
     * Don't drop columns used in the partition key.
     */
    let _ = &mut is_expr; // used by C macro
    if has_partition_attrs(
        rel,
        bms_make_singleton((attnum as i32) - FirstLowInvalidHeapAttributeNumber),
        &mut (false as bool),
    ) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!(
                "cannot drop column \"{}\" because it is part of the partition key of relation \"{}\"",
                std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    ReleaseSysCache(tuple);

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    if !children.is_null() {
        let attr_rel: Relation;

        /*
         * In case of a partitioned table, the column must be dropped from the
         * partitions as well.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 && !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!(
                    "cannot drop column from only the partitioned table when partitions exist"
                )
                /* errhint: "Do not specify the ONLY keyword." */
            );
        }

        attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
        let mut lc = list_head(children);
        while !lc.is_null() {
            let childrelid = lfirst_oid(lc);
            let childrel = table_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);

            let child_tuple = SearchSysCacheCopyAttName(childrelid, col_name);
            if !HeapTupleIsValid(child_tuple) {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "cache lookup failed for attribute \"{}\" of relation {}",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    childrelid
                );
            }
            let childatt = GETSTRUCT(child_tuple) as Form_pg_attribute;

            if (*childatt).attinhcount <= 0 {
                /* shouldn't happen */
                elog!(
                    ERROR,
                    "relation {} has non-inherited attribute \"{}\"",
                    childrelid,
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy()
                );
            }

            if recurse {
                /*
                 * If the child column has other definition sources, just decrement its
                 * inheritance count; if not, recurse to delete it.
                 */
                if (*childatt).attinhcount == 1 && !(*childatt).attislocal {
                    /* Time to delete this child column, too */
                    at_exec_drop_column(
                        wqueue, childrel, col_name, behavior, true, true, false, lockmode, addrs,
                    );
                } else {
                    /* Child column must survive my deletion */
                    (*childatt).attinhcount -= 1;
                    CatalogTupleUpdate(attr_rel, &mut (*child_tuple).t_self, child_tuple);
                    /* Make update visible */
                    CommandCounterIncrement();
                }
            } else {
                /*
                 * If we were told to drop ONLY in this table (no recursion),
                 * mark the inheritors' attributes as locally defined.
                 */
                (*childatt).attinhcount -= 1;
                (*childatt).attislocal = true;
                CatalogTupleUpdate(attr_rel, &mut (*child_tuple).t_self, child_tuple);
                /* Make update visible */
                CommandCounterIncrement();
            }

            heap_freetuple(child_tuple);
            table_close(childrel, NoLock);
            lc = lnext(children, lc);
        }
        table_close(attr_rel, RowExclusiveLock);
    }

    /* Add object to delete */
    object.classId = RelationRelationId;
    object.objectId = RelationGetRelid(rel);
    object.objectSubId = attnum as i32;
    add_exact_object_address(&mut object, addrs);

    if !recursing {
        /* Recursion has ended, drop everything that was collected */
        performMultipleDeletions(addrs, behavior, 0);
        free_object_addresses(addrs);
    }

    object
}

/*
 * Prepare to add a primary key on a table, by adding not-null constraints
 * on all columns.
 */
unsafe fn at_prep_add_primary_key(
    wqueue: *mut *mut List,
    rel: Relation,
    cmd: *mut AlterTableCmd,
    recurse: bool,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    let pkconstr = castNode!(Constraint, T_Constraint, (*cmd).def);
    if (*pkconstr).contype != CONSTR_PRIMARY {
        return;
    }

    let mut children: *mut List = std::ptr::null_mut();
    let mut got_children = false;

    /* Verify that columns are not-null, or request that they be made so */
    let mut lc = list_head((*pkconstr).keys);
    while !lc.is_null() {
        let column = lfirst(lc) as *mut String;
        let col_str = strVal(column as *mut Node);

        /*
         * First check if a suitable constraint exists.  If it does, we don't
         * need to request another one.
         */
        let tuple = findNotNullConstraint(RelationGetRelid(rel), col_str);
        if !tuple.is_null() {
            verifyNotNullPKCompatible(tuple, col_str);
            /* All good with this one; don't request another */
            heap_freetuple(tuple);
            lc = lnext((*pkconstr).keys, lc);
            continue;
        } else if !recurse {
            /*
             * No constraint on this column.  Asked not to recurse, we won't
             * create one here, but verify that all children have one.
             */
            if !got_children {
                children = find_inheritance_children(RelationGetRelid(rel), lockmode);
                got_children = true;
            }

            let mut clc = list_head(children);
            while !clc.is_null() {
                let childrelid = lfirst_oid(clc);
                let tup = findNotNullConstraint(childrelid, col_str);
                if tup.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "column \"{}\" of table \"{}\" is not marked NOT NULL",
                            std::ffi::CStr::from_ptr(col_str).to_string_lossy(),
                            std::ffi::CStr::from_ptr(get_rel_name(childrelid)).to_string_lossy()
                        )
                    );
                }
                /* verify it's good enough */
                verifyNotNullPKCompatible(tup, col_str);
                clc = lnext(children, clc);
            }
        }

        /* This column is not already not-null, so add it to the queue */
        let nnconstr = makeNotNullConstraint(column as *mut Node);
        let newcmd = makeNode!(AlterTableCmd, T_AlterTableCmd) as *mut AlterTableCmd;
        (*newcmd).subtype = AT_AddConstraint;
        /* note we force recurse=true here; see above */
        (*newcmd).recurse = true;
        (*newcmd).def = nnconstr as *mut Node;

        ATPrepCmd(wqueue, rel, newcmd, true, false, lockmode, context);

        lc = lnext((*pkconstr).keys, lc);
    }
}

/*
 * Verify whether the given not-null constraint is compatible with a primary key.
 */
unsafe fn verify_not_null_pk_compatible(tuple: HeapTuple, colname: *const i8) {
    let con_form = GETSTRUCT(tuple) as Form_pg_constraint;

    if (*con_form).contype != CONSTRAINT_NOTNULL as i8 {
        elog!(ERROR, "constraint {} is not a not-null constraint", (*con_form).oid);
    }

    /* a NO INHERIT constraint is no good */
    if (*con_form).connoinherit {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot create primary key on column \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
            /* errdetail, errhint */
        );
    }

    /* an unvalidated constraint is no good */
    if !(*con_form).convalidated {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot create primary key on column \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy()
            )
            /* errdetail, errhint */
        );
    }
}

/*
 * ALTER TABLE ADD INDEX
 *
 * Return value is the address of the new index.
 */
unsafe fn at_exec_add_index(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut IndexStmt,
    is_rebuild: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let check_rights: bool;
    let skip_build: bool;
    let quiet: bool;

    Assert!(IsA!(stmt, T_IndexStmt));
    Assert!(!(*stmt).concurrent);
    /* The IndexStmt has already been through transformIndexStmt */
    Assert!((*stmt).transformed);

    /* suppress schema rights check when rebuilding existing index */
    check_rights = !is_rebuild;
    /* skip index build if phase 3 will do it or we're reusing an old one */
    skip_build = (*tab).rewrite > 0 || RelFileNumberIsValid((*stmt).oldNumber);
    /* suppress notices when rebuilding existing index */
    quiet = is_rebuild;

    let address = DefineIndex(
        RelationGetRelid(rel),
        stmt,
        InvalidOid,  /* no predefined OID */
        InvalidOid,  /* no parent index */
        InvalidOid,  /* no parent constraint */
        -1,          /* total_parts unknown */
        true,        /* is_alter_table */
        check_rights,
        false,       /* check_not_in_use - we did it already */
        skip_build,
        quiet,
    );

    /*
     * If TryReuseIndex() stashed a relfilenumber for us, we used it for the
     * new index instead of building from scratch.
     */
    if RelFileNumberIsValid((*stmt).oldNumber) {
        let irel = index_open(address.objectId, NoLock);
        (*irel).rd_createSubid = (*stmt).oldCreateSubid;
        (*irel).rd_firstRelfilelocatorSubid = (*stmt).oldFirstRelfilelocatorSubid;
        RelationPreserveStorage((*irel).rd_locator, true);
        index_close(irel, NoLock);
    }

    address
}

/*
 * ALTER TABLE ADD STATISTICS
 */
unsafe fn at_exec_add_statistics(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut CreateStatsStmt,
    is_rebuild: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    Assert!(IsA!(stmt, T_CreateStatsStmt));
    Assert!((*stmt).transformed);

    let address = CreateStatistics(stmt, !is_rebuild);
    address
}

/*
 * ALTER TABLE ADD CONSTRAINT USING INDEX
 *
 * Returns the address of the new constraint.
 */
unsafe fn at_exec_add_index_constraint(
    tab: *mut AlteredTableInfo,
    rel: Relation,
    stmt: *mut IndexStmt,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let index_oid = (*stmt).indexOid;
    let index_rel: Relation;
    let index_name: *mut i8;
    let index_info: *mut IndexInfo;
    let constraint_name: *mut i8;
    let constraint_type: i8;
    let mut flags: bits16;

    Assert!(IsA!(stmt, T_IndexStmt));
    Assert!(OidIsValid(index_oid));
    Assert!((*stmt).isconstraint);

    /*
     * Doing this on partitioned tables is not a simple feature to implement.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg!(
                "ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables"
            )
        );
    }

    index_rel = index_open(index_oid, AccessShareLock);
    index_name = pstrdup(RelationGetRelationName(index_rel));
    index_info = BuildIndexInfo(index_rel);

    /* this should have been checked at parse time */
    if !(*index_info).ii_Unique {
        elog!(ERROR, "index \"{}\" is not unique", std::ffi::CStr::from_ptr(index_name).to_string_lossy());
    }

    /*
     * Determine name to assign to constraint.
     */
    constraint_name = (*stmt).idxname;
    let constraint_name = if constraint_name.is_null() {
        index_name
    } else if libc::strcmp(constraint_name, index_name) != 0 {
        ereport!(
            NOTICE,
            errmsg!(
                "ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \"{}\" to \"{}\"",
                std::ffi::CStr::from_ptr(index_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(constraint_name).to_string_lossy()
            )
        );
        RenameRelationInternal(index_oid, constraint_name, false, true);
        constraint_name
    } else {
        constraint_name
    };

    /* Extra checks needed if making primary key */
    if (*stmt).primary {
        index_check_primary_key(rel, index_info, true, stmt);
    }

    /* Note we currently don't support EXCLUSION constraints here */
    if (*stmt).primary {
        constraint_type = CONSTRAINT_PRIMARY as i8;
    } else {
        constraint_type = CONSTRAINT_UNIQUE as i8;
    }

    /* Create the catalog entries for the constraint */
    flags = INDEX_CONSTR_CREATE_UPDATE_INDEX | INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS;
    if (*stmt).initdeferred { flags |= INDEX_CONSTR_CREATE_INIT_DEFERRED; }
    if (*stmt).deferrable   { flags |= INDEX_CONSTR_CREATE_DEFERRABLE; }
    if (*stmt).primary       { flags |= INDEX_CONSTR_CREATE_MARK_AS_PRIMARY; }

    let address = index_constraint_create(
        rel,
        index_oid,
        InvalidOid,
        index_info,
        constraint_name,
        constraint_type,
        flags,
        allowSystemTableMods,
        false, /* is_internal */
    );

    index_close(index_rel, NoLock);

    address
}

/*
 * ALTER TABLE ADD CONSTRAINT
 *
 * Return value is the address of the new constraint; if no constraint was
 * added, InvalidObjectAddress is returned.
 */
unsafe fn at_exec_add_constraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    new_constraint: *mut Constraint,
    recurse: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let mut address = InvalidObjectAddress;

    Assert!(IsA!(new_constraint, T_Constraint));

    /*
     * Currently, we only expect to see CONSTR_CHECK, CONSTR_NOTNULL and
     * CONSTR_FOREIGN nodes arriving here.
     */
    match (*new_constraint).contype {
        CONSTR_CHECK | CONSTR_NOTNULL => {
            address = at_add_check_nn_constraint(
                wqueue, tab, rel, new_constraint, recurse, false, is_readd, lockmode,
            );
        }
        CONSTR_FOREIGN => {
            /*
             * Assign or validate constraint name
             */
            if !(*new_constraint).conname.is_null() {
                if ConstraintNameIsUsed(
                    CONSTRAINT_RELATION,
                    RelationGetRelid(rel),
                    (*new_constraint).conname,
                ) {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg!(
                            "constraint \"{}\" for relation \"{}\" already exists",
                            std::ffi::CStr::from_ptr((*new_constraint).conname).to_string_lossy(),
                            std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
                        )
                    );
                }
            } else {
                (*new_constraint).conname = ChooseConstraintName(
                    RelationGetRelationName(rel),
                    ChooseForeignKeyConstraintNameAddition((*new_constraint).fk_attrs),
                    b"fkey\0".as_ptr() as *const i8,
                    RelationGetNamespace(rel),
                    std::ptr::null_mut(),
                );
            }

            address = at_add_foreign_key_constraint(
                wqueue, tab, rel, new_constraint, recurse, false, lockmode,
            );
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized constraint type: {}",
                (*new_constraint).contype as i32
            );
        }
    }

    address
}

/*
 * Generate the column-name portion of the constraint name for a new foreign
 * key given the list of column names.
 */
unsafe fn choose_foreign_key_constraint_name_addition(colnames: *mut List) -> *mut i8 {
    let mut buf = [0i8; NAMEDATALEN * 2];
    let mut buflen: usize = 0;

    buf[0] = 0;
    let mut lc = list_head(colnames);
    while !lc.is_null() {
        let name = strVal(lfirst(lc) as *mut Node);
        if buflen > 0 {
            buf[buflen] = b'_' as i8;
            buflen += 1;
        }

        /*
         * At this point we have buflen <= NAMEDATALEN.
         */
        libc::strncpy(
            buf.as_mut_ptr().add(buflen),
            name,
            NAMEDATALEN as usize,
        );
        buflen += libc::strlen(buf.as_ptr().add(buflen));
        if buflen >= NAMEDATALEN as usize {
            break;
        }
        lc = lnext(colnames, lc);
    }
    pstrdup(buf.as_ptr())
}

/*
 * Add a check or not-null constraint to a single table and its children.
 * Returns the address of the constraint added to the parent relation.
 */
unsafe fn at_add_check_nn_constraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    constr: *mut Constraint,
    recurse: bool,
    recursing: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let newcons: *mut List;
    let children: *mut List;
    let mut address = InvalidObjectAddress;

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_AddConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    /*
     * Call AddRelationNewConstraints to do the work.
     */
    newcons = AddRelationNewConstraints(
        rel,
        std::ptr::null_mut(),
        list_make1(copyObject(constr as *mut _) as *mut _),
        recursing || is_readd, /* allow_merge */
        !recursing,            /* is_local */
        is_readd,              /* is_internal */
        std::ptr::null_mut(),  /* queryString not available here */
    );

    /* we don't expect more than one constraint here */
    Assert!(list_length(newcons) <= 1);

    /* Add each to-be-validated constraint to Phase 3's queue */
    let mut lcon = list_head(newcons);
    while !lcon.is_null() {
        let ccon = lfirst(lcon) as *mut CookedConstraint;

        if !(*ccon).skip_validation && (*ccon).contype != CONSTR_NOTNULL {
            let newcon =
                palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = (*ccon).name;
            (*newcon).contype = (*ccon).contype;
            (*newcon).qual = (*ccon).expr;

            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }

        /* Save the actually assigned name if it was defaulted */
        if (*constr).conname.is_null() {
            (*constr).conname = (*ccon).name;
        }

        /*
         * If adding a valid not-null constraint, set the pg_attribute flag
         * and tell phase 3 to verify existing rows, if needed.
         */
        if (*constr).contype == CONSTR_NOTNULL {
            set_attnotnull(
                wqueue,
                rel,
                (*ccon).attnum,
                !(*constr).skip_validation,
                !(*constr).skip_validation,
            );
        }

        ObjectAddressSet!(address, ConstraintRelationId, (*ccon).conoid);
        lcon = lnext(newcons, lcon);
    }

    /* At this point we must have a locked-down name to use */
    Assert!(newcons.is_null() || !(*constr).conname.is_null());

    /* Advance command counter in case same table is visited multiple times */
    CommandCounterIncrement();

    /*
     * If the constraint got merged with an existing constraint, we're done.
     */
    if newcons.is_null() {
        return address;
    }

    /* If adding a NO INHERIT constraint, no need to find our children. */
    if (*constr).is_no_inherit {
        return address;
    }

    /*
     * Propagate to children as appropriate.
     */
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    /*
     * Check if ONLY was specified with ALTER TABLE.
     */
    if !recurse && !children.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("constraint must be added to child tables too")
        );
    }

    /* Recurse to create the constraint on each child. */
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childrelid = lfirst_oid(child_lc);
        let childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /* Find or create work queue entry for this table */
        let childtab = ATGetQueueEntry(wqueue, childrel);

        /* Recurse to this child */
        at_add_check_nn_constraint(
            wqueue, childtab, childrel, constr, recurse, true, is_readd, lockmode,
        );

        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    address
}

/*
 * Add a foreign-key constraint to a single table; return the new constraint's address.
 */
unsafe fn at_add_foreign_key_constraint(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    fkconstraint: *mut Constraint,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let pkrel: Relation;
    let mut pkattnum = [0i16; INDEX_MAX_KEYS];
    let mut fkattnum = [0i16; INDEX_MAX_KEYS];
    let mut pktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fktypoid = [InvalidOid; INDEX_MAX_KEYS];
    let mut pkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkcolloid = [InvalidOid; INDEX_MAX_KEYS];
    let mut opclasses = [InvalidOid; INDEX_MAX_KEYS];
    let mut pfeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ppeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut ffeqoperators = [InvalidOid; INDEX_MAX_KEYS];
    let mut fkdelsetcols = [0i16; INDEX_MAX_KEYS];
    let with_period: bool;
    let mut pk_has_without_overlaps = false;
    let mut numfks: i32;
    let numpks: i32;
    let numfkdelsetcols: i32;
    let mut index_oid: Oid = InvalidOid;
    let mut old_check_ok: bool;
    let old_pfeqop_item: *mut ListCell;

    /*
     * Grab ShareRowExclusiveLock on the pk table.
     */
    if OidIsValid((*fkconstraint).old_pktable_oid) {
        pkrel = table_open((*fkconstraint).old_pktable_oid, ShareRowExclusiveLock);
    } else {
        pkrel = table_openrv((*fkconstraint).pktable, ShareRowExclusiveLock);
    }

    /* Validity checks */
    if !recurse && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot use ONLY for foreign key on partitioned table \"{}\" referencing relation \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    if (*(*pkrel).rd_rel).relkind != RELKIND_RELATION as i8
        && (*(*pkrel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "referenced relation \"{}\" is not a table",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    if !allowSystemTableMods && IsSystemRelation(pkrel) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg!(
                "permission denied: \"{}\" is a system catalog",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /*
     * References from permanent or unlogged tables to temp tables, and from
     * permanent tables to unlogged tables, are disallowed.
     */
    match (*(*rel).rd_rel).relpersistence {
        p if p == RELPERSISTENCE_PERMANENT as i8 => {
            if !RelationIsPermanent(pkrel) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on permanent tables may reference only permanent tables"
                    )
                );
            }
        }
        p if p == RELPERSISTENCE_UNLOGGED as i8 => {
            if !RelationIsPermanent(pkrel)
                && (*(*pkrel).rd_rel).relpersistence != RELPERSISTENCE_UNLOGGED as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on unlogged tables may reference only permanent or unlogged tables"
                    )
                );
            }
        }
        p if p == RELPERSISTENCE_TEMP as i8 => {
            if (*(*pkrel).rd_rel).relpersistence != RELPERSISTENCE_TEMP as i8 {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on temporary tables may reference only temporary tables"
                    )
                );
            }
            if !(*pkrel).rd_islocaltemp || !(*rel).rd_islocaltemp {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg!(
                        "constraints on temporary tables must involve temporary tables of this session"
                    )
                );
            }
        }
        _ => {}
    }

    /*
     * Look up the referencing attributes.
     */
    numfks = transformColumnNameList(
        RelationGetRelid(rel),
        (*fkconstraint).fk_attrs,
        fkattnum.as_mut_ptr(),
        fktypoid.as_mut_ptr(),
        fkcolloid.as_mut_ptr(),
    );
    with_period = (*fkconstraint).fk_with_period || (*fkconstraint).pk_with_period;
    if with_period && !(*fkconstraint).fk_with_period {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "foreign key uses PERIOD on the referenced table but not the referencing table"
            )
        );
    }

    let num_fk_del_set_cols_raw = transformColumnNameList(
        RelationGetRelid(rel),
        (*fkconstraint).fk_del_set_cols,
        fkdelsetcols.as_mut_ptr(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    numfkdelsetcols = validateFkOnDeleteSetColumns(
        numfks,
        fkattnum.as_ptr(),
        num_fk_del_set_cols_raw,
        fkdelsetcols.as_mut_ptr(),
        (*fkconstraint).fk_del_set_cols,
    );

    /*
     * If the attribute list for the referenced table was omitted, lookup the
     * definition of the primary key.
     */
    if (*fkconstraint).pk_attrs.is_null() {
        numpks = transformFkeyGetPrimaryKey(
            pkrel,
            &mut index_oid,
            &mut (*fkconstraint).pk_attrs,
            pkattnum.as_mut_ptr(),
            pktypoid.as_mut_ptr(),
            pkcolloid.as_mut_ptr(),
            opclasses.as_mut_ptr(),
            &mut pk_has_without_overlaps,
        );

        /* If the primary key uses WITHOUT OVERLAPS, the fk must use PERIOD */
        if pk_has_without_overlaps && !(*fkconstraint).fk_with_period {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "foreign key uses PERIOD on the referenced table but not the referencing table"
                )
            );
        }
    } else {
        numpks = transformColumnNameList(
            RelationGetRelid(pkrel),
            (*fkconstraint).pk_attrs,
            pkattnum.as_mut_ptr(),
            pktypoid.as_mut_ptr(),
            pkcolloid.as_mut_ptr(),
        );

        /* Since we got pk_attrs, one should be a period. */
        if with_period && !(*fkconstraint).pk_with_period {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "foreign key uses PERIOD on the referencing table but not the referenced table"
                )
            );
        }

        /* Look for an index matching the column list */
        index_oid = transformFkeyCheckAttrs(
            pkrel,
            numpks,
            pkattnum.as_mut_ptr(),
            with_period,
            opclasses.as_mut_ptr(),
            &mut pk_has_without_overlaps,
        );
    }

    /*
     * If the referenced primary key has WITHOUT OVERLAPS, the foreign key must use PERIOD.
     */
    if pk_has_without_overlaps && !with_period {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "foreign key must use PERIOD when referencing a primary key using WITHOUT OVERLAPS"
            )
        );
    }

    /* Now we can check permissions. */
    checkFkeyPermissions(pkrel, pkattnum.as_mut_ptr(), numpks);

    /* Check some things for generated columns. */
    for i in 0..numfks as usize {
        let attgenerated = (*TupleDescAttr(RelationGetDescr(rel), fkattnum[i] as usize - 1)).attgenerated;

        if attgenerated != 0 {
            if (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETNULL as i8
                || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETDEFAULT as i8
                || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_CASCADE as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg!(
                        "invalid {} action for foreign key constraint containing generated column",
                        "ON UPDATE"
                    )
                );
            }
            if (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETNULL as i8
                || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETDEFAULT as i8
            {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg!(
                        "invalid {} action for foreign key constraint containing generated column",
                        "ON DELETE"
                    )
                );
            }
        }

        /*
         * FKs on virtual columns are not supported.
         */
        if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "foreign key constraints on virtual generated columns are not supported"
                )
            );
        }
    }

    /*
     * Some actions are currently unsupported for foreign keys using PERIOD.
     */
    if (*fkconstraint).fk_with_period {
        if (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_RESTRICT as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_CASCADE as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETNULL as i8
            || (*fkconstraint).fk_upd_action == FKCONSTR_ACTION_SETDEFAULT as i8
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "unsupported {} action for foreign key constraint using PERIOD",
                    "ON UPDATE"
                )
            );
        }

        if (*fkconstraint).fk_del_action == FKCONSTR_ACTION_RESTRICT as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_CASCADE as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETNULL as i8
            || (*fkconstraint).fk_del_action == FKCONSTR_ACTION_SETDEFAULT as i8
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "unsupported {} action for foreign key constraint using PERIOD",
                    "ON DELETE"
                )
            );
        }
    }

    if numfks != numpks {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_FOREIGN_KEY),
            errmsg!(
                "number of referencing and referenced columns for foreign key disagree"
            )
        );
    }

    /*
     * On the strength of a previous constraint, we might avoid scanning tables.
     */
    old_check_ok = !(*fkconstraint).old_conpfeqop.is_null();
    Assert!(!old_check_ok || numfks == list_length((*fkconstraint).old_conpfeqop));

    old_pfeqop_item = list_head((*fkconstraint).old_conpfeqop);
    let mut old_pfeqop_item = old_pfeqop_item;

    for i in 0..numpks as usize {
        let pktype = pktypoid[i];
        let fktype = fktypoid[i];
        let pkcoll = pkcolloid[i];
        let fkcoll = fkcolloid[i];
        let cla_ht: HeapTuple;
        let cla_tup: Form_pg_opclass;
        let amid: Oid;
        let opfamily: Oid;
        let opcintype: Oid;
        let for_overlaps: bool;
        let cmptype: CompareType;
        let mut pfeqop: Oid = InvalidOid;
        let mut ppeqop: Oid;
        let mut ffeqop: Oid = InvalidOid;
        let eqstrategy: i16;
        let mut pfeqop_right: Oid = InvalidOid;

        /* We need several fields out of the pg_opclass entry */
        cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclasses[i]));
        if !HeapTupleIsValid(cla_ht) {
            elog!(ERROR, "cache lookup failed for opclass {}", opclasses[i]);
        }
        cla_tup = GETSTRUCT(cla_ht) as Form_pg_opclass;
        amid = (*cla_tup).opcmethod;
        opfamily = (*cla_tup).opcfamily;
        opcintype = (*cla_tup).opcintype;
        ReleaseSysCache(cla_ht);

        for_overlaps = with_period && i == numpks as usize - 1;
        cmptype = if for_overlaps { COMPARE_OVERLAP } else { COMPARE_EQ };
        eqstrategy = IndexAmTranslateCompareType(cmptype, amid, opfamily, true);
        if eqstrategy == InvalidStrategy as i16 {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg!(
                    "{}",
                    if for_overlaps {
                        "could not identify an overlaps operator for foreign key"
                    } else {
                        "could not identify an equality operator for foreign key"
                    }
                )
            );
        }

        /* There had better be a primary equality operator for the index. */
        ppeqop = get_opfamily_member(opfamily, opcintype, opcintype, eqstrategy);
        if !OidIsValid(ppeqop) {
            elog!(
                ERROR,
                "missing operator {}({},{}) in opfamily {}",
                eqstrategy, opcintype, opcintype, opfamily
            );
        }

        /* Are there equality operators that take exactly the FK type? */
        let fktyped = getBaseType(fktype);
        pfeqop = get_opfamily_member(opfamily, opcintype, fktyped, eqstrategy);
        if OidIsValid(pfeqop) {
            pfeqop_right = fktyped;
            ffeqop = get_opfamily_member(opfamily, fktyped, fktyped, eqstrategy);
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            /*
             * Otherwise, look for an implicit cast from the FK type to the opcintype.
             */
            let input_typeids = [pktype, fktype];
            let target_typeids = [opcintype, opcintype];
            if can_coerce_type(
                2,
                input_typeids.as_ptr(),
                target_typeids.as_ptr(),
                COERCION_IMPLICIT,
            ) {
                pfeqop = ppeqop;
                ffeqop = ppeqop;
                pfeqop_right = opcintype;
            }
        }

        if !(OidIsValid(pfeqop) && OidIsValid(ffeqop)) {
            ereport!(
                ERROR,
                errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg!(
                    "foreign key constraint \"{}\" cannot be implemented",
                    std::ffi::CStr::from_ptr((*fkconstraint).conname).to_string_lossy()
                )
                /* errdetail: Key columns ... are of incompatible types */
            );
        }

        /* Collation checks */
        if OidIsValid(pkcoll) && OidIsValid(fkcoll) {
            let pkcolldet = get_collation_isdeterministic(pkcoll);
            let fkcolldet = get_collation_isdeterministic(fkcoll);

            if (!pkcolldet || !fkcolldet) && pkcoll != fkcoll {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_COLLATION_MISMATCH),
                    errmsg!(
                        "foreign key constraint \"{}\" cannot be implemented",
                        std::ffi::CStr::from_ptr((*fkconstraint).conname).to_string_lossy()
                    )
                    /* errdetail */
                );
            }
        }

        if old_check_ok {
            /*
             * When a pfeqop changes, revalidate the constraint.
             */
            let oid_pfeqop = lfirst_oid(old_pfeqop_item);
            old_check_ok = pfeqop == oid_pfeqop;
            old_pfeqop_item = lnext((*fkconstraint).old_conpfeqop, old_pfeqop_item);
        }
        if old_check_ok && !(*tab).oldDesc.is_null() {
            let attr = TupleDescAttr((*tab).oldDesc, fkattnum[i] as usize - 1);
            let old_fktype = (*attr).atttypid;
            let new_fktype = fktype;
            let mut old_castfunc = InvalidOid;
            let mut new_castfunc = InvalidOid;
            let old_pathtype = findFkeyCast(pfeqop_right, old_fktype, &mut old_castfunc);
            let new_pathtype = findFkeyCast(pfeqop_right, new_fktype, &mut new_castfunc);
            let old_fkcoll = (*attr).attcollation;
            let new_fkcoll = fkcoll;

            old_check_ok = new_pathtype == old_pathtype
                && new_castfunc == old_castfunc
                && (!IsPolymorphicType(pfeqop_right) || new_fktype == old_fktype)
                && (new_fkcoll == old_fkcoll
                    || (get_collation_isdeterministic(old_fkcoll)
                        && get_collation_isdeterministic(new_fkcoll)));
        }

        pfeqoperators[i] = pfeqop;
        ppeqoperators[i] = ppeqop;
        ffeqoperators[i] = ffeqop;
    }

    /*
     * For FKs with PERIOD we need additional operators.
     */
    if with_period {
        let mut periodoperoid = InvalidOid;
        let mut aggedperiodoperoid = InvalidOid;
        let mut intersectoperoid = InvalidOid;
        FindFKPeriodOpers(
            opclasses[(numpks as usize) - 1],
            &mut periodoperoid,
            &mut aggedperiodoperoid,
            &mut intersectoperoid,
        );
    }

    /* First, create the constraint catalog entry itself. */
    let address = addFkConstraint(
        addFkBothSides,
        (*fkconstraint).conname,
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        InvalidOid, /* no parent constraint */
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        false,
        with_period,
    );

    /* Next process the action triggers at the referenced side and recurse */
    addFkRecurseReferenced(
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        address.objectId,
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        old_check_ok,
        InvalidOid,
        InvalidOid,
        with_period,
    );

    /* Lastly create the check triggers at the referencing side and recurse */
    addFkRecurseReferencing(
        wqueue,
        fkconstraint,
        rel,
        pkrel,
        index_oid,
        address.objectId,
        numfks,
        pkattnum.as_mut_ptr(),
        fkattnum.as_mut_ptr(),
        pfeqoperators.as_mut_ptr(),
        ppeqoperators.as_mut_ptr(),
        ffeqoperators.as_mut_ptr(),
        numfkdelsetcols,
        fkdelsetcols.as_mut_ptr(),
        old_check_ok,
        lockmode,
        InvalidOid,
        InvalidOid,
        with_period,
    );

    /* Done. Close pk table, but keep lock until we've committed. */
    table_close(pkrel, NoLock);

    address
}

/*
 * validateFkOnDeleteSetColumns
 *   Verifies that columns used in ON DELETE SET NULL/DEFAULT column lists are valid.
 */
unsafe fn validate_fk_on_delete_set_columns(
    numfks: i32,
    fkattnums: *const i16,
    numfksetcols: i32,
    fksetcolsattnums: *mut i16,
    fksetcols: *mut List,
) -> i32 {
    let mut numcolsout: i32 = 0;

    for i in 0..numfksetcols as usize {
        let setcol_attnum = *fksetcolsattnums.add(i);
        let mut seen = false;

        /* Make sure it's in fkattnums[] */
        for j in 0..numfks as usize {
            if *fkattnums.add(j) == setcol_attnum {
                seen = true;
                break;
            }
        }

        if !seen {
            let col = strVal(list_nth(fksetcols, i as i32) as *mut Node);
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg!(
                    "column \"{}\" referenced in ON DELETE SET action must be part of foreign key",
                    std::ffi::CStr::from_ptr(col).to_string_lossy()
                )
            );
        }

        /* Now check for dups */
        seen = false;
        for j in 0..numcolsout as usize {
            if *fksetcolsattnums.add(j) == setcol_attnum {
                seen = true;
                break;
            }
        }
        if !seen {
            *fksetcolsattnums.add(numcolsout as usize) = setcol_attnum;
            numcolsout += 1;
        }
    }
    numcolsout
}

/*
 * addFkConstraint
 *   Install pg_constraint entries to implement a foreign key constraint.
 */
unsafe fn add_fk_constraint(
    fkside: addFkConstraintSides,
    constraintname: *mut i8,
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    is_internal: bool,
    with_period: bool,
) -> ObjectAddress {
    let constr_oid: Oid;
    let conname: *mut i8;
    let conislocal: bool;
    let coninhcount: i16;
    let connoinherit: bool;

    /*
     * Verify relkind for each referenced partition.
     */
    if (*(*pkrel).rd_rel).relkind != RELKIND_RELATION as i8
        && (*(*pkrel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "referenced relation \"{}\" is not a table",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /*
     * Caller supplies us with a constraint name; however, it may be used in
     * this partition, so come up with a different one in that case.
     */
    if ConstraintNameIsUsed(CONSTRAINT_RELATION, RelationGetRelid(rel), constraintname) {
        conname = ChooseConstraintName(
            constraintname,
            std::ptr::null_mut(),
            b"\0".as_ptr() as *const i8,
            RelationGetNamespace(rel),
            std::ptr::null_mut(),
        );
    } else {
        conname = constraintname;
    }

    if (*fkconstraint).conname.is_null() {
        (*fkconstraint).conname = pstrdup(conname);
    }

    if OidIsValid(parent_constr) {
        conislocal = false;
        coninhcount = 1;
        connoinherit = false;
    } else {
        conislocal = true;
        coninhcount = 0;
        /*
         * always inherit for partitioned tables, never for legacy inheritance
         */
        connoinherit = (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE as i8;
    }

    /* Record the FK constraint in pg_constraint. */
    constr_oid = CreateConstraintEntry(
        conname,
        RelationGetNamespace(rel),
        CONSTRAINT_FOREIGN as i8,
        (*fkconstraint).deferrable,
        (*fkconstraint).initdeferred,
        (*fkconstraint).is_enforced,
        (*fkconstraint).initially_valid,
        parent_constr,
        RelationGetRelid(rel),
        fkattnum,
        numfks,
        numfks,
        InvalidOid, /* not a domain constraint */
        index_oid,
        RelationGetRelid(pkrel),
        pkattnum,
        pfeqoperators,
        ppeqoperators,
        ffeqoperators,
        numfks,
        (*fkconstraint).fk_upd_action,
        (*fkconstraint).fk_del_action,
        fkdelsetcols,
        numfkdelsetcols,
        (*fkconstraint).fk_matchtype,
        std::ptr::null_mut(), /* no exclusion constraint */
        std::ptr::null_mut(), /* no check constraint */
        std::ptr::null_mut(),
        conislocal,   /* islocal */
        coninhcount,  /* inhcount */
        connoinherit, /* conNoInherit */
        with_period,  /* conPeriod */
        is_internal,  /* is_internal */
    );

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    ObjectAddressSet!(address, ConstraintRelationId, constr_oid);

    /*
     * In partitioning cases, create the dependency entries for this constraint.
     */
    if OidIsValid(parent_constr) {
        let mut referenced = ObjectAddress {
            classId: InvalidOid,
            objectId: InvalidOid,
            objectSubId: 0,
        };
        ObjectAddressSet!(referenced, ConstraintRelationId, parent_constr);

        Assert!(fkside != addFkBothSides);
        if fkside == addFkReferencedSide {
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_INTERNAL);
        } else {
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_PARTITION_PRI);
            ObjectAddressSet!(referenced, RelationRelationId, RelationGetRelid(rel));
            recordDependencyOn(&mut address, &mut referenced, DEPENDENCY_PARTITION_SEC);
        }
    }

    /* make new constraint visible, in case we add more */
    CommandCounterIncrement();

    address
}

/*
 * addFkRecurseReferenced
 *   Recursive helper for the referenced side of foreign key creation.
 */
unsafe fn add_fk_recurse_referenced(
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    old_check_ok: bool,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) {
    let mut delete_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    Assert!(CheckRelationLockedByMe(pkrel, ShareRowExclusiveLock, true));
    Assert!(CheckRelationLockedByMe(rel, ShareRowExclusiveLock, true));

    /*
     * Create action triggers to enforce the constraint, or skip if NOT ENFORCED.
     */
    if (*fkconstraint).is_enforced {
        createForeignKeyActionTriggers(
            RelationGetRelid(rel),
            RelationGetRelid(pkrel),
            fkconstraint,
            parent_constr,
            index_oid,
            parent_del_trigger,
            parent_upd_trigger,
            &mut delete_trigger_oid,
            &mut update_trigger_oid,
        );
    }

    /*
     * If the referenced table is partitioned, recurse.
     */
    if (*(*pkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        let pd = RelationGetPartitionDesc(pkrel, true);

        for i in 0..(*pd).nparts as usize {
            let part_rel = table_open((*pd).oids[i], ShareRowExclusiveLock);
            let map = build_attrmap_by_name_if_req(
                RelationGetDescr(part_rel),
                RelationGetDescr(pkrel),
                false,
            );
            let mapped_pkattnum: *mut AttrNumber;
            let mapped_pkattnum_buf: *mut AttrNumber;

            if !map.is_null() {
                mapped_pkattnum_buf =
                    palloc(core::mem::size_of::<AttrNumber>() * numfks as usize)
                        as *mut AttrNumber;
                for j in 0..numfks as usize {
                    *mapped_pkattnum_buf.add(j) =
                        (*map).attnums[(*pkattnum.add(j) as usize) - 1];
                }
                mapped_pkattnum = mapped_pkattnum_buf;
            } else {
                mapped_pkattnum = pkattnum;
            }

            let part_index_id = index_get_partition(part_rel, index_oid);
            if !OidIsValid(part_index_id) {
                elog!(
                    ERROR,
                    "index for {} not found in partition {}",
                    index_oid,
                    std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy()
                );
            }

            /* Create entry at this level ... */
            let sub_address = add_fk_constraint(
                addFkReferencedSide,
                (*fkconstraint).conname,
                fkconstraint,
                rel,
                part_rel,
                part_index_id,
                parent_constr,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            );
            /* ... and recurse to our children */
            add_fk_recurse_referenced(
                fkconstraint,
                rel,
                part_rel,
                part_index_id,
                sub_address.objectId,
                numfks,
                mapped_pkattnum,
                fkattnum,
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                delete_trigger_oid,
                update_trigger_oid,
                with_period,
            );

            /* Done -- clean up (but keep the lock) */
            table_close(part_rel, NoLock);
            if !map.is_null() {
                pfree(mapped_pkattnum as *mut _);
                free_attrmap(map);
            }
        }
    }
}

/*
 * addFkRecurseReferencing
 *   Recursive helper for the referencing side of foreign key creation.
 */
unsafe fn add_fk_recurse_referencing(
    wqueue: *mut *mut List,
    fkconstraint: *mut Constraint,
    rel: Relation,
    pkrel: Relation,
    index_oid: Oid,
    parent_constr: Oid,
    numfks: i32,
    pkattnum: *mut i16,
    fkattnum: *mut i16,
    pfeqoperators: *mut Oid,
    ppeqoperators: *mut Oid,
    ffeqoperators: *mut Oid,
    numfkdelsetcols: i32,
    fkdelsetcols: *mut i16,
    old_check_ok: bool,
    lockmode: LOCKMODE,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    with_period: bool,
) {
    let mut insert_trigger_oid = InvalidOid;
    let mut update_trigger_oid = InvalidOid;

    Assert!(OidIsValid(parent_constr));
    Assert!(CheckRelationLockedByMe(rel, ShareRowExclusiveLock, true));
    Assert!(CheckRelationLockedByMe(pkrel, ShareRowExclusiveLock, true));

    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("foreign key constraints are not supported on foreign tables")
        );
    }

    /*
     * Add check triggers if the constraint is ENFORCED.
     */
    if (*fkconstraint).is_enforced {
        createForeignKeyCheckTriggers(
            RelationGetRelid(rel),
            RelationGetRelid(pkrel),
            fkconstraint,
            parent_constr,
            index_oid,
            parent_ins_trigger,
            parent_upd_trigger,
            &mut insert_trigger_oid,
            &mut update_trigger_oid,
        );
    }

    if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8 {
        /*
         * Tell Phase 3 to check that the constraint is satisfied by existing rows.
         */
        if !wqueue.is_null()
            && !old_check_ok
            && !(*fkconstraint).skip_validation
            && (*fkconstraint).is_enforced
        {
            let tab = ATGetQueueEntry(wqueue, rel);
            let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = get_constraint_name(parent_constr);
            (*newcon).contype = CONSTR_FOREIGN;
            (*newcon).refrelid = RelationGetRelid(pkrel);
            (*newcon).refindid = index_oid;
            (*newcon).conid = parent_constr;
            (*newcon).conwithperiod = (*fkconstraint).fk_with_period;
            (*newcon).qual = fkconstraint as *mut Node;

            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }
    } else if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
        let pd = RelationGetPartitionDesc(rel, true);
        let trigrel = table_open(TriggerRelationId, RowExclusiveLock);

        /*
         * Recurse to take appropriate action on each partition.
         */
        for i in 0..(*pd).nparts as usize {
            let partition = table_open((*pd).oids[i], lockmode);
            let attmap = build_attrmap_by_name(
                RelationGetDescr(partition),
                RelationGetDescr(rel),
                false,
            );
            let mut mapped_fkattnum = [0 as AttrNumber; INDEX_MAX_KEYS];
            for j in 0..numfks as usize {
                mapped_fkattnum[j] = (*attmap).attnums[(*fkattnum.add(j) as usize) - 1];
            }

            CheckAlterTableIsSafe(partition);

            /* Check whether an existing constraint can be repurposed */
            let part_fks = copyObject(RelationGetFKeyList(partition)) as *mut List;
            let mut attached = false;
            let mut fklc = list_head(part_fks);
            while !fklc.is_null() {
                let fk = lfirst_node!(ForeignKeyCacheInfo, T_ForeignKeyCacheInfo, current_cell!(fklc));
                if try_attach_partition_foreign_key(
                    wqueue,
                    fk,
                    partition,
                    parent_constr,
                    numfks,
                    mapped_fkattnum.as_mut_ptr(),
                    pkattnum,
                    pfeqoperators,
                    insert_trigger_oid,
                    update_trigger_oid,
                    trigrel,
                ) {
                    attached = true;
                    break;
                }
                fklc = lnext(part_fks, fklc);
            }

            if attached {
                table_close(partition, NoLock);
                continue;
            }

            /*
             * No luck finding a good constraint to reuse; create our own.
             */
            let sub_address = add_fk_constraint(
                addFkReferencingSide,
                (*fkconstraint).conname,
                fkconstraint,
                partition,
                pkrel,
                index_oid,
                parent_constr,
                numfks,
                pkattnum,
                mapped_fkattnum.as_mut_ptr(),
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                true,
                with_period,
            );

            add_fk_recurse_referencing(
                wqueue,
                fkconstraint,
                partition,
                pkrel,
                index_oid,
                sub_address.objectId,
                numfks,
                pkattnum,
                mapped_fkattnum.as_mut_ptr(),
                pfeqoperators,
                ppeqoperators,
                ffeqoperators,
                numfkdelsetcols,
                fkdelsetcols,
                old_check_ok,
                lockmode,
                insert_trigger_oid,
                update_trigger_oid,
                with_period,
            );

            table_close(partition, NoLock);
        }

        table_close(trigrel, RowExclusiveLock);
    }
}

/*
 * CloneForeignKeyConstraints
 *   Clone foreign keys from a partitioned table to a newly acquired partition.
 */
unsafe fn clone_foreign_key_constraints(
    wqueue: *mut *mut List,
    parent_rel: Relation,
    partition_rel: Relation,
) {
    /* This only works for declarative partitioning */
    Assert!((*(*parent_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8);

    /*
     * First, clone constraints where the parent is on the referencing side.
     */
    clone_fk_referencing(wqueue, parent_rel, partition_rel);

    /*
     * Clone constraints for which the parent is on the referenced side.
     */
    clone_fk_referenced(parent_rel, partition_rel);
}

/*
 * CloneFkReferenced
 *   Find all the FKs that have the parent relation on the referenced side;
 *   clone those constraints to the given partition.
 */
unsafe fn clone_fk_referenced(parent_rel: Relation, partition_rel: Relation) {
    let pg_constraint: Relation;
    let attmap: *mut AttrMap;
    let mut clone: *mut List = std::ptr::null_mut();
    let trigrel: Relation;

    /*
     * Search for any constraints where this partition's parent is in the
     * referenced side. Build the list to clone in two steps to avoid duplicates.
     */
    pg_constraint = table_open(ConstraintRelationId, RowShareLock);

    let mut key = [ScanKeyData::default(); 2];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_constraint_confrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(parent_rel)),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_constraint_contype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(CONSTRAINT_FOREIGN as i8 as i64),
    );
    /* This is a seqscan, as we don't have a usable index ... */
    let scan = systable_beginscan(
        pg_constraint,
        InvalidOid,
        true,
        std::ptr::null_mut(),
        2,
        key.as_mut_ptr(),
    );
    let mut tuple: HeapTuple;
    loop {
        tuple = systable_getnext(scan);
        if tuple.is_null() { break; }
        let constr_form = GETSTRUCT(tuple) as Form_pg_constraint;
        clone = lappend_oid(clone, (*constr_form).oid);
    }
    systable_endscan(scan);
    table_close(pg_constraint, RowShareLock);

    /*
     * Triggers will be manipulated a bunch of times in the loop below.
     */
    trigrel = table_open(TriggerRelationId, RowExclusiveLock);

    attmap = build_attrmap_by_name(
        RelationGetDescr(partition_rel),
        RelationGetDescr(parent_rel),
        false,
    );

    let mut cell = list_head(clone);
    while !cell.is_null() {
        let constr_oid = lfirst_oid(cell);
        let constr_form: Form_pg_constraint;
        let fk_rel: Relation;
        let index_oid: Oid;
        let part_index_id: Oid;
        let mut numfks: i32 = 0;
        let mut conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut mapped_confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut conpfeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conppeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conffeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut numfkdelsetcols: i32 = 0;
        let mut confdelsetcols = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut delete_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        let con_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constr_oid));
        if !HeapTupleIsValid(con_tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", constr_oid);
        }
        constr_form = GETSTRUCT(con_tuple) as Form_pg_constraint;

        /*
         * As explained above: don't try to clone a constraint for which we're
         * going to clone the parent.
         */
        if list_member_oid(clone, (*constr_form).conparentid) {
            ReleaseSysCache(con_tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /* We need the same lock level that CreateTrigger will acquire */
        fk_rel = table_open((*constr_form).conrelid, ShareRowExclusiveLock);
        index_oid = (*constr_form).conindid;

        DeconstructFkConstraintRow(
            con_tuple,
            &mut numfks,
            conkey.as_mut_ptr(),
            confkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            &mut numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
        );

        for i in 0..numfks as usize {
            mapped_confkey[i] = (*attmap).attnums[(confkey[i] as usize) - 1];
        }

        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).contype = CONSTRAINT_FOREIGN;
        (*fkconstraint).conname = NameStr!((*constr_form).conname) as *mut i8;
        (*fkconstraint).deferrable = (*constr_form).condeferrable;
        (*fkconstraint).initdeferred = (*constr_form).condeferred;
        (*fkconstraint).location = -1;
        (*fkconstraint).pktable = std::ptr::null_mut();
        (*fkconstraint).pk_attrs = std::ptr::null_mut();
        (*fkconstraint).fk_matchtype = (*constr_form).confmatchtype;
        (*fkconstraint).fk_upd_action = (*constr_form).confupdtype;
        (*fkconstraint).fk_del_action = (*constr_form).confdeltype;
        (*fkconstraint).fk_del_set_cols = std::ptr::null_mut();
        (*fkconstraint).old_conpfeqop = std::ptr::null_mut();
        (*fkconstraint).old_pktable_oid = InvalidOid;
        (*fkconstraint).is_enforced = (*constr_form).conenforced;
        (*fkconstraint).skip_validation = false;
        (*fkconstraint).initially_valid = (*constr_form).convalidated;

        /* set up colnames that are used to generate the constraint name */
        for i in 0..numfks as usize {
            let att = TupleDescAttr(RelationGetDescr(fk_rel), conkey[i] as usize - 1);
            (*fkconstraint).fk_attrs = lappend(
                (*fkconstraint).fk_attrs,
                makeString(NameStr!((*att).attname) as *mut i8) as *mut _,
            );
        }

        /*
         * Add the new foreign key constraint pointing to the new partition.
         */
        part_index_id = index_get_partition(partition_rel, index_oid);
        if !OidIsValid(part_index_id) {
            elog!(
                ERROR,
                "index for {} not found in partition {}",
                index_oid,
                std::ffi::CStr::from_ptr(RelationGetRelationName(partition_rel)).to_string_lossy()
            );
        }

        /*
         * Get the "action" triggers belonging to the constraint.
         */
        if (*constr_form).conenforced {
            GetForeignKeyActionTriggers(
                trigrel,
                constr_oid,
                (*constr_form).confrelid,
                (*constr_form).conrelid,
                &mut delete_trigger_oid,
                &mut update_trigger_oid,
            );
        }

        /* Add this constraint ... */
        let sub_address = add_fk_constraint(
            addFkReferencedSide,
            (*fkconstraint).conname,
            fkconstraint,
            fk_rel,
            partition_rel,
            part_index_id,
            constr_oid,
            numfks,
            mapped_confkey.as_mut_ptr(),
            conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false,
            (*constr_form).conperiod,
        );
        /* ... and recurse */
        add_fk_recurse_referenced(
            fkconstraint,
            fk_rel,
            partition_rel,
            part_index_id,
            sub_address.objectId,
            numfks,
            mapped_confkey.as_mut_ptr(),
            conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            true,
            delete_trigger_oid,
            update_trigger_oid,
            (*constr_form).conperiod,
        );

        table_close(fk_rel, NoLock);
        ReleaseSysCache(con_tuple);
        cell = lnext(clone, cell);
    }

    table_close(trigrel, RowExclusiveLock);
}

/*
 * CloneFkReferencing
 *   For each FK constraint of the parent relation, find an equivalent constraint
 *   in its partition relation that can be reparented, or create a new one.
 */
unsafe fn clone_fk_referencing(
    wqueue: *mut *mut List,
    parent_rel: Relation,
    part_rel: Relation,
) {
    let attmap: *mut AttrMap;
    let part_fks: *mut List;
    let mut clone: *mut List = std::ptr::null_mut();
    let trigrel: Relation;

    /* obtain a list of constraints that we need to clone */
    let fk_list = RelationGetFKeyList(parent_rel);
    let mut fk_lc = list_head(fk_list);
    while !fk_lc.is_null() {
        let fk = lfirst(fk_lc) as *mut ForeignKeyCacheInfo;

        /*
         * Refuse to attach a table as partition that this partitioned table
         * already has a foreign key to.
         */
        if (*fk).confrelid == RelationGetRelid(part_rel) {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!(
                    "cannot attach table \"{}\" as a partition because it is referenced by foreign key \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(part_rel)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(get_constraint_name((*fk).conoid)).to_string_lossy()
                )
            );
        }

        clone = lappend_oid(clone, (*fk).conoid);
        fk_lc = lnext(fk_list, fk_lc);
    }

    /* Silently do nothing if there's nothing to do. */
    if clone.is_null() {
        return;
    }

    if (*(*part_rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!("foreign key constraints are not supported on foreign tables")
        );
    }

    trigrel = table_open(TriggerRelationId, RowExclusiveLock);
    attmap = build_attrmap_by_name(
        RelationGetDescr(part_rel),
        RelationGetDescr(parent_rel),
        false,
    );
    part_fks = copyObject(RelationGetFKeyList(part_rel)) as *mut List;

    let mut cell = list_head(clone);
    while !cell.is_null() {
        let parent_constr_oid = lfirst_oid(cell);
        let constr_form: Form_pg_constraint;
        let pkrel: Relation;
        let mut numfks: i32 = 0;
        let mut conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut mapped_conkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut confkey = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut conpfeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conppeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut conffeqop = [InvalidOid; INDEX_MAX_KEYS];
        let mut numfkdelsetcols: i32 = 0;
        let mut confdelsetcols = [0 as AttrNumber; INDEX_MAX_KEYS];
        let mut insert_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        let tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
        }
        constr_form = GETSTRUCT(tuple) as Form_pg_constraint;

        /* Don't clone constraints whose parents are being cloned */
        if list_member_oid(clone, (*constr_form).conparentid) {
            ReleaseSysCache(tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /*
         * Need to prevent concurrent deletions.
         */
        pkrel = table_open((*constr_form).confrelid, ShareRowExclusiveLock);
        if (*(*pkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            find_all_inheritors(RelationGetRelid(pkrel), ShareRowExclusiveLock, std::ptr::null_mut());
        }

        DeconstructFkConstraintRow(
            tuple,
            &mut numfks,
            conkey.as_mut_ptr(),
            confkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            &mut numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
        );
        for i in 0..numfks as usize {
            mapped_conkey[i] = (*attmap).attnums[(conkey[i] as usize) - 1];
        }

        /*
         * Get the "check" triggers belonging to the constraint.
         */
        if (*constr_form).conenforced {
            GetForeignKeyCheckTriggers(
                trigrel,
                (*constr_form).oid,
                (*constr_form).confrelid,
                (*constr_form).conrelid,
                &mut insert_trigger_oid,
                &mut update_trigger_oid,
            );
        }

        /*
         * Before creating a new constraint, see whether any existing FKs are fit.
         */
        let mut attached = false;
        let mut fk_lc2 = list_head(part_fks);
        while !fk_lc2.is_null() {
            let fk = lfirst_node!(ForeignKeyCacheInfo, T_ForeignKeyCacheInfo, current_cell!(fk_lc2));
            if try_attach_partition_foreign_key(
                wqueue,
                fk,
                part_rel,
                parent_constr_oid,
                numfks,
                mapped_conkey.as_mut_ptr(),
                confkey.as_mut_ptr(),
                conpfeqop.as_mut_ptr(),
                insert_trigger_oid,
                update_trigger_oid,
                trigrel,
            ) {
                attached = true;
                table_close(pkrel, NoLock);
                break;
            }
            fk_lc2 = lnext(part_fks, fk_lc2);
        }
        if attached {
            ReleaseSysCache(tuple);
            cell = lnext(clone, cell);
            continue;
        }

        /* No dice.  Set up to create our own constraint */
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).contype = CONSTRAINT_FOREIGN;
        (*fkconstraint).deferrable = (*constr_form).condeferrable;
        (*fkconstraint).initdeferred = (*constr_form).condeferred;
        (*fkconstraint).location = -1;
        (*fkconstraint).pktable = std::ptr::null_mut();
        (*fkconstraint).pk_attrs = std::ptr::null_mut();
        (*fkconstraint).fk_matchtype = (*constr_form).confmatchtype;
        (*fkconstraint).fk_upd_action = (*constr_form).confupdtype;
        (*fkconstraint).fk_del_action = (*constr_form).confdeltype;
        (*fkconstraint).fk_del_set_cols = std::ptr::null_mut();
        (*fkconstraint).old_conpfeqop = std::ptr::null_mut();
        (*fkconstraint).old_pktable_oid = InvalidOid;
        (*fkconstraint).is_enforced = (*constr_form).conenforced;
        (*fkconstraint).skip_validation = false;
        (*fkconstraint).initially_valid = (*constr_form).convalidated;
        for i in 0..numfks as usize {
            let att = TupleDescAttr(RelationGetDescr(part_rel), mapped_conkey[i] as usize - 1);
            (*fkconstraint).fk_attrs = lappend(
                (*fkconstraint).fk_attrs,
                makeString(NameStr!((*att).attname) as *mut i8) as *mut _,
            );
        }

        let index_oid = (*constr_form).conindid;
        let with_period = (*constr_form).conperiod;

        /* Create the pg_constraint entry at this level */
        let sub_address = add_fk_constraint(
            addFkReferencingSide,
            NameStr!((*constr_form).conname) as *mut i8,
            fkconstraint,
            part_rel,
            pkrel,
            index_oid,
            parent_constr_oid,
            numfks,
            confkey.as_mut_ptr(),
            mapped_conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false,
            with_period,
        );

        /* Done with the cloned constraint's tuple */
        ReleaseSysCache(tuple);

        /* Create the check triggers, and recurse to partitions, if any */
        add_fk_recurse_referencing(
            wqueue,
            fkconstraint,
            part_rel,
            pkrel,
            index_oid,
            sub_address.objectId,
            numfks,
            confkey.as_mut_ptr(),
            mapped_conkey.as_mut_ptr(),
            conpfeqop.as_mut_ptr(),
            conppeqop.as_mut_ptr(),
            conffeqop.as_mut_ptr(),
            numfkdelsetcols,
            confdelsetcols.as_mut_ptr(),
            false, /* no old check exists */
            AccessExclusiveLock,
            insert_trigger_oid,
            update_trigger_oid,
            with_period,
        );
        table_close(pkrel, NoLock);
        cell = lnext(clone, cell);
    }

    table_close(trigrel, RowExclusiveLock);
}

/*
 * tryAttachPartitionForeignKey
 *   Examine whether an existing FK constraint on partition can be used
 *   as-is rather than creating a new one.
 */
unsafe fn try_attach_partition_foreign_key(
    wqueue: *mut *mut List,
    fk: *mut ForeignKeyCacheInfo,
    partition: Relation,
    parent_constr_oid: Oid,
    numfks: i32,
    mapped_conkey: *mut AttrNumber,
    confkey: *mut AttrNumber,
    conpfeqop: *mut Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: Relation,
) -> bool {
    let parent_constr_tup: HeapTuple;
    let parent_constr: Form_pg_constraint;
    let partcontup: HeapTuple;
    let part_constr: Form_pg_constraint;

    parent_constr_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
    if !HeapTupleIsValid(parent_constr_tup) {
        elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
    }
    parent_constr = GETSTRUCT(parent_constr_tup) as Form_pg_constraint;

    /* Quick initial checks */
    if (*fk).confrelid != (*parent_constr).confrelid || (*fk).nkeys != numfks {
        ReleaseSysCache(parent_constr_tup);
        return false;
    }
    for i in 0..numfks as usize {
        if (*fk).conkey[i] != *mapped_conkey.add(i)
            || (*fk).confkey[i] != *confkey.add(i)
            || (*fk).conpfeqop[i] != *conpfeqop.add(i)
        {
            ReleaseSysCache(parent_constr_tup);
            return false;
        }
    }

    /* More extensive checks */
    partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum((*fk).conoid));
    if !HeapTupleIsValid(partcontup) {
        elog!(ERROR, "cache lookup failed for constraint {}", (*fk).conoid);
    }
    part_constr = GETSTRUCT(partcontup) as Form_pg_constraint;

    /*
     * An error should be raised if the constraint enforceability is different.
     */
    if (*part_constr).conenforced != (*parent_constr).conenforced {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg!(
                "constraint \"{}\" enforceability conflicts with constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr(NameStr!((*parent_constr).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(NameStr!((*part_constr).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(partition)).to_string_lossy()
            )
        );
    }

    if OidIsValid((*part_constr).conparentid)
        || (*part_constr).condeferrable != (*parent_constr).condeferrable
        || (*part_constr).condeferred != (*parent_constr).condeferred
        || (*part_constr).confupdtype != (*parent_constr).confupdtype
        || (*part_constr).confdeltype != (*parent_constr).confdeltype
        || (*part_constr).confmatchtype != (*parent_constr).confmatchtype
    {
        ReleaseSysCache(parent_constr_tup);
        ReleaseSysCache(partcontup);
        return false;
    }

    ReleaseSysCache(parent_constr_tup);
    ReleaseSysCache(partcontup);

    /* Looks good! Attach this constraint */
    attach_partition_foreign_key(
        wqueue,
        partition,
        (*fk).conoid,
        parent_constr_oid,
        parent_ins_trigger,
        parent_upd_trigger,
        trigrel,
    );

    true
}

/*
 * AttachPartitionForeignKey
 *   Final tasks of attaching a FK constraint to a partition.
 */
unsafe fn attach_partition_foreign_key(
    wqueue: *mut *mut List,
    partition: Relation,
    part_constr_oid: Oid,
    parent_constr_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    trigrel: Relation,
) {
    let parent_constr_tup: HeapTuple;
    let parent_constr: Form_pg_constraint;
    let mut partcontup: HeapTuple;
    let part_constr: Form_pg_constraint;
    let queue_validation: bool;
    let part_constr_frelid: Oid;
    let part_constr_relid: Oid;
    let parent_constr_is_enforced: bool;

    parent_constr_tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent_constr_oid));
    if !HeapTupleIsValid(parent_constr_tup) {
        elog!(ERROR, "cache lookup failed for constraint {}", parent_constr_oid);
    }
    parent_constr = GETSTRUCT(parent_constr_tup) as Form_pg_constraint;
    parent_constr_is_enforced = (*parent_constr).conenforced;

    partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(part_constr_oid));
    if !HeapTupleIsValid(partcontup) {
        elog!(ERROR, "cache lookup failed for constraint {}", part_constr_oid);
    }
    part_constr = GETSTRUCT(partcontup) as Form_pg_constraint;
    part_constr_frelid = (*part_constr).confrelid;
    part_constr_relid = (*part_constr).conrelid;

    /*
     * If the referenced table is partitioned, remove extra pg_constraint rows
     * and action triggers that are no longer needed.
     */
    if get_rel_relkind(part_constr_frelid) == RELKIND_PARTITIONED_TABLE as i8 {
        let pg_constraint = table_open(ConstraintRelationId, RowShareLock);
        remove_inherited_constraint(pg_constraint, trigrel, part_constr_oid, part_constr_relid);
        table_close(pg_constraint, RowShareLock);
    }

    queue_validation = (*parent_constr).convalidated && !(*part_constr).convalidated;

    ReleaseSysCache(partcontup);
    ReleaseSysCache(parent_constr_tup);

    /*
     * The action triggers in the new partition become redundant -- remove them.
     */
    drop_foreign_key_constraint_triggers(trigrel, part_constr_oid, part_constr_frelid, part_constr_relid);

    ConstraintSetParentConstraint(part_constr_oid, parent_constr_oid, RelationGetRelid(partition));

    /*
     * Like the constraint, attach partition's "check" triggers to the
     * corresponding parent triggers if the constraint is ENFORCED.
     */
    if parent_constr_is_enforced {
        let mut insert_trigger_oid = InvalidOid;
        let mut update_trigger_oid = InvalidOid;

        GetForeignKeyCheckTriggers(
            trigrel,
            part_constr_oid,
            part_constr_frelid,
            part_constr_relid,
            &mut insert_trigger_oid,
            &mut update_trigger_oid,
        );
        Assert!(OidIsValid(insert_trigger_oid) && OidIsValid(parent_ins_trigger));
        TriggerSetParentTrigger(trigrel, insert_trigger_oid, parent_ins_trigger, RelationGetRelid(partition));
        Assert!(OidIsValid(update_trigger_oid) && OidIsValid(parent_upd_trigger));
        TriggerSetParentTrigger(trigrel, update_trigger_oid, parent_upd_trigger, RelationGetRelid(partition));
    }

    CommandCounterIncrement();

    if queue_validation {
        let conrel = table_open(ConstraintRelationId, RowExclusiveLock);
        partcontup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(part_constr_oid));
        if !HeapTupleIsValid(partcontup) {
            elog!(ERROR, "cache lookup failed for constraint {}", part_constr_oid);
        }
        let confrelid = (*(GETSTRUCT(partcontup) as Form_pg_constraint)).confrelid;
        /* Use the same lock as for AT_ValidateConstraint */
        QueueFKConstraintValidation(
            wqueue,
            conrel,
            partition,
            confrelid,
            partcontup,
            ShareUpdateExclusiveLock,
        );
        ReleaseSysCache(partcontup);
        table_close(conrel, RowExclusiveLock);
    }
}

/*
 * RemoveInheritedConstraint
 *   Remove the constraint and its associated triggers from the given relation,
 *   which inherited the given constraint.
 */
unsafe fn remove_inherited_constraint(
    conrel: Relation,
    trigrel: Relation,
    conoid: Oid,
    conrelid: Oid,
) {
    let objs: *mut ObjectAddresses;
    let mut consttup: HeapTuple;
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    ScanKeyInit(
        &mut key,
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conrelid),
    );
    scan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        std::ptr::null_mut(),
        1,
        &mut key,
    );
    objs = new_object_addresses();
    loop {
        consttup = systable_getnext(scan);
        if consttup.is_null() { break; }
        let conform = GETSTRUCT(consttup) as Form_pg_constraint;

        if (*conform).conparentid != conoid {
            continue;
        } else {
            let mut addr = ObjectAddress::default();
            let scan2: SysScanDesc;
            let mut key2 = ScanKeyData::default();

            ObjectAddressSet!(addr, ConstraintRelationId, (*conform).oid);
            add_exact_object_address(&mut addr, objs);

            /*
             * Delete the dependency record binding the two constraint records.
             */
            /* n = */ deleteDependencyRecordsForSpecific(
                ConstraintRelationId,
                (*conform).oid,
                DEPENDENCY_INTERNAL,
                ConstraintRelationId,
                conoid,
            );
            /* Assert n == 1 */

            /*
             * Now search for the triggers and set them up for deletion.
             */
            ScanKeyInit(
                &mut key2,
                Anum_pg_trigger_tgconstraint,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum((*conform).oid),
            );
            scan2 = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key2);
            loop {
                trigtup = systable_getnext(scan2);
                if trigtup.is_null() { break; }
                ObjectAddressSet!(addr, TriggerRelationId, (*(GETSTRUCT(trigtup) as Form_pg_trigger)).oid);
                add_exact_object_address(&mut addr, objs);
            }
            systable_endscan(scan2);
        }
    }
    /* make the dependency deletions visible */
    CommandCounterIncrement();
    performMultipleDeletions(objs, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);
    systable_endscan(scan);
}

/*
 * DropForeignKeyConstraintTriggers
 *   Delete action triggers for the given FK constraint.
 */
unsafe fn drop_foreign_key_constraint_triggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;
        let mut trigger_addr = ObjectAddress::default();

        /* Invalid if trigger is not for a referential integrity constraint */
        if !OidIsValid((*trgform).tgconstrrelid) {
            continue;
        }
        if OidIsValid(conrelid) && (*trgform).tgconstrrelid != conrelid {
            continue;
        }
        if OidIsValid(confrelid) && (*trgform).tgrelid != confrelid {
            continue;
        }

        /* We should be dropping trigger related to foreign key constraint */
        Assert!(
            (*trgform).tgfoid == F_RI_FKEY_CHECK_INS
                || (*trgform).tgfoid == F_RI_FKEY_CHECK_UPD
                || (*trgform).tgfoid == F_RI_FKEY_CASCADE_DEL
                || (*trgform).tgfoid == F_RI_FKEY_CASCADE_UPD
                || (*trgform).tgfoid == F_RI_FKEY_RESTRICT_DEL
                || (*trgform).tgfoid == F_RI_FKEY_RESTRICT_UPD
                || (*trgform).tgfoid == F_RI_FKEY_SETNULL_DEL
                || (*trgform).tgfoid == F_RI_FKEY_SETNULL_UPD
                || (*trgform).tgfoid == F_RI_FKEY_SETDEFAULT_DEL
                || (*trgform).tgfoid == F_RI_FKEY_SETDEFAULT_UPD
                || (*trgform).tgfoid == F_RI_FKEY_NOACTION_DEL
                || (*trgform).tgfoid == F_RI_FKEY_NOACTION_UPD
        );

        /*
         * Remove the dependency link so we can drop the trigger while
         * keeping the constraint intact.
         */
        deleteDependencyRecordsFor(TriggerRelationId, (*trgform).oid, false);
        /* make dependency deletion visible to performDeletion */
        CommandCounterIncrement();
        ObjectAddressSet!(trigger_addr, TriggerRelationId, (*trgform).oid);
        performDeletion(&trigger_addr, DROP_RESTRICT, 0);
        /* make trigger drop visible, in case the loop iterates */
        CommandCounterIncrement();
    }

    systable_endscan(scan);
}

/*
 * GetForeignKeyActionTriggers
 *   Returns delete and update "action" triggers of the given relation
 *   belonging to the given constraint.
 */
unsafe fn GetForeignKeyActionTriggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
    delete_trigger_oid: *mut Oid,
    update_trigger_oid: *mut Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    *delete_trigger_oid = InvalidOid;
    *update_trigger_oid = InvalidOid;
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );

    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;

        if (*trgform).tgconstrrelid != conrelid {
            continue;
        }
        if (*trgform).tgrelid != confrelid {
            continue;
        }
        /* Only ever look at "action" triggers on the PK side. */
        if RI_FKey_trigger_type((*trgform).tgfoid) != RI_TRIGGER_PK {
            continue;
        }
        if TRIGGER_FOR_DELETE((*trgform).tgtype) {
            Assert!(*delete_trigger_oid == InvalidOid);
            *delete_trigger_oid = (*trgform).oid;
        } else if TRIGGER_FOR_UPDATE((*trgform).tgtype) {
            Assert!(*update_trigger_oid == InvalidOid);
            *update_trigger_oid = (*trgform).oid;
        }
        /* In an assert-enabled build, continue looking to find duplicates */
        #[cfg(not(debug_assertions))]
        if OidIsValid(*delete_trigger_oid) && OidIsValid(*update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(*delete_trigger_oid) {
        elog!(ERROR, "could not find ON DELETE action trigger of foreign key constraint {}", conoid);
    }
    if !OidIsValid(*update_trigger_oid) {
        elog!(ERROR, "could not find ON UPDATE action trigger of foreign key constraint {}", conoid);
    }

    systable_endscan(scan);
}

/*
 * GetForeignKeyCheckTriggers
 *   Returns insert and update "check" triggers of the given relation
 *   belonging to the given constraint.
 */
unsafe fn GetForeignKeyCheckTriggers(
    trigrel: Relation,
    conoid: Oid,
    confrelid: Oid,
    conrelid: Oid,
    insert_trigger_oid: *mut Oid,
    update_trigger_oid: *mut Oid,
) {
    let mut key = ScanKeyData::default();
    let scan: SysScanDesc;
    let mut trigtup: HeapTuple;

    *insert_trigger_oid = InvalidOid;
    *update_trigger_oid = InvalidOid;
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );

    scan = systable_beginscan(trigrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut key);
    loop {
        trigtup = systable_getnext(scan);
        if trigtup.is_null() { break; }
        let trgform = GETSTRUCT(trigtup) as Form_pg_trigger;

        if (*trgform).tgconstrrelid != confrelid {
            continue;
        }
        if (*trgform).tgrelid != conrelid {
            continue;
        }
        /* Only ever look at "check" triggers on the FK side. */
        if RI_FKey_trigger_type((*trgform).tgfoid) != RI_TRIGGER_FK {
            continue;
        }
        if TRIGGER_FOR_INSERT((*trgform).tgtype) {
            Assert!(*insert_trigger_oid == InvalidOid);
            *insert_trigger_oid = (*trgform).oid;
        } else if TRIGGER_FOR_UPDATE((*trgform).tgtype) {
            Assert!(*update_trigger_oid == InvalidOid);
            *update_trigger_oid = (*trgform).oid;
        }
        /* In an assert-enabled build, continue looking to find duplicates. */
        #[cfg(not(debug_assertions))]
        if OidIsValid(*insert_trigger_oid) && OidIsValid(*update_trigger_oid) {
            break;
        }
    }

    if !OidIsValid(*insert_trigger_oid) {
        elog!(ERROR, "could not find ON INSERT check triggers of foreign key constraint {}", conoid);
    }
    if !OidIsValid(*update_trigger_oid) {
        elog!(ERROR, "could not find ON UPDATE check triggers of foreign key constraint {}", conoid);
    }

    systable_endscan(scan);
}

/*
 * ATExecAlterConstraint
 *   ALTER TABLE ALTER CONSTRAINT -- update attributes of a constraint.
 *   Currently only works for Foreign Key and not-null constraints.
 */
unsafe fn at_exec_alter_constraint(
    wqueue: *mut *mut List,
    rel: Relation,
    cmdcon: *mut ATAlterConstraint,
    recurse: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let tgrel: Relation;
    let scan: SysScanDesc;
    let mut skey = [ScanKeyData::default(); 3];
    let mut contuple: HeapTuple;
    let currcon: Form_pg_constraint;
    let mut address = InvalidObjectAddress;

    /*
     * Disallow altering ONLY a partitioned table, as it would make no sense.
     * This is okay for legacy inheritance.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 && !recurse {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg!("constraint must be altered in child tables too")
            /* errhint: Do not specify the ONLY keyword. */
        );
    }

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    /* Find and check the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum((*cmdcon).conname),
    );
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, std::ptr::null_mut(), 3, skey.as_mut_ptr());

    /* There can be at most one matching row */
    contuple = systable_getnext(scan);
    if !HeapTupleIsValid(contuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    if (*cmdcon).alterDeferrability && (*currcon).contype != CONSTRAINT_FOREIGN as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" is not a foreign key constraint",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    if (*cmdcon).alterEnforceability && (*currcon).contype != CONSTRAINT_FOREIGN as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot alter enforceability of constraint \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }
    if (*cmdcon).alterInheritability && (*currcon).contype != CONSTRAINT_NOTNULL as i8 {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" is not a not-null constraint",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /* Refuse to modify inheritability of inherited constraints */
    if (*cmdcon).alterInheritability && (*cmdcon).noinherit && (*currcon).coninhcount > 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot alter inherited constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr(NameStr!((*currcon).conname) as *mut i8).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    /*
     * If it's not the topmost constraint, raise an error.
     */
    if OidIsValid((*currcon).conparentid) {
        let mut parent = (*currcon).conparentid;
        let mut ancestor_name: *mut i8 = std::ptr::null_mut();
        let mut ancestor_table: *mut i8 = std::ptr::null_mut();

        /* Loop to find the topmost constraint */
        loop {
            let tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(parent));
            if !HeapTupleIsValid(tp) { break; }
            let contup = GETSTRUCT(tp) as Form_pg_constraint;
            if !OidIsValid((*contup).conparentid) {
                ancestor_name = pstrdup(NameStr!((*contup).conname) as *mut i8);
                ancestor_table = get_rel_name((*contup).conrelid);
                ReleaseSysCache(tp);
                break;
            }
            parent = (*contup).conparentid;
            ReleaseSysCache(tp);
        }

        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!(
                "cannot alter constraint \"{}\" on relation \"{}\"",
                std::ffi::CStr::from_ptr((*cmdcon).conname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errdetail and errhint omitted - see C source */
        );
    }

    /*
     * Do the actual catalog work, and recurse if necessary.
     */
    if at_exec_alter_constraint_internal(wqueue, cmdcon, conrel, tgrel, rel, contuple, recurse, lockmode) {
        ObjectAddressSet!(address, ConstraintRelationId, (*currcon).oid);
    }

    systable_endscan(scan);
    table_close(tgrel, RowExclusiveLock);
    table_close(conrel, RowExclusiveLock);

    address
}

/*
 * A subroutine of ATExecAlterConstraint that calls the respective routines for
 * altering constraint's enforceability, deferrability or inheritability.
 */
unsafe fn at_exec_alter_constraint_internal(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    lockmode: LOCKMODE,
) -> bool {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let mut changed = false;
    let mut otherrelids: *mut List = std::ptr::null_mut();

    /*
     * Note that even if deferrability is requested to be altered along with
     * enforceability, we don't need to explicitly update multiple entries in
     * pg_trigger related to deferrability.
     */
    if (*cmdcon).alterEnforceability
        && at_exec_alter_constr_enforceability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            (*currcon).conrelid,
            (*currcon).confrelid,
            contuple,
            lockmode,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
        )
    {
        changed = true;
    } else if (*cmdcon).alterDeferrability
        && at_exec_alter_constr_deferrability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            rel,
            contuple,
            recurse,
            &mut otherrelids,
            lockmode,
        )
    {
        /*
         * AlterConstrUpdateConstraintEntry already invalidated relcache for
         * the relations having the constraint itself; here we also invalidate
         * for relations that have any triggers that are part of the constraint.
         */
        let mut lc = list_head(otherrelids);
        while !lc.is_null() {
            CacheInvalidateRelcacheByRelid(lfirst_oid(lc));
            lc = lnext(otherrelids, lc);
        }
        changed = true;
    }

    /* Do the catalog work for the inheritability change. */
    if (*cmdcon).alterInheritability
        && at_exec_alter_constr_inheritability(wqueue, cmdcon, conrel, rel, contuple, lockmode)
    {
        changed = true;
    }

    changed
}

/*
 * Returns true if the constraint's enforceability is altered.
 */
unsafe fn at_exec_alter_constr_enforceability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    fkrelid: Oid,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> bool {
    check_stack_depth();
    Assert!((*cmdcon).alterEnforceability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    Assert!((*currcon).contype == CONSTRAINT_FOREIGN as i8);

    let rel = table_open((*currcon).conrelid, lockmode);
    let mut changed = false;

    if (*currcon).conenforced != (*cmdcon).is_enforced {
        alter_constr_update_constraint_entry(cmdcon, conrel, contuple);
        changed = true;
    }

    /* Drop triggers */
    if !(*cmdcon).is_enforced {
        /*
         * When setting a constraint to NOT ENFORCED, process child relations first,
         * then the parent.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind((*currcon).confrelid) == RELKIND_PARTITIONED_TABLE as i8
        {
            alter_constr_enforceability_recurse(
                wqueue,
                cmdcon,
                conrel,
                tgrel,
                fkrelid,
                pkrelid,
                contuple,
                lockmode,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
            );
        }
        /* Drop all the triggers */
        drop_foreign_key_constraint_triggers(tgrel, conoid, InvalidOid, InvalidOid);
    } else if changed {
        /* Create triggers */
        let mut referenced_del_trigger_oid = InvalidOid;
        let mut referenced_upd_trigger_oid = InvalidOid;
        let mut referencing_ins_trigger_oid = InvalidOid;
        let mut referencing_upd_trigger_oid = InvalidOid;

        /* Prepare the minimal information required for trigger creation. */
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        (*fkconstraint).conname = pstrdup(NameStr!((*currcon).conname) as *mut i8);
        (*fkconstraint).fk_matchtype = (*currcon).confmatchtype;
        (*fkconstraint).fk_upd_action = (*currcon).confupdtype;
        (*fkconstraint).fk_del_action = (*currcon).confdeltype;

        /* Create referenced triggers */
        if (*currcon).conrelid == fkrelid {
            create_foreign_key_action_triggers(
                (*currcon).conrelid,
                (*currcon).confrelid,
                fkconstraint,
                conoid,
                (*currcon).conindid,
                referenced_parent_del_trigger,
                referenced_parent_upd_trigger,
                &mut referenced_del_trigger_oid,
                &mut referenced_upd_trigger_oid,
            );
        }

        /* Create referencing triggers */
        if (*currcon).confrelid == pkrelid {
            create_foreign_key_check_triggers(
                (*currcon).conrelid,
                pkrelid,
                fkconstraint,
                conoid,
                (*currcon).conindid,
                referencing_parent_ins_trigger,
                referencing_parent_upd_trigger,
                &mut referencing_ins_trigger_oid,
                &mut referencing_upd_trigger_oid,
            );
        }

        /*
         * Tell Phase 3 to check that the constraint is satisfied by existing rows.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_RELATION as i8 && (*currcon).confrelid == pkrelid {
            let tab = ATGetQueueEntry(wqueue, rel);
            let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
            (*newcon).name = (*fkconstraint).conname;
            (*newcon).contype = CONSTR_FOREIGN;
            (*newcon).refrelid = (*currcon).confrelid;
            (*newcon).refindid = (*currcon).conindid;
            (*newcon).conid = (*currcon).oid;
            (*newcon).qual = fkconstraint as *mut Node;
            (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
        }

        /*
         * If the table at either end of the constraint is partitioned, we need to
         * recurse and create triggers for each constraint that is a child.
         */
        if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind((*currcon).confrelid) == RELKIND_PARTITIONED_TABLE as i8
        {
            alter_constr_enforceability_recurse(
                wqueue,
                cmdcon,
                conrel,
                tgrel,
                fkrelid,
                pkrelid,
                contuple,
                lockmode,
                referenced_del_trigger_oid,
                referenced_upd_trigger_oid,
                referencing_ins_trigger_oid,
                referencing_upd_trigger_oid,
            );
        }
    }

    table_close(rel, NoLock);
    changed
}

/*
 * Returns true if the constraint's deferrability is altered.
 */
unsafe fn at_exec_alter_constr_deferrability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    otherrelids: *mut *mut List,
    lockmode: LOCKMODE,
) -> bool {
    check_stack_depth();
    Assert!((*cmdcon).alterDeferrability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let refrelid = (*currcon).confrelid;
    let mut changed = false;

    /* Should be foreign key constraint */
    Assert!((*currcon).contype == CONSTRAINT_FOREIGN as i8);

    if (*currcon).condeferrable != (*cmdcon).deferrable
        || (*currcon).condeferred != (*cmdcon).initdeferred
    {
        alter_constr_update_constraint_entry(cmdcon, conrel, contuple);
        changed = true;

        /* Update the triggers that implement the constraint */
        alter_constr_trigger_deferrability(
            (*currcon).oid,
            tgrel,
            rel,
            (*cmdcon).deferrable,
            (*cmdcon).initdeferred,
            otherrelids,
        );
    }

    /*
     * If the table at either end of the constraint is partitioned, handle
     * every constraint that is a child of this one.
     */
    if recurse
        && changed
        && ((*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
            || get_rel_relkind(refrelid) == RELKIND_PARTITIONED_TABLE as i8)
    {
        alter_constr_deferrability_recurse(wqueue, cmdcon, conrel, tgrel, rel, contuple, recurse, otherrelids, lockmode);
    }

    changed
}

/*
 * Returns true if the constraint's inheritability is altered.
 */
unsafe fn at_exec_alter_constr_inheritability(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
) -> bool {
    Assert!((*cmdcon).alterInheritability);

    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;

    /* The current implementation only works for NOT NULL constraints */
    Assert!((*currcon).contype == CONSTRAINT_NOTNULL as i8);

    /* If already in desired state, silently do nothing. */
    if (*cmdcon).noinherit == (*currcon).connoinherit {
        return false;
    }

    alter_constr_update_constraint_entry(cmdcon, conrel, contuple);
    CommandCounterIncrement();

    /* Fetch the column number and name */
    let col_num = extractNotNullColumn(contuple);
    let col_name = get_attname((*currcon).conrelid, col_num, false);

    /* Propagate the change to children. */
    let children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);

        if (*cmdcon).noinherit {
            let child_tup = findNotNullConstraint(childoid, col_name);
            if child_tup.is_null() {
                elog!(
                    ERROR,
                    "cache lookup failed for not-null constraint on column \"{}\" of relation {}",
                    std::ffi::CStr::from_ptr(col_name).to_string_lossy(),
                    childoid
                );
            }
            let childcon = GETSTRUCT(child_tup) as Form_pg_constraint;
            Assert!((*childcon).coninhcount > 0);
            (*childcon).coninhcount -= 1;
            (*childcon).conislocal = true;
            CatalogTupleUpdate(conrel, &mut (*child_tup).t_self, child_tup);
            heap_freetuple(child_tup);
        } else {
            let childrel = table_open(childoid, NoLock);
            let addr = at_exec_set_not_null(
                wqueue,
                childrel,
                NameStr!((*currcon).conname) as *mut i8,
                col_name,
                true,
                true,
                lockmode,
            );
            if OidIsValid(addr.objectId) {
                CommandCounterIncrement();
            }
            table_close(childrel, NoLock);
        }
        child_lc = lnext(children, child_lc);
    }

    true
}

/*
 * AlterConstrTriggerDeferrability
 *   Update constraint trigger deferrability for the given constraint.
 */
unsafe fn alter_constr_trigger_deferrability(
    conoid: Oid,
    tgrel: Relation,
    rel: Relation,
    deferrable: bool,
    initdeferred: bool,
    otherrelids: *mut *mut List,
) {
    let mut tgtuple: HeapTuple;
    let mut tgkey = ScanKeyData::default();
    let tgscan: SysScanDesc;

    ScanKeyInit(
        &mut tgkey,
        Anum_pg_trigger_tgconstraint,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true, std::ptr::null_mut(), 1, &mut tgkey);
    loop {
        tgtuple = systable_getnext(tgscan);
        if !HeapTupleIsValid(tgtuple) { break; }
        let tgform = GETSTRUCT(tgtuple) as Form_pg_trigger;

        /*
         * Remember OIDs of other relation(s) involved in FK constraint.
         */
        if (*tgform).tgrelid != RelationGetRelid(rel) {
            *otherrelids = list_append_unique_oid(*otherrelids, (*tgform).tgrelid);
        }

        /*
         * Update enable status and deferrability of RI_FKey_noaction_del,
         * RI_FKey_noaction_upd, RI_FKey_check_ins and RI_FKey_check_upd
         * triggers, but not others.
         */
        if (*tgform).tgfoid != F_RI_FKEY_NOACTION_DEL
            && (*tgform).tgfoid != F_RI_FKEY_NOACTION_UPD
            && (*tgform).tgfoid != F_RI_FKEY_CHECK_INS
            && (*tgform).tgfoid != F_RI_FKEY_CHECK_UPD
        {
            continue;
        }

        let tg_copy_tuple = heap_copytuple(tgtuple);
        let copy_tg = GETSTRUCT(tg_copy_tuple) as Form_pg_trigger;
        (*copy_tg).tgdeferrable = deferrable;
        (*copy_tg).tginitdeferred = initdeferred;
        CatalogTupleUpdate(tgrel, &mut (*tg_copy_tuple).t_self, tg_copy_tuple);
        InvokeObjectPostAlterHook(TriggerRelationId, (*tgform).oid, 0);
        heap_freetuple(tg_copy_tuple);
    }

    systable_endscan(tgscan);
}

/*
 * Invokes at_exec_alter_constr_enforceability for each child constraint.
 */
unsafe fn alter_constr_enforceability_recurse(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    fkrelid: Oid,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    let mut pkey = ScanKeyData::default();
    let pscan: SysScanDesc;
    let mut childtup: HeapTuple;

    ScanKeyInit(
        &mut pkey,
        Anum_pg_constraint_conparentid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
    loop {
        childtup = systable_getnext(pscan);
        if !HeapTupleIsValid(childtup) { break; }
        at_exec_alter_constr_enforceability(
            wqueue,
            cmdcon,
            conrel,
            tgrel,
            fkrelid,
            pkrelid,
            childtup,
            lockmode,
            referenced_parent_del_trigger,
            referenced_parent_upd_trigger,
            referencing_parent_ins_trigger,
            referencing_parent_upd_trigger,
        );
    }
    systable_endscan(pscan);
}

/*
 * Invokes at_exec_alter_constr_deferrability for each child constraint.
 */
unsafe fn alter_constr_deferrability_recurse(
    wqueue: *mut *mut List,
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    tgrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    otherrelids: *mut *mut List,
    lockmode: LOCKMODE,
) {
    let currcon = GETSTRUCT(contuple) as Form_pg_constraint;
    let conoid = (*currcon).oid;
    let mut pkey = ScanKeyData::default();
    let pscan: SysScanDesc;
    let mut childtup: HeapTuple;

    ScanKeyInit(
        &mut pkey,
        Anum_pg_constraint_conparentid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(conoid),
    );
    pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
    loop {
        childtup = systable_getnext(pscan);
        if !HeapTupleIsValid(childtup) { break; }
        let childcon = GETSTRUCT(childtup) as Form_pg_constraint;
        let childrel = table_open((*childcon).conrelid, lockmode);
        at_exec_alter_constr_deferrability(
            wqueue, cmdcon, conrel, tgrel, childrel, childtup, recurse, otherrelids, lockmode,
        );
        table_close(childrel, NoLock);
    }
    systable_endscan(pscan);
}

/*
 * Update the constraint entry for the given ATAlterConstraint command.
 */
unsafe fn alter_constr_update_constraint_entry(
    cmdcon: *mut ATAlterConstraint,
    conrel: Relation,
    contuple: HeapTuple,
) {
    Assert!((*cmdcon).alterEnforceability || (*cmdcon).alterDeferrability || (*cmdcon).alterInheritability);

    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;

    if (*cmdcon).alterEnforceability {
        (*copy_con).conenforced = (*cmdcon).is_enforced;
        (*copy_con).convalidated = (*cmdcon).is_enforced;
    }
    if (*cmdcon).alterDeferrability {
        (*copy_con).condeferrable = (*cmdcon).deferrable;
        (*copy_con).condeferred = (*cmdcon).initdeferred;
    }
    if (*cmdcon).alterInheritability {
        (*copy_con).connoinherit = (*cmdcon).noinherit;
    }

    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*copy_con).oid, 0);

    /* Make new constraint flags visible to others */
    CacheInvalidateRelcacheByRelid((*copy_con).conrelid);

    heap_freetuple(copy_tuple);
}

/*
 * ATExecValidateConstraint
 *   ALTER TABLE VALIDATE CONSTRAINT
 *   Return value is the address of the validated constraint.
 *   If the constraint was already validated, InvalidObjectAddress is returned.
 */
unsafe fn at_exec_validate_constraint(
    wqueue: *mut *mut List,
    rel: Relation,
    constr_name: *mut i8,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let scan: SysScanDesc;
    let mut skey = [ScanKeyData::default(); 3];
    let mut tuple: HeapTuple;
    let con: Form_pg_constraint;
    let mut address = InvalidObjectAddress;

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Find and check the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(constr_name),
    );
    scan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true, std::ptr::null_mut(), 3, skey.as_mut_ptr());

    /* There can be at most one matching row */
    tuple = systable_getnext(scan);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "constraint \"{}\" of relation \"{}\" does not exist",
                std::ffi::CStr::from_ptr(constr_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    con = GETSTRUCT(tuple) as Form_pg_constraint;
    if (*con).contype != CONSTRAINT_FOREIGN as i8
        && (*con).contype != CONSTRAINT_CHECK as i8
        && (*con).contype != CONSTRAINT_NOTNULL as i8
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg!(
                "cannot validate constraint \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(constr_name).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
            /* errdetail: This operation is not supported for this type of constraint. */
        );
    }

    if !(*con).conenforced {
        ereport!(
            ERROR,
            errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!("cannot validate NOT ENFORCED constraint")
        );
    }

    if !(*con).convalidated {
        if (*con).contype == CONSTRAINT_FOREIGN as i8 {
            queue_fk_constraint_validation(wqueue, conrel, rel, (*con).confrelid, tuple, lockmode);
        } else if (*con).contype == CONSTRAINT_CHECK as i8 {
            queue_check_constraint_validation(
                wqueue, conrel, rel, constr_name, tuple, recurse, recursing, lockmode,
            );
        } else if (*con).contype == CONSTRAINT_NOTNULL as i8 {
            queue_nn_constraint_validation(wqueue, conrel, rel, tuple, recurse, recursing, lockmode);
        }

        ObjectAddressSet!(address, ConstraintRelationId, (*con).oid);
    } else {
        address = InvalidObjectAddress; /* already validated */
    }

    systable_endscan(scan);
    table_close(conrel, RowExclusiveLock);

    address
}

/*
 * QueueFKConstraintValidation
 *   Add an entry to wqueue to validate the given FK constraint in Phase 3.
 */
unsafe fn queue_fk_constraint_validation(
    wqueue: *mut *mut List,
    conrel: Relation,
    fkrel: Relation,
    pkrelid: Oid,
    contuple: HeapTuple,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_FOREIGN as i8);
    Assert!(!(*con).convalidated);

    /*
     * Add the validation to phase 3's queue; not needed for partitioned
     * tables themselves, only for their partitions.
     */
    if (*(*fkrel).rd_rel).relkind == RELKIND_RELATION as i8 && (*con).confrelid == pkrelid {
        let fkconstraint = makeNode!(Constraint, T_Constraint) as *mut Constraint;
        /* for now this is all we need */
        (*fkconstraint).conname = pstrdup(NameStr!((*con).conname) as *mut i8);

        let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
        (*newcon).name = (*fkconstraint).conname;
        (*newcon).contype = CONSTR_FOREIGN;
        (*newcon).refrelid = (*con).confrelid;
        (*newcon).refindid = (*con).conindid;
        (*newcon).conid = (*con).oid;
        (*newcon).qual = fkconstraint as *mut Node;

        let tab = ATGetQueueEntry(wqueue, fkrel);
        (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);
    }

    /*
     * If the table at either end of the constraint is partitioned, recurse
     * to handle every unvalidated constraint that is a child.
     */
    if (*(*fkrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
        || get_rel_relkind((*con).confrelid) == RELKIND_PARTITIONED_TABLE as i8
    {
        let mut pkey = ScanKeyData::default();
        let pscan: SysScanDesc;
        let mut childtup: HeapTuple;

        ScanKeyInit(
            &mut pkey,
            Anum_pg_constraint_conparentid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum((*con).oid),
        );
        pscan = systable_beginscan(conrel, ConstraintParentIndexId, true, std::ptr::null_mut(), 1, &mut pkey);
        loop {
            childtup = systable_getnext(pscan);
            if !HeapTupleIsValid(childtup) { break; }
            let childcon = GETSTRUCT(childtup) as Form_pg_constraint;

            /* If the child constraint has already been validated, skip it. */
            if (*childcon).convalidated { continue; }

            let childrel = table_open((*childcon).conrelid, lockmode);
            /*
             * pkrelid should be passed as-is during recursion to identify the root referenced table.
             */
            queue_fk_constraint_validation(wqueue, conrel, childrel, pkrelid, childtup, lockmode);
            table_close(childrel, NoLock);
        }
        systable_endscan(pscan);
    }

    /*
     * Now mark the pg_constraint row as validated.
     */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * QueueCheckConstraintValidation
 *   Add an entry to wqueue to validate the given check constraint in Phase 3.
 */
unsafe fn queue_check_constraint_validation(
    wqueue: *mut *mut List,
    conrel: Relation,
    rel: Relation,
    constr_name: *mut i8,
    contuple: HeapTuple,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_CHECK as i8);

    let mut children: *mut List = std::ptr::null_mut();

    /*
     * If we're recursing, the parent has already done this.
     */
    if !recursing && !(*con).connoinherit {
        children = find_all_inheritors(RelationGetRelid(rel), lockmode, std::ptr::null_mut());
    }

    /*
     * We recurse before validating on the parent, to reduce risk of deadlocks.
     */
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);
        if childoid == RelationGetRelid(rel) {
            child_lc = lnext(children, child_lc);
            continue;
        }

        if !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be validated on child tables too")
            );
        }

        /* find_all_inheritors already got lock */
        let childrel = table_open(childoid, NoLock);
        at_exec_validate_constraint(wqueue, childrel, constr_name, false, true, lockmode);
        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    /* Queue validation for phase 3 */
    let newcon = palloc0(core::mem::size_of::<NewConstraint>()) as *mut NewConstraint;
    (*newcon).name = constr_name;
    (*newcon).contype = CONSTR_CHECK;
    (*newcon).refrelid = InvalidOid;
    (*newcon).refindid = InvalidOid;
    (*newcon).conid = (*con).oid;

    let val = SysCacheGetAttrNotNull(CONSTROID, contuple, Anum_pg_constraint_conbin);
    let conbin = TextDatumGetCString(val);
    (*newcon).qual = expand_generated_columns_in_expr(stringToNode(conbin), rel, 1);

    let tab = ATGetQueueEntry(wqueue, rel);
    (*tab).constraints = lappend((*tab).constraints, newcon as *mut _);

    /* Invalidate relcache */
    CacheInvalidateRelcache(rel);

    /* Update catalog */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * QueueNNConstraintValidation
 *   Add an entry to wqueue to validate the given not-null constraint in Phase 3.
 */
unsafe fn queue_nn_constraint_validation(
    wqueue: *mut *mut List,
    conrel: Relation,
    rel: Relation,
    contuple: HeapTuple,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) {
    let con = GETSTRUCT(contuple) as Form_pg_constraint;
    Assert!((*con).contype == CONSTRAINT_NOTNULL as i8);

    let attnum = extractNotNullColumn(contuple);
    let mut children: *mut List = std::ptr::null_mut();

    if !recursing && !(*con).connoinherit {
        children = find_all_inheritors(RelationGetRelid(rel), lockmode, std::ptr::null_mut());
    }

    let colname = get_attname(RelationGetRelid(rel), attnum, false);
    let mut child_lc = list_head(children);
    while !child_lc.is_null() {
        let childoid = lfirst_oid(child_lc);
        if childoid == RelationGetRelid(rel) {
            child_lc = lnext(children, child_lc);
            continue;
        }

        if !recurse {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg!("constraint must be validated on child tables too")
            );
        }

        /* The column on child might have a different attnum, search by column name. */
        let contup = findNotNullConstraint(childoid, colname);
        if contup.is_null() {
            elog!(
                ERROR,
                "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_rel_name(childoid)).to_string_lossy()
            );
        }
        let childcon = GETSTRUCT(contup) as Form_pg_constraint;
        if (*childcon).convalidated {
            child_lc = lnext(children, child_lc);
            continue;
        }

        /* find_all_inheritors already got lock */
        let childrel = table_open(childoid, NoLock);
        let conname = pstrdup(NameStr!((*childcon).conname) as *mut i8);
        /* XXX improve at_exec_validate_constraint API to avoid double search */
        at_exec_validate_constraint(wqueue, childrel, conname, false, true, lockmode);
        table_close(childrel, NoLock);
        child_lc = lnext(children, child_lc);
    }

    /* Set attnotnull appropriately without queueing another validation */
    set_attnotnull(std::ptr::null_mut(), rel, attnum, true, false);

    let tab = ATGetQueueEntry(wqueue, rel);
    (*tab).verify_new_notnull = true;

    /* Invalidate relcache */
    CacheInvalidateRelcache(rel);

    /* Update catalogs */
    let copy_tuple = heap_copytuple(contuple);
    let copy_con = GETSTRUCT(copy_tuple) as Form_pg_constraint;
    (*copy_con).convalidated = true;
    CatalogTupleUpdate(conrel, &mut (*copy_tuple).t_self, copy_tuple);
    InvokeObjectPostAlterHook(ConstraintRelationId, (*con).oid, 0);
    heap_freetuple(copy_tuple);
}

/*
 * transformColumnNameList - transform list of column names
 *   Lookup each name and return its attnum and, optionally, type and collation OIDs.
 */
unsafe fn transform_column_name_list(
    rel_id: Oid,
    col_list: *mut List,
    attnums: *mut i16,
    atttypids: *mut Oid,
    attcollids: *mut Oid,
) -> i32 {
    let mut attnum: i32 = 0;
    let mut lc = list_head(col_list);
    while !lc.is_null() {
        let attname = strVal(lfirst(lc)) as *mut i8;
        let atttuple = SearchSysCacheAttName(rel_id, attname);
        if !HeapTupleIsValid(atttuple) {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg!(
                    "column \"{}\" referenced in foreign key constraint does not exist",
                    std::ffi::CStr::from_ptr(attname).to_string_lossy()
                )
            );
        }
        let attform = GETSTRUCT(atttuple) as Form_pg_attribute;
        if (*attform).attnum < 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg!("system columns cannot be used in foreign keys")
            );
        }
        if attnum >= INDEX_MAX_KEYS as i32 {
            ereport!(
                ERROR,
                errcode(ERRCODE_TOO_MANY_COLUMNS),
                errmsg!("cannot have more than {} keys in a foreign key", INDEX_MAX_KEYS)
            );
        }
        *attnums.add(attnum as usize) = (*attform).attnum;
        if !atttypids.is_null() {
            *atttypids.add(attnum as usize) = (*attform).atttypid;
        }
        if !attcollids.is_null() {
            *attcollids.add(attnum as usize) = (*attform).attcollation;
        }
        ReleaseSysCache(atttuple);
        attnum += 1;
        lc = lnext(col_list, lc);
    }
    attnum
}

/*
 * transformFkeyGetPrimaryKey -
 *   Look up the names, attnums, types, and collations of the primary key
 *   attributes for the pkrel.
 */
unsafe fn transform_fkey_get_primary_key(
    pkrel: Relation,
    index_oid: *mut Oid,
    attnamelist: *mut *mut List,
    attnums: *mut i16,
    atttypids: *mut Oid,
    attcollids: *mut Oid,
    opclasses: *mut Oid,
    pk_has_without_overlaps: *mut bool,
) -> i32 {
    let mut index_tuple: HeapTuple = std::ptr::null_mut();
    let mut index_struct: Form_pg_index = std::ptr::null_mut();

    *index_oid = InvalidOid;

    let indexoidlist = RelationGetIndexList(pkrel);
    let mut scan_lc = list_head(indexoidlist);
    while !scan_lc.is_null() {
        let indexoid = lfirst_oid(scan_lc);
        index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if !HeapTupleIsValid(index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexoid);
        }
        index_struct = GETSTRUCT(index_tuple) as Form_pg_index;
        if (*index_struct).indisprimary && (*index_struct).indisvalid {
            if !(*index_struct).indimmediate {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg!(
                        "cannot use a deferrable primary key for referenced table \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                    )
                );
            }
            *index_oid = indexoid;
            break;
        }
        ReleaseSysCache(index_tuple);
        index_tuple = std::ptr::null_mut();
        scan_lc = lnext(indexoidlist, scan_lc);
    }

    list_free(indexoidlist);

    if !OidIsValid(*index_oid) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg!(
                "there is no primary key for referenced table \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
            )
        );
    }

    /* Must get indclass the hard way */
    let indclass_datum = SysCacheGetAttrNotNull(INDEXRELID, index_tuple, Anum_pg_index_indclass);
    let indclass = DatumGetPointer(indclass_datum) as *mut oidvector;

    /* Build the list of PK attributes from indkey */
    *attnamelist = std::ptr::null_mut();
    let mut i = 0;
    while i < (*index_struct).indnkeyatts as usize {
        let pkattno = (*index_struct).indkey.values[i];
        *attnums.add(i) = pkattno as i16;
        *atttypids.add(i) = attnumTypeId(pkrel, pkattno as i32);
        *attcollids.add(i) = attnumCollationId(pkrel, pkattno as i32);
        *opclasses.add(i) = (*indclass).values[i];
        *attnamelist = lappend(
            *attnamelist,
            makeString(pstrdup(NameStr!(*attnumAttName(pkrel, pkattno as i32)) as *mut i8)) as *mut _,
        );
        i += 1;
    }

    *pk_has_without_overlaps = (*index_struct).indisexclusion;
    ReleaseSysCache(index_tuple);

    i as i32
}

/*
 * transformFkeyCheckAttrs -
 *   Validate that the 'attnums' columns in the 'pkrel' relation are valid to
 *   reference as part of a foreign key constraint.
 */
unsafe fn transform_fkey_check_attrs(
    pkrel: Relation,
    numattrs: i32,
    attnums: *mut i16,
    with_period: bool,
    opclasses: *mut Oid,
    pk_has_without_overlaps: *mut bool,
) -> Oid {
    let mut indexoid = InvalidOid;
    let mut found = false;
    let mut found_deferrable = false;

    /* Reject duplicate appearances of columns */
    for i in 0..numattrs as usize {
        for j in (i + 1)..numattrs as usize {
            if *attnums.add(i) == *attnums.add(j) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_FOREIGN_KEY),
                    errmsg!("foreign key referenced-columns list must not contain duplicates")
                );
            }
        }
    }

    let indexoidlist = RelationGetIndexList(pkrel);
    let mut scan_lc = list_head(indexoidlist);
    while !scan_lc.is_null() {
        let index_tuple: HeapTuple;
        let index_struct: Form_pg_index;

        indexoid = lfirst_oid(scan_lc);
        index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
        if !HeapTupleIsValid(index_tuple) {
            elog!(ERROR, "cache lookup failed for index {}", indexoid);
        }
        index_struct = GETSTRUCT(index_tuple) as Form_pg_index;

        /*
         * Must have the right number of columns; must be unique (or exclusion for temporal)
         * and not a partial index; forget it if there are any expressions.
         */
        if (*index_struct).indnkeyatts == numattrs as i16
            && (if with_period { (*index_struct).indisexclusion } else { (*index_struct).indisunique })
            && (*index_struct).indisvalid
            && heap_attisnull(index_tuple, Anum_pg_index_indpred, std::ptr::null_mut())
            && heap_attisnull(index_tuple, Anum_pg_index_indexprs, std::ptr::null_mut())
        {
            let indclass_datum = SysCacheGetAttrNotNull(INDEXRELID, index_tuple, Anum_pg_index_indclass);
            let indclass = DatumGetPointer(indclass_datum) as *mut oidvector;

            /* Check for a match (columns may appear in different order) */
            'outer: {
                for i in 0..numattrs as usize {
                    found = false;
                    for j in 0..numattrs as usize {
                        if *attnums.add(i) == (*index_struct).indkey.values[j] as i16 {
                            *opclasses.add(i) = (*indclass).values[j];
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        break 'outer;
                    }
                }
                /* The last attribute must be the PERIOD FK part for temporal FKs */
                if found && with_period {
                    let period_attnum = *attnums.add(numattrs as usize - 1);
                    found = period_attnum == (*index_struct).indkey.values[numattrs as usize - 1] as i16;
                }
                /* Refuse deferrable unique/primary key */
                if found && !(*index_struct).indimmediate {
                    found_deferrable = true;
                    found = false;
                }
                /* Record whether index has WITHOUT OVERLAPS */
                if found {
                    *pk_has_without_overlaps = (*index_struct).indisexclusion;
                }
            }
        }
        ReleaseSysCache(index_tuple);
        if found { break; }
        scan_lc = lnext(indexoidlist, scan_lc);
    }

    if !found {
        if found_deferrable {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!(
                    "cannot use a deferrable unique constraint for referenced table \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                )
            );
        } else {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_FOREIGN_KEY),
                errmsg!(
                    "there is no unique constraint matching given keys for referenced table \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName(pkrel)).to_string_lossy()
                )
            );
        }
    }

    list_free(indexoidlist);
    indexoid
}

/*
 * findFkeyCast -
 *   Wrapper around find_coercion_pathway() for ATAddForeignKeyConstraint().
 */
unsafe fn find_fkey_cast(target_type_id: Oid, source_type_id: Oid, funcid: *mut Oid) -> CoercionPathType {
    let ret: CoercionPathType;
    if target_type_id == source_type_id {
        ret = COERCION_PATH_RELABELTYPE;
        *funcid = InvalidOid;
    } else {
        ret = find_coercion_pathway(target_type_id, source_type_id, COERCION_IMPLICIT, funcid);
        if ret == COERCION_PATH_NONE {
            /* A previously-relied-upon cast is now gone. */
            elog!(ERROR, "could not find cast from {} to {}", source_type_id, target_type_id);
        }
    }
    ret
}

/*
 * checkFkeyPermissions
 *   Permissions checks on the referenced table for ADD FOREIGN KEY.
 */
unsafe fn check_fkey_permissions(rel: Relation, attnums: *mut i16, natts: i32) {
    let roleid = GetUserId();
    let aclresult = pg_class_aclcheck(RelationGetRelid(rel), roleid, ACL_REFERENCES);
    if aclresult == ACLCHECK_OK {
        return;
    }
    /* Else we must have REFERENCES on each column */
    for i in 0..natts as usize {
        let aclresult = pg_attribute_aclcheck(RelationGetRelid(rel), *attnums.add(i), roleid, ACL_REFERENCES);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, get_relkind_objtype((*(*rel).rd_rel).relkind), RelationGetRelationName(rel));
        }
    }
}

/*
 * validateForeignKeyConstraint
 *   Scan the existing rows in a table to verify they meet a proposed FK constraint.
 */
unsafe fn validate_foreign_key_constraint(
    conname: *mut i8,
    rel: Relation,
    pkrel: Relation,
    pkind_oid: Oid,
    constraint_oid: Oid,
    hasperiod: bool,
) {
    let mut slot: *mut TupleTableSlot;
    let scan: TableScanDesc;
    let mut trig: Trigger = core::mem::zeroed();
    let snapshot: Snapshot;
    let oldcxt: MemoryContext;
    let per_tup_cxt: MemoryContext;

    ereport!(
        DEBUG1,
        errmsg_internal!("validating foreign key constraint \"{}\"", std::ffi::CStr::from_ptr(conname).to_string_lossy())
    );

    /* Build a trigger call structure */
    trig.tgoid = InvalidOid;
    trig.tgname = conname;
    trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
    trig.tgisinternal = true;
    trig.tgconstrrelid = RelationGetRelid(pkrel);
    trig.tgconstrindid = pkind_oid;
    trig.tgconstraint = constraint_oid;
    trig.tgdeferrable = false;
    trig.tginitdeferred = false;
    /* we needn't fill in remaining fields */

    /*
     * See if we can do it with a single LEFT JOIN query.
     */
    if !hasperiod && RI_Initial_Check(&mut trig, rel, pkrel) {
        return;
    }

    /*
     * Scan through each tuple, calling RI_FKey_check_ins as if it had just been inserted.
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    slot = table_slot_create(rel, std::ptr::null_mut());
    scan = table_beginscan(rel, snapshot, 0, std::ptr::null_mut());

    per_tup_cxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"validateForeignKeyConstraint\0".as_ptr() as *const i8,
        ALLOCSET_SMALL_SIZES,
    );
    oldcxt = MemoryContextSwitchTo(per_tup_cxt);

    while table_scan_getnextslot(scan, ForwardScanDirection, slot) {
        let fcinfo = LOCAL_FCINFO!(0);
        let mut trigdata: TriggerData = core::mem::zeroed();

        CHECK_FOR_INTERRUPTS!();

        /* Make a call to the trigger function. No parameters are passed. */
        core::ptr::write_bytes(fcinfo, 0, SizeForFunctionCallInfo(0));

        /* We assume RI_FKey_check_ins won't look at flinfo... */
        trigdata.r#type = T_TriggerData;
        trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
        trigdata.tg_relation = rel;
        trigdata.tg_trigtuple = ExecFetchSlotHeapTuple(slot, false, std::ptr::null_mut());
        trigdata.tg_trigslot = slot;
        trigdata.tg_trigger = &mut trig;

        (*fcinfo).context = &mut trigdata as *mut TriggerData as *mut Node;

        RI_FKey_check_ins(fcinfo);

        MemoryContextReset(per_tup_cxt);
    }

    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(per_tup_cxt);
    table_endscan(scan);
    UnregisterSnapshot(snapshot);
    ExecDropSingleTupleTableSlot(slot);
}

/*
 * CreateFKCheckTrigger
 *   Creates the insert (on_insert=true) or update "check" trigger that
 *   implements a given foreign key. Returns the OID of the created trigger.
 */
unsafe fn create_fk_check_trigger(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_trig_oid: Oid,
    on_insert: bool,
) -> Oid {
    let trig_address: ObjectAddress;
    let fk_trigger = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;

    /*
     * Note: for a self-referential FK, action triggers fire before check triggers,
     * using names RI_ConstraintTrigger_a_NNNN and RI_ConstraintTrigger_c_NNNN.
     */
    (*fk_trigger).replace = false;
    (*fk_trigger).isconstraint = true;
    (*fk_trigger).trigname = b"RI_ConstraintTrigger_c\0".as_ptr() as *mut i8;
    (*fk_trigger).relation = std::ptr::null_mut();

    /* Either ON INSERT or ON UPDATE */
    if on_insert {
        (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_check_ins\0".as_ptr() as *mut i8);
        (*fk_trigger).events = TRIGGER_TYPE_INSERT;
    } else {
        (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_check_upd\0".as_ptr() as *mut i8);
        (*fk_trigger).events = TRIGGER_TYPE_UPDATE;
    }

    (*fk_trigger).args = std::ptr::null_mut();
    (*fk_trigger).row = true;
    (*fk_trigger).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger).columns = std::ptr::null_mut();
    (*fk_trigger).whenClause = std::ptr::null_mut();
    (*fk_trigger).transitionRels = std::ptr::null_mut();
    (*fk_trigger).deferrable = (*fkconstraint).deferrable;
    (*fk_trigger).initdeferred = (*fkconstraint).initdeferred;
    (*fk_trigger).constrrel = std::ptr::null_mut();

    trig_address = CreateTrigger(
        fk_trigger,
        std::ptr::null_mut(),
        my_rel_oid,
        ref_rel_oid,
        constraint_oid,
        index_oid,
        InvalidOid,
        parent_trig_oid,
        std::ptr::null_mut(),
        true,
        false,
    );

    /* Make changes-so-far visible */
    CommandCounterIncrement();

    trig_address.objectId
}

/*
 * createForeignKeyActionTriggers
 *   Create the referenced-side "action" triggers that implement a foreign key.
 *   Returns OIDs in *deleteTrigOid and *updateTrigOid.
 */
unsafe fn create_foreign_key_action_triggers(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_del_trigger: Oid,
    parent_upd_trigger: Oid,
    delete_trig_oid: *mut Oid,
    update_trig_oid: *mut Oid,
) {
    let fk_trigger: *mut CreateTrigStmt;
    let trig_address: ObjectAddress;

    /* Build and execute CREATE CONSTRAINT TRIGGER for ON DELETE action */
    fk_trigger = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
    (*fk_trigger).replace = false;
    (*fk_trigger).isconstraint = true;
    (*fk_trigger).trigname = b"RI_ConstraintTrigger_a\0".as_ptr() as *mut i8;
    (*fk_trigger).relation = std::ptr::null_mut();
    (*fk_trigger).args = std::ptr::null_mut();
    (*fk_trigger).row = true;
    (*fk_trigger).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger).events = TRIGGER_TYPE_DELETE;
    (*fk_trigger).columns = std::ptr::null_mut();
    (*fk_trigger).whenClause = std::ptr::null_mut();
    (*fk_trigger).transitionRels = std::ptr::null_mut();
    (*fk_trigger).constrrel = std::ptr::null_mut();

    match (*fkconstraint).fk_del_action as i32 {
        FKCONSTR_ACTION_NOACTION => {
            (*fk_trigger).deferrable = (*fkconstraint).deferrable;
            (*fk_trigger).initdeferred = (*fkconstraint).initdeferred;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_noaction_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_RESTRICT => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_restrict_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_CASCADE => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_cascade_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETNULL => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_setnull_del\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETDEFAULT => {
            (*fk_trigger).deferrable = false;
            (*fk_trigger).initdeferred = false;
            (*fk_trigger).funcname = SystemFuncName(b"RI_FKey_setdefault_del\0".as_ptr() as *mut i8);
        }
        _ => {
            elog!(ERROR, "unrecognized FK action type: {}", (*fkconstraint).fk_del_action as i32);
        }
    }

    trig_address = CreateTrigger(
        fk_trigger, std::ptr::null_mut(), ref_rel_oid, my_rel_oid,
        constraint_oid, index_oid, InvalidOid,
        parent_del_trigger, std::ptr::null_mut(), true, false,
    );
    if !delete_trig_oid.is_null() {
        *delete_trig_oid = trig_address.objectId;
    }

    /* Make changes-so-far visible */
    CommandCounterIncrement();

    /* Build and execute CREATE CONSTRAINT TRIGGER for ON UPDATE action */
    let fk_trigger2 = makeNode!(CreateTrigStmt, T_CreateTrigStmt) as *mut CreateTrigStmt;
    (*fk_trigger2).replace = false;
    (*fk_trigger2).isconstraint = true;
    (*fk_trigger2).trigname = b"RI_ConstraintTrigger_a\0".as_ptr() as *mut i8;
    (*fk_trigger2).relation = std::ptr::null_mut();
    (*fk_trigger2).args = std::ptr::null_mut();
    (*fk_trigger2).row = true;
    (*fk_trigger2).timing = TRIGGER_TYPE_AFTER;
    (*fk_trigger2).events = TRIGGER_TYPE_UPDATE;
    (*fk_trigger2).columns = std::ptr::null_mut();
    (*fk_trigger2).whenClause = std::ptr::null_mut();
    (*fk_trigger2).transitionRels = std::ptr::null_mut();
    (*fk_trigger2).constrrel = std::ptr::null_mut();

    match (*fkconstraint).fk_upd_action as i32 {
        FKCONSTR_ACTION_NOACTION => {
            (*fk_trigger2).deferrable = (*fkconstraint).deferrable;
            (*fk_trigger2).initdeferred = (*fkconstraint).initdeferred;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_noaction_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_RESTRICT => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_restrict_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_CASCADE => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_cascade_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETNULL => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_setnull_upd\0".as_ptr() as *mut i8);
        }
        FKCONSTR_ACTION_SETDEFAULT => {
            (*fk_trigger2).deferrable = false;
            (*fk_trigger2).initdeferred = false;
            (*fk_trigger2).funcname = SystemFuncName(b"RI_FKey_setdefault_upd\0".as_ptr() as *mut i8);
        }
        _ => {
            elog!(ERROR, "unrecognized FK action type: {}", (*fkconstraint).fk_upd_action as i32);
        }
    }

    let trig_address2 = CreateTrigger(
        fk_trigger2, std::ptr::null_mut(), ref_rel_oid, my_rel_oid,
        constraint_oid, index_oid, InvalidOid,
        parent_upd_trigger, std::ptr::null_mut(), true, false,
    );
    if !update_trig_oid.is_null() {
        *update_trig_oid = trig_address2.objectId;
    }
}

/*
 * createForeignKeyCheckTriggers
 *   Create the referencing-side "check" triggers that implement a foreign key.
 */
unsafe fn create_foreign_key_check_triggers(
    my_rel_oid: Oid,
    ref_rel_oid: Oid,
    fkconstraint: *mut Constraint,
    constraint_oid: Oid,
    index_oid: Oid,
    parent_ins_trigger: Oid,
    parent_upd_trigger: Oid,
    insert_trig_oid: *mut Oid,
    update_trig_oid: *mut Oid,
) {
    *insert_trig_oid = create_fk_check_trigger(
        my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid, parent_ins_trigger, true,
    );
    *update_trig_oid = create_fk_check_trigger(
        my_rel_oid, ref_rel_oid, fkconstraint, constraint_oid, index_oid, parent_upd_trigger, false,
    );
}

/*
 * ALTER TABLE DROP CONSTRAINT
 *
 * Like DROP COLUMN, we can't use the normal ALTER TABLE recursion mechanism.
 */
unsafe fn at_exec_drop_constraint(
    rel: Relation,
    constr_name: *const i8,
    behavior: DropBehavior,
    recurse: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) {
    let conrel: Relation;
    let scan: SysScanDesc;
    let mut skey: [ScanKeyData; 3] = std::mem::zeroed();
    let tuple: *mut HeapTupleData;
    let mut found = false;

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    /* Find and drop the target constraint */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid as i16,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname as i16,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(constr_name as *mut i8),
    );
    scan = systable_beginscan(
        conrel,
        ConstraintRelidTypidNameIndexId,
        true,
        std::ptr::null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    tuple = systable_getnext(scan);
    if HeapTupleIsValid(tuple) {
        dropconstraint_internal(rel, tuple, behavior, recurse, false, missing_ok, lockmode);
        found = true;
    }

    systable_endscan(scan);

    if !found {
        if !missing_ok {
            ereport!(
                ERROR,
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("constraint \"{}\" of relation \"{}\" does not exist", /* C also: constrName, RelationGetRelationName(rel) */
                    CStr::from_ptr(constr_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        } else {
            ereport!(
                NOTICE,
                errmsg("constraint \"{}\" of relation \"{}\" does not exist, skipping",
                    CStr::from_ptr(constr_name).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }
    }

    table_close(conrel, RowExclusiveLock);
}

/*
 * Remove a constraint, using its pg_constraint tuple
 *
 * Implementation for ALTER TABLE DROP CONSTRAINT and ALTER TABLE ALTER COLUMN
 * DROP NOT NULL.
 *
 * Returns the address of the constraint being removed.
 */
unsafe fn dropconstraint_internal(
    rel: Relation,
    constraint_tup: *mut HeapTupleData,
    behavior: DropBehavior,
    recurse: bool,
    recursing: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> ObjectAddress {
    let conrel: Relation;
    let con: Form_pg_constraint;
    let mut conobj: ObjectAddress = std::mem::zeroed();
    let children: *mut List;
    let mut is_no_inherit_constraint = false;
    let constr_name: *mut i8;
    let mut colname: *mut i8 = std::ptr::null_mut();

    /* Guard against stack overflow due to overly deep inheritance tree. */
    check_stack_depth();

    /* At top level, permission check was done in ATPrepCmd, else do it */
    if recursing {
        ATSimplePermissions(
            AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        );
    }

    conrel = table_open(ConstraintRelationId, RowExclusiveLock);

    con = GETSTRUCT(constraint_tup) as Form_pg_constraint;
    constr_name = NameStr((*con).conname) as *mut i8;

    /* Don't allow drop of inherited constraints */
    if (*con).coninhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot drop inherited constraint \"{}\" of relation \"{}\"",
                CStr::from_ptr(constr_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
        );
    }

    /*
     * Reset pg_constraint.attnotnull, if this is a not-null constraint.
     *
     * While doing that, we're in a good position to disallow dropping a not-
     * null constraint underneath a primary key, a replica identity index, or
     * a generated identity column.
     */
    if (*con).contype == CONSTRAINT_NOTNULL as i8 {
        let attrel: Relation = table_open(AttributeRelationId, RowExclusiveLock);
        let attnum: AttrNumber = extractNotNullColumn(constraint_tup);
        let mut pkattrs: *mut Bitmapset;
        let irattrs: *mut Bitmapset;
        let atttup: *mut HeapTupleData;
        let att_form: Form_pg_attribute;

        /* save column name for recursion step */
        colname = get_attname(RelationGetRelid(rel), attnum, false);

        /*
         * Disallow if it's in the primary key.  For partitioned tables we
         * cannot rely solely on RelationGetIndexAttrBitmap, because it'll
         * return NULL if the primary key is invalid; but we still need to
         * protect not-null constraints under such a constraint, so check the
         * slow way.
         */
        pkattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_PRIMARY_KEY);

        if pkattrs.is_null() && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8 {
            let pkindex: Oid = RelationGetPrimaryKeyIndex(rel, true);
            if OidIsValid(pkindex) {
                let pk: Relation = relation_open(pkindex, AccessShareLock);
                pkattrs = std::ptr::null_mut();
                for i in 0..(*(*pk).rd_index).indnkeyatts as usize {
                    pkattrs = bms_add_member(
                        pkattrs,
                        (*(*pk).rd_index).indkey.values[i] - FirstLowInvalidHeapAttributeNumber,
                    );
                }
                relation_close(pk, AccessShareLock);
            }
        }

        if !pkattrs.is_null()
            && bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, pkattrs)
        {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("column \"{}\" is in a primary key",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy())
            );
        }

        /* Disallow if it's in the replica identity */
        irattrs = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_IDENTITY_KEY);
        if bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber, irattrs) {
            ereport!(
                ERROR,
                errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("column \"{}\" is in index used as replica identity",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy())
            );
        }

        /* Disallow if it's a GENERATED AS IDENTITY column */
        atttup = SearchSysCacheCopyAttNum(RelationGetRelid(rel), attnum);
        if !HeapTupleIsValid(atttup) {
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                RelationGetRelid(rel)
            );
        }
        att_form = GETSTRUCT(atttup) as Form_pg_attribute;
        if (*att_form).attidentity != 0 {
            ereport!(
                ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("column \"{}\" of relation \"{}\" is an identity column",
                    CStr::from_ptr(get_attname(RelationGetRelid(rel), attnum, false)).to_string_lossy(),
                    CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
            );
        }

        /* All good -- reset attnotnull if needed */
        if (*att_form).attnotnull {
            (*att_form).attnotnull = false;
            CatalogTupleUpdate(attrel, &mut (*atttup).t_self, atttup);
        }

        table_close(attrel, RowExclusiveLock);
    }

    is_no_inherit_constraint = (*con).connoinherit;

    /*
     * If it's a foreign-key constraint, we'd better lock the referenced table
     * and check that that's not in use, just as we've already done for the
     * constrained table (else we might, eg, be dropping a trigger that has
     * unfired events).  But we can/must skip that in the self-referential case.
     */
    if (*con).contype == CONSTRAINT_FOREIGN as i8
        && (*con).confrelid != RelationGetRelid(rel)
    {
        let frel: Relation;
        /* Must match lock taken by RemoveTriggerById: */
        frel = table_open((*con).confrelid, AccessExclusiveLock);
        CheckAlterTableIsSafe(frel);
        table_close(frel, NoLock);
    }

    /* Perform the actual constraint deletion */
    ObjectAddressSet(&mut conobj, ConstraintRelationId, (*con).oid);
    performDeletion(&conobj, behavior, 0);

    /*
     * For partitioned tables, non-CHECK, non-NOT-NULL inherited constraints
     * are dropped via the dependency mechanism, so we're done here.
     */
    if (*con).contype != CONSTRAINT_CHECK as i8
        && (*con).contype != CONSTRAINT_NOTNULL as i8
        && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE as i8
    {
        table_close(conrel, RowExclusiveLock);
        return conobj;
    }

    /*
     * Propagate to children as appropriate.  Unlike most other ALTER
     * routines, we have to do this one level of recursion at a time; we can't
     * use find_all_inheritors to do it in one pass.
     */
    if !is_no_inherit_constraint {
        children = find_inheritance_children(RelationGetRelid(rel), lockmode);
    } else {
        children = NIL;
    }

    foreach_oid!(childrelid, children, {
        let childrel: Relation;
        let tuple: *mut HeapTupleData;
        let childcon: Form_pg_constraint;

        /* find_inheritance_children already got lock */
        childrel = table_open(childrelid, NoLock);
        CheckAlterTableIsSafe(childrel);

        /*
         * We search for not-null constraints by column name, and others by
         * constraint name.
         */
        if (*con).contype == CONSTRAINT_NOTNULL as i8 {
            tuple = findNotNullConstraint(childrelid, colname);
            if !HeapTupleIsValid(tuple) {
                elog!(
                    ERROR,
                    "cache lookup failed for not-null constraint on column \"{}\" of relation {}",
                    CStr::from_ptr(colname).to_string_lossy(),
                    RelationGetRelid(childrel)
                );
            }
        } else {
            let scan: SysScanDesc;
            let mut skey: [ScanKeyData; 3] = std::mem::zeroed();

            ScanKeyInit(
                &mut skey[0],
                Anum_pg_constraint_conrelid as i16,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(childrelid),
            );
            ScanKeyInit(
                &mut skey[1],
                Anum_pg_constraint_contypid as i16,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(InvalidOid),
            );
            ScanKeyInit(
                &mut skey[2],
                Anum_pg_constraint_conname as i16,
                BTEqualStrategyNumber,
                F_NAMEEQ,
                CStringGetDatum(constr_name),
            );
            scan = systable_beginscan(
                conrel,
                ConstraintRelidTypidNameIndexId,
                true,
                std::ptr::null_mut(),
                3,
                skey.as_mut_ptr(),
            );
            /* There can only be one, so no need to loop */
            tuple = systable_getnext(scan);
            if !HeapTupleIsValid(tuple) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("constraint \"{}\" of relation \"{}\" does not exist",
                        CStr::from_ptr(constr_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }
            let tuple = heap_copytuple(tuple);
            systable_endscan(scan);
            // use heap_copytuple result
            let _ = tuple;
        }

        childcon = GETSTRUCT(tuple) as Form_pg_constraint;

        /* Right now only CHECK and not-null constraints can be inherited */
        if (*childcon).contype != CONSTRAINT_CHECK as i8
            && (*childcon).contype != CONSTRAINT_NOTNULL as i8
        {
            elog!(ERROR, "inherited constraint is not a CHECK or not-null constraint");
        }

        if (*childcon).coninhcount <= 0 {
            /* shouldn't happen */
            elog!(
                ERROR,
                "relation {} has non-inherited constraint \"{}\"",
                childrelid,
                CStr::from_ptr(NameStr((*childcon).conname) as *const i8).to_string_lossy()
            );
        }

        if recurse {
            /*
             * If the child constraint has other definition sources, just
             * decrement its inheritance count; if not, recurse to delete it.
             */
            if (*childcon).coninhcount == 1 && !(*childcon).conislocal {
                /* Time to delete this child constraint, too */
                dropconstraint_internal(
                    childrel, tuple, behavior, recurse, true, missing_ok, lockmode,
                );
            } else {
                /* Child constraint must survive my deletion */
                (*childcon).coninhcount -= 1;
                CatalogTupleUpdate(conrel, &mut (*tuple).t_self, tuple);
                /* Make update visible */
                CommandCounterIncrement();
            }
        } else {
            /*
             * If we were told to drop ONLY in this table (no recursion) and
             * there are no further parents for this constraint, we need to
             * mark the inheritors' constraints as locally defined rather than
             * inherited.
             */
            (*childcon).coninhcount -= 1;
            if (*childcon).coninhcount == 0 {
                (*childcon).conislocal = true;
            }
            CatalogTupleUpdate(conrel, &mut (*tuple).t_self, tuple);
            /* Make update visible */
            CommandCounterIncrement();
        }

        heap_freetuple(tuple);

        table_close(childrel, NoLock);
    });

    table_close(conrel, RowExclusiveLock);

    conobj
}

/*
 * ALTER COLUMN TYPE
 *
 * Unlike other subcommand types, we do parse transformation for ALTER COLUMN
 * TYPE during phase 1 --- the AlterTableCmd passed in here is already
 * transformed (and must be, because we rely on some transformed fields).
 *
 * The point of this is that the execution of all ALTER COLUMN TYPEs for a
 * table will be done "in parallel" during phase 3, so all the USING
 * expressions should be parsed assuming the original column types.  Also,
 * this allows a USING expression to refer to a field that will be dropped.
 *
 * To make this work safely, AT_PASS_DROP then AT_PASS_ALTER_TYPE must be
 * the first two execution steps in phase 2; they must not see the effects
 * of any other subcommand types, since the USING expressions are parsed
 * against the unmodified table's state.
 */
unsafe fn at_prep_alter_column_type(
    wqueue: *mut *mut List,
    tab: *mut AlteredTableInfo,
    rel: Relation,
    recurse: bool,
    recursing: bool,
    cmd: *mut AlterTableCmd,
    lockmode: LOCKMODE,
    context: *mut AlterTableUtilityContext,
) {
    let col_name: *mut i8 = (*cmd).name;
    let def: *mut ColumnDef = (*cmd).def as *mut ColumnDef;
    let type_name: *mut TypeName = (*def).typeName;
    let mut transform: *mut Node = (*def).cooked_default as *mut Node;
    let tuple: *mut HeapTupleData;
    let att_tup: Form_pg_attribute;
    let attnum: AttrNumber;
    let mut targettype: Oid = InvalidOid;
    let mut targettypmod: i32 = 0;
    let targetcollid: Oid;
    let newval: *mut NewColumnValue;
    let pstate: *mut ParseState = make_parsestate(std::ptr::null_mut());
    let aclresult: AclResult;
    let mut is_expr: bool = false;

    (*pstate).p_sourcetext = (*context).queryString;

    if (*(*rel).rd_rel).reloftype && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("cannot alter column type of typed table"),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* lookup the attribute so we can check inheritance status */
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), col_name);
    if !HeapTupleIsValid(tuple) {
        ereport!(
            ERROR,
            errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("column \"{}\" of relation \"{}\" does not exist",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }
    att_tup = GETSTRUCT(tuple) as Form_pg_attribute;
    attnum = (*att_tup).attnum;

    /* Can't alter a system attribute */
    if attnum <= 0 {
        ereport!(
            ERROR,
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot alter system column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /*
     * Cannot specify USING when altering type of a generated column, because
     * that would violate the generation expression.
     */
    if (*att_tup).attgenerated != 0 && !(*def).cooked_default.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
            errmsg("cannot specify USING when altering type of generated column"),
            errdetail("Column \"{}\" is a generated column.",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /*
     * Don't alter inherited columns.  At outer level, there had better not be
     * any inherited definition; when recursing, we assume this was checked at
     * the parent level (see below).
     */
    if (*att_tup).attinhcount > 0 && !recursing {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot alter inherited column \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* Don't alter columns used in the partition key */
    if has_partition_attrs(
        rel,
        bms_make_singleton(attnum as i32 - FirstLowInvalidHeapAttributeNumber),
        &mut is_expr,
    ) {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("cannot alter column \"{}\" because it is part of the partition key of relation \"{}\"",
                CStr::from_ptr(col_name).to_string_lossy(),
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()),
            parser_errposition(pstate, (*def).location)
        );
    }

    /* Look up the target type */
    typenameTypeIdAndMod(pstate, type_name, &mut targettype, &mut targettypmod);

    aclresult = object_aclcheck(TypeRelationId, targettype, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, targettype);
    }

    /* And the collation */
    targetcollid = GetColumnDefCollation(pstate, def, targettype);

    /* make sure datatype is legal for a column */
    CheckAttributeType(
        col_name,
        targettype,
        targetcollid,
        list_make1_oid((*(*rel).rd_rel).reltype),
        if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            CHKATYPE_IS_VIRTUAL
        } else {
            0
        },
    );

    if (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
        /* do nothing */
    } else if (*tab).relkind == RELKIND_RELATION as i8
        || (*tab).relkind == RELKIND_PARTITIONED_TABLE as i8
    {
        /*
         * Set up an expression to transform the old data value to the new
         * type. If a USING option was given, use the expression as
         * transformed by transformAlterTableStmt, else just take the old
         * value and try to coerce it.  We do this first so that type
         * incompatibility can be detected before we waste effort, and because
         * we need the expression to be parsed against the original table row
         * type.
         */
        if transform.is_null() {
            transform = makeVar(
                1,
                attnum,
                (*att_tup).atttypid,
                (*att_tup).atttypmod,
                (*att_tup).attcollation,
                0,
            ) as *mut Node;
        }

        transform = coerce_to_target_type(
            pstate,
            transform,
            exprType(transform),
            targettype,
            targettypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if transform.is_null() {
            /* error text depends on whether USING was specified or not */
            if !(*def).cooked_default.is_null() {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("result of USING clause for column \"{}\" cannot be cast automatically to type {}",
                        /* C also: colName, format_type_be(targettype) */
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()),
                    errhint("You might need to add an explicit cast.")
                );
            } else {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("column \"{}\" cannot be cast automatically to type {}",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(format_type_be(targettype)).to_string_lossy()),
                    // translator: USING is SQL, don't translate it
                    if (*att_tup).attgenerated == 0 {
                        errhint("You might need to specify \"USING {}::{}\".",
                            CStr::from_ptr(quote_identifier(col_name)).to_string_lossy(),
                            CStr::from_ptr(format_type_with_typemod(targettype, targettypmod)).to_string_lossy())
                    } else { 0 }
                );
            }
        }

        /* Fix collations after all else */
        assign_expr_collations(pstate, transform);

        /* Expand virtual generated columns in the expr. */
        transform = expand_generated_columns_in_expr(transform, rel, 1);

        /* Plan the expr now so we can accurately assess the need to rewrite. */
        transform = expression_planner(transform as *mut Expr) as *mut Node;

        /*
         * Add a work queue item to make ATRewriteTable update the column
         * contents.
         */
        newval = palloc0(std::mem::size_of::<NewColumnValue>()) as *mut NewColumnValue;
        (*newval).attnum = attnum;
        (*newval).expr = transform as *mut Expr;
        (*newval).is_generated = false;

        (*tab).newvals = lappend((*tab).newvals, newval as *mut std::ffi::c_void);
        if ATColumnChangeRequiresRewrite(transform, attnum) {
            (*tab).rewrite |= AT_REWRITE_COLUMN_REWRITE;
        }
    } else if !transform.is_null() {
        ereport!(
            ERROR,
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("\"{}\" is not a table",
                CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
        );
    }

    if !RELKIND_HAS_STORAGE((*tab).relkind)
        || (*att_tup).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8
    {
        /*
         * For relations or columns without storage, do this check now.
         * Regular tables will check it later when the table is being rewritten.
         */
        find_composite_type_dependencies((*(*rel).rd_rel).reltype, rel, std::ptr::null_mut());
    }

    ReleaseSysCache(tuple);

    /*
     * Recurse manually by queueing a new command for each child, if
     * necessary. We cannot apply ATSimpleRecursion here because we need to
     * remap attribute numbers in the USING expression, if any.
     *
     * If we are told not to recurse, there had better not be any child
     * tables; else the alter would put them out of step.
     */
    if recurse {
        let relid: Oid = RelationGetRelid(rel);
        let child_oids: *mut List;
        let child_numparents: *mut List;

        child_oids = find_all_inheritors(relid, lockmode, &mut child_numparents);

        /*
         * find_all_inheritors does the recursive search of the inheritance
         * hierarchy, so all we have to do is process all of the relids in the
         * list that it returns.
         */
        let mut lo: *mut ListCell = list_head(child_oids);
        let mut li: *mut ListCell = list_head(child_numparents);
        while !lo.is_null() {
            let childrelid: Oid = lfirst_oid(lo);
            let numparents: i32 = lfirst_int(li);
            let childrel: Relation;
            let childtuple: *mut HeapTupleData;
            let childatt_tup: Form_pg_attribute;
            let mut cmd = cmd; // rebind for possible copy

            if childrelid == relid {
                lo = lnext(child_oids, lo);
                li = lnext(child_numparents, li);
                continue;
            }

            /* find_all_inheritors already got lock */
            childrel = relation_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);

            /*
             * Verify that the child doesn't have any inherited definitions of
             * this column that came from outside this inheritance hierarchy.
             * (renameatt makes a similar test, though in a different way
             * because of its different recursion mechanism.)
             */
            childtuple = SearchSysCacheAttName(RelationGetRelid(childrel), col_name);
            if !HeapTupleIsValid(childtuple) {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"{}\" of relation \"{}\" does not exist",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }
            childatt_tup = GETSTRUCT(childtuple) as Form_pg_attribute;

            if (*childatt_tup).attinhcount > numparents {
                ereport!(
                    ERROR,
                    errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("cannot alter inherited column \"{}\" of relation \"{}\"",
                        CStr::from_ptr(col_name).to_string_lossy(),
                        CStr::from_ptr(RelationGetRelationName(childrel)).to_string_lossy())
                );
            }

            ReleaseSysCache(childtuple);

            /*
             * Remap the attribute numbers.  If no USING expression was
             * specified, there is no need for this step.
             */
            if !(*def).cooked_default.is_null() {
                let attmap: *mut AttrMap;
                let mut found_whole_row: bool = false;

                /* create a copy to scribble on */
                cmd = copyObject(cmd as *mut std::ffi::c_void) as *mut AlterTableCmd;

                attmap = build_attrmap_by_name(
                    RelationGetDescr(childrel),
                    RelationGetDescr(rel),
                    false,
                );
                (*((*cmd).def as *mut ColumnDef)).cooked_default = map_variable_attnos(
                    (*def).cooked_default,
                    1,
                    0,
                    attmap,
                    InvalidOid,
                    &mut found_whole_row,
                );
                if found_whole_row {
                    ereport!(
                        ERROR,
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert whole-row table reference"),
                        errdetail("USING expression contains a whole-row table reference.")
                    );
                }
                pfree(attmap as *mut std::ffi::c_void);
            }
            ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
            relation_close(childrel, NoLock);

            lo = lnext(child_oids, lo);
            li = lnext(child_numparents, li);
        }
    } else if !recursing
        && !find_inheritance_children(RelationGetRelid(rel), NoLock).is_null()
    {
        ereport!(
            ERROR,
            errcode(ERRCODE_INVALID_TABLE_DEFINITION),
            errmsg("type of inherited column \"{}\" must be changed in child tables too",
                CStr::from_ptr(col_name).to_string_lossy())
        );
    }

    if (*tab).relkind == RELKIND_COMPOSITE_TYPE as i8 {
        ATTypedTableRecursion(wqueue, rel, cmd, lockmode, context);
    }
}

/*
 * When the data type of a column is changed, a rewrite might not be required
 * if the new type is sufficiently identical to the old one, and the USING
 * clause isn't trying to insert some other value.  It's safe to skip the
 * rewrite in these cases:
 *
 * - the old type is binary coercible to the new type
 * - the new type is an unconstrained domain over the old type
 * - {NEW,OLD} or {OLD,NEW} is {timestamptz,timestamp} and the timezone is UTC
 *
 * In the case of a constrained domain, we could get by with scanning the
 * table and checking the constraint rather than actually rewriting it, but we
 * don't currently try to do that.
 */
unsafe fn ATColumnChangeRequiresRewrite(expr: *mut Node, varattno: AttrNumber) -> bool {
    assert!(!expr.is_null());

    let mut expr = expr;
    loop {
        /* only one varno, so no need to check that */
        if IsA(expr, T_Var) && (*(expr as *mut Var)).varattno == varattno {
            return false;
        } else if IsA(expr, T_RelabelType) {
            expr = (*(expr as *mut RelabelType)).arg as *mut Node;
        } else if IsA(expr, T_CoerceToDomain) {
            let d = expr as *mut CoerceToDomain;
            if DomainHasConstraints((*d).resulttype) {
                return true;
            }
            expr = (*d).arg as *mut Node;
        } else if IsA(expr, T_FuncExpr) {
            let f = expr as *mut FuncExpr;
            match (*f).funcid {
                F_TIMESTAMPTZ_TIMESTAMP | F_TIMESTAMP_TIMESTAMPTZ => {
                    if TimestampTimestampTzRequiresRewrite() {
                        return true;
                    } else {
                        expr = linitial((*f).args) as *mut Node;
                    }
                }
                _ => {
                    return true;
                }
            }
        } else {
            return true;
        }
    }
}
