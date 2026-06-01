//! optimizer/geqo/geqo_eval.c - Routines to evaluate query trees

/* contributed by:
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
   *  Martin Utesch				 * Institute of Automatic Control	   *
   =							 = University of Mining and Technology =
   *  utesch@aut.tu-freiberg.de  * Freiberg, Germany				   *
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 */

use crate::prelude::*;

use crate::current_cell;
use crate::foreach;
use crate::foreach_delete_current;

use crate::nodes::nodes::Cost;
use crate::nodes::pathnodes::{Path, PlannerInfo, RelOptInfo, HTAB};
use crate::nodes::pg_list::{
    lappend, lfirst, linitial, list_insert_nth, list_length, list_nth, list_truncate, List, NIL,
};
use crate::nodes::bitmapset::bms_equal;
use crate::optimizer::geqo::geqo_gene::Gene;
use crate::optimizer::geqo::geqo::GeqoPrivateData;
use crate::optimizer::paths::{
    generate_partitionwise_join_paths, generate_useful_gather_paths, have_join_order_restriction,
    make_join_rel,
};
use crate::optimizer::util::joininfo::have_relevant_joinclause;

/* A "clump" of already-joined relations within gimme_tree */
#[repr(C)]
struct Clump {
    joinrel: *mut RelOptInfo, /* joinrel for the set of relations */
    size: c_int,              /* number of input relations in clump */
}

/*
 * geqo_eval
 *
 * Returns cost of a query tree as an individual of the population.
 *
 * If no legal join order can be extracted from the proposed tour,
 * returns DBL_MAX.
 */
pub unsafe fn geqo_eval(root: *mut PlannerInfo, tour: *mut Gene, num_gene: c_int) -> Cost {
    let mycontext: MemoryContext;
    let oldcxt: MemoryContext;
    let joinrel: *mut RelOptInfo;
    let fitness: Cost;
    let savelength: c_int;
    let savehash: *mut HTAB;

    /*
     * Create a private memory context that will hold all temp storage
     * allocated inside gimme_tree().
     *
     * Since geqo_eval() will be called many times, we can't afford to let all
     * that memory go unreclaimed until end of statement.  Note we make the
     * temp context a child of the planner's normal context, so that it will
     * be freed even if we abort via ereport(ERROR).
     */
    mycontext = AllocSetContextCreate!(CurrentMemoryContext, "GEQO", ALLOCSET_DEFAULT_SIZES)
        as *mut _;
    oldcxt = MemoryContextSwitchTo(mycontext);

    /*
     * gimme_tree will add entries to root->join_rel_list, which may or may
     * not already contain some entries.  The newly added entries will be
     * recycled by the MemoryContextDelete below, so we must ensure that the
     * list is restored to its former state before exiting.  We can do this by
     * truncating the list to its original length.  NOTE this assumes that any
     * added entries are appended at the end!
     *
     * We also must take care not to mess up the outer join_rel_hash, if there
     * is one.  We can do this by just temporarily setting the link to NULL.
     * (If we are dealing with enough join rels, which we very likely are, a
     * new hash table will get built and used locally.)
     *
     * join_rel_level[] shouldn't be in use, so just Assert it isn't.
     */
    savelength = list_length((*root).join_rel_list);
    savehash = (*root).join_rel_hash;
    Assert!((*root).join_rel_level.is_null());

    (*root).join_rel_hash = null_mut();

    /* construct the best path for the given combination of relations */
    joinrel = gimme_tree(root, tour, num_gene);

    /*
     * compute fitness, if we found a valid join
     *
     * XXX geqo does not currently support optimization for partial result
     * retrieval, nor do we take any cognizance of possible use of
     * parameterized paths --- how to fix?
     */
    if !joinrel.is_null() {
        let best_path: *mut Path = (*joinrel).cheapest_total_path;

        fitness = (*best_path).total_cost;
    } else {
        fitness = f64::MAX;
    }

    /*
     * Restore join_rel_list to its former state, and put back original
     * hashtable if any.
     */
    (*root).join_rel_list = list_truncate((*root).join_rel_list, savelength);
    (*root).join_rel_hash = savehash;

    /* release all the memory acquired within gimme_tree */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(mycontext);

    fitness
}

/*
 * gimme_tree
 *	  Form planner estimates for a join tree constructed in the specified
 *	  order.
 *
 *	 'tour' is the proposed join order, of length 'num_gene'
 *
 * Returns a new join relation whose cheapest path is the best plan for
 * this join order.  NB: will return NULL if join order is invalid and
 * we can't modify it into a valid order.
 *
 * The original implementation of this routine always joined in the specified
 * order, and so could only build left-sided plans (and right-sided and
 * mixtures, as a byproduct of the fact that make_join_rel() is symmetric).
 * It could never produce a "bushy" plan.  This had a couple of big problems,
 * of which the worst was that there are situations involving join order
 * restrictions where the only valid plans are bushy.
 *
 * The present implementation takes the given tour as a guideline, but
 * postpones joins that are illegal or seem unsuitable according to some
 * heuristic rules.  This allows correct bushy plans to be generated at need,
 * and as a nice side-effect it seems to materially improve the quality of the
 * generated plans.  Note however that since it's just a heuristic, it can
 * still fail in some cases.  (In particular, we might clump together
 * relations that actually mustn't be joined yet due to LATERAL restrictions;
 * since there's no provision for un-clumping, this must lead to failure.)
 */
pub unsafe fn gimme_tree(
    root: *mut PlannerInfo,
    tour: *mut Gene,
    num_gene: c_int,
) -> *mut RelOptInfo {
    let private: *mut GeqoPrivateData = (*root).join_search_private as *mut GeqoPrivateData;
    let mut clumps: *mut List;
    let mut rel_count: c_int;

    /*
     * Sometimes, a relation can't yet be joined to others due to heuristics
     * or actual semantic restrictions.  We maintain a list of "clumps" of
     * successfully joined relations, with larger clumps at the front. Each
     * new relation from the tour is added to the first clump it can be joined
     * to; if there is none then it becomes a new clump of its own. When we
     * enlarge an existing clump we check to see if it can now be merged with
     * any other clumps.  After the tour is all scanned, we forget about the
     * heuristics and try to forcibly join any remaining clumps.  If we are
     * unable to merge all the clumps into one, fail.
     */
    clumps = NIL;

    rel_count = 0;
    while rel_count < num_gene {
        let cur_rel_index: c_int;
        let cur_rel: *mut RelOptInfo;
        let cur_clump: *mut Clump;

        /* Get the next input relation */
        cur_rel_index = *tour.offset(rel_count as isize) as c_int;
        cur_rel = list_nth((*private).initial_rels, cur_rel_index - 1) as *mut RelOptInfo;

        /* Make it into a single-rel clump */
        cur_clump = palloc(std::mem::size_of::<Clump>()) as *mut Clump;
        (*cur_clump).joinrel = cur_rel;
        (*cur_clump).size = 1;

        /* Merge it into the clumps list, using only desirable joins */
        clumps = merge_clump(root, clumps, cur_clump, num_gene, false);

        rel_count += 1;
    }

    if list_length(clumps) > 1 {
        /* Force-join the remaining clumps in some legal order */
        let mut fclumps: *mut List;

        fclumps = NIL;
        foreach!(lc, clumps, {
            let clump: *mut Clump = lfirst(current_cell!(lc)) as *mut Clump;

            fclumps = merge_clump(root, fclumps, clump, num_gene, true);
        });
        clumps = fclumps;
    }

    /* Did we succeed in forming a single join relation? */
    if list_length(clumps) != 1 {
        return null_mut();
    }

    (*(linitial(clumps) as *mut Clump)).joinrel
}

/*
 * Merge a "clump" into the list of existing clumps for gimme_tree.
 *
 * We try to merge the clump into some existing clump, and repeat if
 * successful.  When no more merging is possible, insert the clump
 * into the list, preserving the list ordering rule (namely, that
 * clumps of larger size appear earlier).
 *
 * If force is true, merge anywhere a join is legal, even if it causes
 * a cartesian join to be performed.  When force is false, do only
 * "desirable" joins.
 */
unsafe fn merge_clump(
    root: *mut PlannerInfo,
    mut clumps: *mut List,
    new_clump: *mut Clump,
    num_gene: c_int,
    force: bool,
) -> *mut List {
    let mut pos: c_int;

    /* Look for a clump that new_clump can join to */
    foreach!(lc, clumps, {
        let old_clump: *mut Clump = lfirst(current_cell!(lc)) as *mut Clump;

        if force || desirable_join(root, (*old_clump).joinrel, (*new_clump).joinrel) {
            let joinrel: *mut RelOptInfo;

            /*
             * Construct a RelOptInfo representing the join of these two input
             * relations.  Note that we expect the joinrel not to exist in
             * root->join_rel_list yet, and so the paths constructed for it
             * will only include the ones we want.
             */
            joinrel = make_join_rel(root, (*old_clump).joinrel, (*new_clump).joinrel);

            /* Keep searching if join order is not valid */
            if !joinrel.is_null() {
                /* Create paths for partitionwise joins. */
                generate_partitionwise_join_paths(root, joinrel);

                /*
                 * Except for the topmost scan/join rel, consider gathering
                 * partial paths.  We'll do the same for the topmost scan/join
                 * rel once we know the final targetlist (see
                 * grouping_planner).
                 */
                if !bms_equal((*joinrel).relids, (*root).all_query_rels) {
                    generate_useful_gather_paths(root, joinrel, false);
                }

                /* Find and save the cheapest paths for this joinrel */
                set_cheapest(joinrel);

                /* Absorb new clump into old */
                (*old_clump).joinrel = joinrel;
                (*old_clump).size += (*new_clump).size;
                pfree(new_clump as *mut c_void);

                /* Remove old_clump from list */
                clumps = foreach_delete_current!(clumps, lc);

                /*
                 * Recursively try to merge the enlarged old_clump with
                 * others.  When no further merge is possible, we'll reinsert
                 * it into the list.
                 */
                return merge_clump(root, clumps, old_clump, num_gene, force);
            }
        }
    });

    /*
     * No merging is possible, so add new_clump as an independent clump, in
     * proper order according to size.  We can be fast for the common case
     * where it has size 1 --- it should always go at the end.
     */
    if clumps.is_null() || (*new_clump).size == 1 {
        return lappend(clumps, new_clump as *mut c_void);
    }

    /* Else search for the place to insert it */
    pos = 0;
    while pos < list_length(clumps) {
        let old_clump: *mut Clump = list_nth(clumps, pos) as *mut Clump;

        if (*new_clump).size > (*old_clump).size {
            break; /* new_clump belongs before old_clump */
        }
        pos += 1;
    }
    clumps = list_insert_nth(clumps, pos, new_clump as *mut c_void);

    clumps
}

/*
 * Heuristics for gimme_tree: do we want to join these two relations?
 */
unsafe fn desirable_join(
    root: *mut PlannerInfo,
    outer_rel: *mut RelOptInfo,
    inner_rel: *mut RelOptInfo,
) -> bool {
    /*
     * Join if there is an applicable join clause, or if there is a join order
     * restriction forcing these rels to be joined.
     */
    if have_relevant_joinclause(root, outer_rel, inner_rel)
        || have_join_order_restriction(root, outer_rel, inner_rel)
    {
        return true;
    }

    /* Otherwise postpone the join till later. */
    false
}

/* set_cheapest (allpaths.c) is not yet ported. */
// TODO: import set_cheapest once optimizer/path/allpaths.c is translated.
unsafe fn set_cheapest(_parent_rel: *mut RelOptInfo) {
    unimplemented!()
}
