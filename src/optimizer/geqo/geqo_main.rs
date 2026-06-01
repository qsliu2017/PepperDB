//! optimizer/geqo/geqo_main.c - solution to the query optimization problem by means of a Genetic Algorithm (GA)

/* contributed by:
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
   *  Martin Utesch				 * Institute of Automatic Control	   *
   =							 = University of Mining and Technology =
   *  utesch@aut.tu-freiberg.de  * Freiberg, Germany				   *
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 */

/* -- parts of this are adapted from D. Whitley's Genitor algorithm -- */

use crate::prelude::*;

use std::ffi::c_void;

use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::nodes::pg_list::List;

use crate::optimizer::geqo::geqo::{
    Geqo_pool_size, Geqo_effort, Geqo_generations, Geqo_selection_bias, Geqo_seed,
    GeqoPrivateData,
};
use crate::optimizer::geqo::geqo_copy::Chromosome;
use crate::optimizer::geqo::geqo_pool::free_chromo;
use crate::optimizer::geqo::geqo_erx::{
    alloc_edge_table, free_edge_table, gimme_edge_table, gimme_tour,
};
use crate::optimizer::geqo::geqo_eval::{geqo_eval, gimme_tree};
use crate::optimizer::geqo::geqo_gene::Gene;
use crate::optimizer::geqo::geqo_recombination::Edge;
use crate::optimizer::geqo::geqo_pool::{
    alloc_chromo, alloc_pool, free_pool, random_init_pool, sort_pool, spread_chromo, Pool,
};
use crate::optimizer::geqo::geqo_random::geqo_set_seed;
use crate::optimizer::geqo::geqo_selection::geqo_selection;

/*
 * geqo
 *	  solution of the query optimization problem
 *	  similar to a constrained Traveling Salesman Problem (TSP)
 */
pub unsafe fn geqo(
    root: *mut PlannerInfo,
    number_of_rels: c_int,
    initial_rels: *mut List,
) -> *mut RelOptInfo {
    let mut private: GeqoPrivateData = std::mem::zeroed();
    let mut generation: c_int;
    let momma: *mut Chromosome;
    let daddy: *mut Chromosome;
    let pool: *mut Pool;
    let pool_size: c_int;
    let number_generations: c_int;

    let best_tour: *mut Gene;
    let best_rel: *mut RelOptInfo;

    /* defined(ERX) */
    let edge_table: *mut Edge; /* list of edges */
    let mut edge_failures: c_int = 0;

    /* set up private information */
    (*root).join_search_private = (&raw mut private) as *mut c_void;
    private.initial_rels = initial_rels;

    /* initialize private number generator */
    geqo_set_seed(root, Geqo_seed);

    /* set GA parameters */
    pool_size = gimme_pool_size(number_of_rels);
    number_generations = gimme_number_generations(pool_size);

    /* allocate genetic pool memory */
    pool = alloc_pool(root, pool_size, number_of_rels);

    /* random initialization of the pool */
    random_init_pool(root, pool);

    /* sort the pool according to cheapest path as fitness */
    sort_pool(root, pool); /* we have to do it only one time, since all
                            * kids replace the worst individuals in
                            * future (-> geqo_pool.c:spread_chromo ) */

    /* allocate chromosome momma and daddy memory */
    momma = alloc_chromo(root, (*pool).string_length);
    daddy = alloc_chromo(root, (*pool).string_length);

    /* defined (ERX) */
    /* allocate edge table memory */
    edge_table = alloc_edge_table(root, (*pool).string_length);

    /* my pain main part: */
    /* iterative optimization */

    generation = 0;
    while generation < number_generations {
        /* SELECTION: using linear bias function */
        geqo_selection(root, momma, daddy, pool as *mut _, Geqo_selection_bias);

        /* defined (ERX) */
        /* EDGE RECOMBINATION CROSSOVER */
        gimme_edge_table(
            root,
            (*momma).string,
            (*daddy).string,
            (*pool).string_length,
            edge_table,
        );

        let kid: *mut Chromosome = momma; /* C: kid = momma (ERX reuses momma's storage) */

        /* are there any edge failures ? */
        edge_failures +=
            gimme_tour(root, edge_table, (*kid).string, (*pool).string_length);

        /* EVALUATE FITNESS */
        (*kid).worth = geqo_eval(root, (*kid).string, (*pool).string_length);

        /* push the kid into the wilderness of life according to its worth */
        spread_chromo(root, kid, pool);

        generation += 1;
    }

    /* defined(ERX), no GEQO_DEBUG */
    /* suppress variable-set-but-not-used warnings from some compilers */
    let _ = edge_failures;

    /*
     * got the cheapest query tree processed by geqo; first element of the
     * population indicates the best query tree
     */
    best_tour = (*(*pool).data.offset(0)).string as *mut Gene;

    best_rel = gimme_tree(root, best_tour, (*pool).string_length);

    if best_rel.is_null() {
        elog!(ERROR, "geqo failed to make a valid plan");
    }

    /* ... free memory stuff */
    free_chromo(root, momma);
    free_chromo(root, daddy);

    /* defined (ERX) */
    free_edge_table(root, edge_table);

    free_pool(root, pool);

    /* ... clear root pointer to our private storage */
    (*root).join_search_private = std::ptr::null_mut();

    best_rel
}

/*
 * Return either configured pool size or a good default
 *
 * The default is based on query size (no. of relations) = 2^(QS+1),
 * but constrained to a range based on the effort value.
 */
unsafe fn gimme_pool_size(nr_rel: c_int) -> c_int {
    let size: f64;
    let minsize: c_int;
    let maxsize: c_int;

    /* Legal pool size *must* be at least 2, so ignore attempt to select 1 */
    if Geqo_pool_size >= 2 {
        return Geqo_pool_size;
    }

    size = (2.0_f64).powf(nr_rel as f64 + 1.0);

    maxsize = 50 * Geqo_effort; /* 50 to 500 individuals */
    if size > maxsize as f64 {
        return maxsize;
    }

    minsize = 10 * Geqo_effort; /* 10 to 100 individuals */
    if size < minsize as f64 {
        return minsize;
    }

    size.ceil() as c_int
}

/*
 * Return either configured number of generations or a good default
 *
 * The default is the same as the pool size, which allows us to be
 * sure that less-fit individuals get pushed out of the breeding
 * population before the run finishes.
 */
unsafe fn gimme_number_generations(pool_size: c_int) -> c_int {
    if Geqo_generations > 0 {
        return Geqo_generations;
    }

    pool_size
}
