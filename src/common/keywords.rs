//! Translation of postgres/src/common/keywords.c + the gen_keywordlist.pl-generated
//! kwlist_d.h (data from postgres/src/include/parser/kwlist.h).
//!
//! The SQL `ScanKeywords` keyword list for the lexer. The C build uses a
//! perfect hash from PerfectHash.pm; we substitute a binary search over the
//! (sorted) keyword list, which ScanKeywordLookup strcmp-verifies, so results
//! are identical. TODO(pg-port): generate the real perfect hash.

use crate::common::kwlookup::ScanKeywordList;
use core::ffi::{c_char, c_int, c_void};

// keyword categories (parser/keywords.h)
pub const UNRESERVED_KEYWORD: c_int = 0;
pub const COL_NAME_KEYWORD: c_int = 1;
pub const TYPE_FUNC_NAME_KEYWORD: c_int = 2;
pub const RESERVED_KEYWORD: c_int = 3;

pub const NumScanKeywords: c_int = 494;

static SCANKEYWORDS_KW_STRING: &[u8; 3800] = b"abort\x00absent\x00absolute\x00access\x00action\x00add\x00admin\x00after\x00aggregate\x00all\x00also\x00alter\x00always\x00analyse\x00analyze\x00and\x00any\x00array\x00as\x00asc\x00asensitive\x00assertion\x00assignment\x00asymmetric\x00at\x00atomic\x00attach\x00attribute\x00authorization\x00backward\x00before\x00begin\x00between\x00bigint\x00binary\x00bit\x00boolean\x00both\x00breadth\x00by\x00cache\x00call\x00called\x00cascade\x00cascaded\x00case\x00cast\x00catalog\x00chain\x00char\x00character\x00characteristics\x00check\x00checkpoint\x00class\x00close\x00cluster\x00coalesce\x00collate\x00collation\x00column\x00columns\x00comment\x00comments\x00commit\x00committed\x00compression\x00concurrently\x00conditional\x00configuration\x00conflict\x00connection\x00constraint\x00constraints\x00content\x00continue\x00conversion\x00copy\x00cost\x00create\x00cross\x00csv\x00cube\x00current\x00current_catalog\x00current_date\x00current_role\x00current_schema\x00current_time\x00current_timestamp\x00current_user\x00cursor\x00cycle\x00data\x00database\x00day\x00deallocate\x00dec\x00decimal\x00declare\x00default\x00defaults\x00deferrable\x00deferred\x00definer\x00delete\x00delimiter\x00delimiters\x00depends\x00depth\x00desc\x00detach\x00dictionary\x00disable\x00discard\x00distinct\x00do\x00document\x00domain\x00double\x00drop\x00each\x00else\x00empty\x00enable\x00encoding\x00encrypted\x00end\x00enforced\x00enum\x00error\x00escape\x00event\x00except\x00exclude\x00excluding\x00exclusive\x00execute\x00exists\x00explain\x00expression\x00extension\x00external\x00extract\x00false\x00family\x00fetch\x00filter\x00finalize\x00first\x00float\x00following\x00for\x00force\x00foreign\x00format\x00forward\x00freeze\x00from\x00full\x00function\x00functions\x00generated\x00global\x00grant\x00granted\x00greatest\x00group\x00grouping\x00groups\x00handler\x00having\x00header\x00hold\x00hour\x00identity\x00if\x00ilike\x00immediate\x00immutable\x00implicit\x00import\x00in\x00include\x00including\x00increment\x00indent\x00index\x00indexes\x00inherit\x00inherits\x00initially\x00inline\x00inner\x00inout\x00input\x00insensitive\x00insert\x00instead\x00int\x00integer\x00intersect\x00interval\x00into\x00invoker\x00is\x00isnull\x00isolation\x00join\x00json\x00json_array\x00json_arrayagg\x00json_exists\x00json_object\x00json_objectagg\x00json_query\x00json_scalar\x00json_serialize\x00json_table\x00json_value\x00keep\x00key\x00keys\x00label\x00language\x00large\x00last\x00lateral\x00leading\x00leakproof\x00least\x00left\x00level\x00like\x00limit\x00listen\x00load\x00local\x00localtime\x00localtimestamp\x00location\x00lock\x00locked\x00logged\x00mapping\x00match\x00matched\x00materialized\x00maxvalue\x00merge\x00merge_action\x00method\x00minute\x00minvalue\x00mode\x00month\x00move\x00name\x00names\x00national\x00natural\x00nchar\x00nested\x00new\x00next\x00nfc\x00nfd\x00nfkc\x00nfkd\x00no\x00none\x00normalize\x00normalized\x00not\x00nothing\x00notify\x00notnull\x00nowait\x00null\x00nullif\x00nulls\x00numeric\x00object\x00objects\x00of\x00off\x00offset\x00oids\x00old\x00omit\x00on\x00only\x00operator\x00option\x00options\x00or\x00order\x00ordinality\x00others\x00out\x00outer\x00over\x00overlaps\x00overlay\x00overriding\x00owned\x00owner\x00parallel\x00parameter\x00parser\x00partial\x00partition\x00passing\x00password\x00path\x00period\x00placing\x00plan\x00plans\x00policy\x00position\x00preceding\x00precision\x00prepare\x00prepared\x00preserve\x00primary\x00prior\x00privileges\x00procedural\x00procedure\x00procedures\x00program\x00publication\x00quote\x00quotes\x00range\x00read\x00real\x00reassign\x00recursive\x00ref\x00references\x00referencing\x00refresh\x00reindex\x00relative\x00release\x00rename\x00repeatable\x00replace\x00replica\x00reset\x00restart\x00restrict\x00return\x00returning\x00returns\x00revoke\x00right\x00role\x00rollback\x00rollup\x00routine\x00routines\x00row\x00rows\x00rule\x00savepoint\x00scalar\x00schema\x00schemas\x00scroll\x00search\x00second\x00security\x00select\x00sequence\x00sequences\x00serializable\x00server\x00session\x00session_user\x00set\x00setof\x00sets\x00share\x00show\x00similar\x00simple\x00skip\x00smallint\x00snapshot\x00some\x00source\x00sql\x00stable\x00standalone\x00start\x00statement\x00statistics\x00stdin\x00stdout\x00storage\x00stored\x00strict\x00string\x00strip\x00subscription\x00substring\x00support\x00symmetric\x00sysid\x00system\x00system_user\x00table\x00tables\x00tablesample\x00tablespace\x00target\x00temp\x00template\x00temporary\x00text\x00then\x00ties\x00time\x00timestamp\x00to\x00trailing\x00transaction\x00transform\x00treat\x00trigger\x00trim\x00true\x00truncate\x00trusted\x00type\x00types\x00uescape\x00unbounded\x00uncommitted\x00unconditional\x00unencrypted\x00union\x00unique\x00unknown\x00unlisten\x00unlogged\x00until\x00update\x00user\x00using\x00vacuum\x00valid\x00validate\x00validator\x00value\x00values\x00varchar\x00variadic\x00varying\x00verbose\x00version\x00view\x00views\x00virtual\x00volatile\x00when\x00where\x00whitespace\x00window\x00with\x00within\x00without\x00work\x00wrapper\x00write\x00xml\x00xmlattributes\x00xmlconcat\x00xmlelement\x00xmlexists\x00xmlforest\x00xmlnamespaces\x00xmlparse\x00xmlpi\x00xmlroot\x00xmlserialize\x00xmltable\x00year\x00yes\x00zone\x00";

static SCANKEYWORDS_KW_OFFSETS: [u16; 494] = [
    0, 6, 13, 22, 29, 36, 40, 46, 52, 62, 66, 71, 77, 84, 92, 100,
    104, 108, 114, 117, 121, 132, 142, 153, 164, 167, 174, 181, 191, 205, 214, 221,
    227, 235, 242, 249, 253, 261, 266, 274, 277, 283, 288, 295, 303, 312, 317, 322,
    330, 336, 341, 351, 367, 373, 384, 390, 396, 404, 413, 421, 431, 438, 446, 454,
    463, 470, 480, 492, 505, 517, 531, 540, 551, 562, 574, 582, 591, 602, 607, 612,
    619, 625, 629, 634, 642, 658, 671, 684, 699, 712, 730, 743, 750, 756, 761, 770,
    774, 785, 789, 797, 805, 813, 822, 833, 842, 850, 857, 867, 878, 886, 892, 897,
    904, 915, 923, 931, 940, 943, 952, 959, 966, 971, 976, 981, 987, 994, 1003, 1013,
    1017, 1026, 1031, 1037, 1044, 1050, 1057, 1065, 1075, 1085, 1093, 1100, 1108, 1119, 1129, 1138,
    1146, 1152, 1159, 1165, 1172, 1181, 1187, 1193, 1203, 1207, 1213, 1221, 1228, 1236, 1243, 1248,
    1253, 1262, 1272, 1282, 1289, 1295, 1303, 1312, 1318, 1327, 1334, 1342, 1349, 1356, 1361, 1366,
    1375, 1378, 1384, 1394, 1404, 1413, 1420, 1423, 1431, 1441, 1451, 1458, 1464, 1472, 1480, 1489,
    1499, 1506, 1512, 1518, 1524, 1536, 1543, 1551, 1555, 1563, 1573, 1582, 1587, 1595, 1598, 1605,
    1615, 1620, 1625, 1636, 1650, 1662, 1674, 1689, 1700, 1712, 1727, 1738, 1749, 1754, 1758, 1763,
    1769, 1778, 1784, 1789, 1797, 1805, 1815, 1821, 1826, 1832, 1837, 1843, 1850, 1855, 1861, 1871,
    1886, 1895, 1900, 1907, 1914, 1922, 1928, 1936, 1949, 1958, 1964, 1977, 1984, 1991, 2000, 2005,
    2011, 2016, 2021, 2027, 2036, 2044, 2050, 2057, 2061, 2066, 2070, 2074, 2079, 2084, 2087, 2092,
    2102, 2113, 2117, 2125, 2132, 2140, 2147, 2152, 2159, 2165, 2173, 2180, 2188, 2191, 2195, 2202,
    2207, 2211, 2216, 2219, 2224, 2233, 2240, 2248, 2251, 2257, 2268, 2275, 2279, 2285, 2290, 2299,
    2307, 2318, 2324, 2330, 2339, 2349, 2356, 2364, 2374, 2382, 2391, 2396, 2403, 2411, 2416, 2422,
    2429, 2438, 2448, 2458, 2466, 2475, 2484, 2492, 2498, 2509, 2520, 2530, 2541, 2549, 2561, 2567,
    2574, 2580, 2585, 2590, 2599, 2609, 2613, 2624, 2636, 2644, 2652, 2661, 2669, 2676, 2687, 2695,
    2703, 2709, 2717, 2726, 2733, 2743, 2751, 2758, 2764, 2769, 2778, 2785, 2793, 2802, 2806, 2811,
    2816, 2826, 2833, 2840, 2848, 2855, 2862, 2869, 2878, 2885, 2894, 2904, 2917, 2924, 2932, 2945,
    2949, 2955, 2960, 2966, 2971, 2979, 2986, 2991, 3000, 3009, 3014, 3021, 3025, 3032, 3043, 3049,
    3059, 3070, 3076, 3083, 3091, 3098, 3105, 3112, 3118, 3131, 3141, 3149, 3159, 3165, 3172, 3184,
    3190, 3197, 3209, 3220, 3227, 3232, 3241, 3251, 3256, 3261, 3266, 3271, 3281, 3284, 3293, 3305,
    3315, 3321, 3329, 3334, 3339, 3348, 3356, 3361, 3367, 3375, 3385, 3397, 3411, 3423, 3429, 3436,
    3444, 3453, 3462, 3468, 3475, 3480, 3486, 3493, 3499, 3508, 3518, 3524, 3531, 3539, 3548, 3556,
    3564, 3572, 3577, 3583, 3591, 3600, 3605, 3611, 3622, 3629, 3634, 3641, 3649, 3654, 3662, 3668,
    3672, 3686, 3696, 3707, 3717, 3727, 3741, 3750, 3756, 3764, 3777, 3786, 3791, 3795,
];

pub static ScanKeywordCategories: [u8; 494] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 3,
    0, 0, 0, 0, 2, 0, 0, 0, 1, 1, 2, 1, 1, 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0,
    0, 1, 1, 0, 3, 0, 0, 0, 0, 1, 3, 2, 3, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
    3, 0, 0, 0, 0, 0, 0, 3, 2, 0, 0, 0, 3, 3, 3, 2, 3, 3, 3, 0, 0, 0, 0, 0,
    0, 1, 1, 0, 3, 0, 3, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 3, 3, 0, 0, 0,
    0, 0, 3, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1,
    3, 0, 3, 0, 0, 0, 1, 0, 3, 0, 3, 0, 0, 2, 3, 2, 0, 0, 0, 0, 3, 0, 1, 3,
    1, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 3,
    0, 2, 1, 0, 0, 0, 0, 1, 1, 3, 1, 3, 0, 2, 2, 0, 2, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 1, 2, 0, 2, 3, 0, 0, 0, 3, 3,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 1, 1, 0, 3, 0, 0, 2, 0, 3, 1, 0, 1, 0, 0, 0, 0, 3, 0,
    0, 0, 3, 3, 0, 0, 0, 3, 3, 0, 0, 1, 2, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 2,
    0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0,
    1, 0, 0, 0, 2, 0, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1, 0, 3, 0, 0, 3, 3, 0, 2, 0, 0, 0, 0, 0, 0, 3, 0, 1, 1, 3, 3, 0, 0,
    1, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 3, 3, 0, 0,
    0, 0, 0, 1, 1, 3, 0, 2, 0, 0, 0, 0, 0, 3, 3, 0, 3, 3, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0,
];

pub static ScanKeywordBareLabel: [bool; 494] = [
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, false, false, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, false, false, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, false, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, false,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, false, true, true, true, true, true, true, true, true, true, true,
    true, true, false, false, true, true, true, true, false, true, true, true,
    true, true, false, true, true, true, true, true, false, true, true, false,
    true, true, true, false, true, true, false, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, false, true, false,
    true, true, false, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, false, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    false, true, true, false, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    false, true, true, true, true, true, true, true, true, true, false, true,
    true, true, false, true, true, true, true, true, false, true, true, true,
    true, false, false, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, false, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, false, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, false, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, true, true, false, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, true,
    true, false, true, true, true, true, true, true, true, true, true, true,
    true, true, true, true, true, true, false, true, true, true, true, true,
    true, true, false, true, false, false, false, false, true, true, true, true,
    true, true, true, true, true, true, true, true, true, true, true, false,
    true, true,
];

// ScanKeywordList holds raw pointers into the 'static tables above; sound to share.
unsafe impl Sync for ScanKeywordList {}

/// Binary-search stand-in for the generated perfect hash. Returns the index of
/// the keyword case-insensitively equal to `key[..keylen]`, or -1.
unsafe extern "C" fn sql_keyword_hash(key: *const c_void, keylen: usize) -> c_int {
    let key = key as *const u8;
    let (mut lo, mut hi): (i32, i32) = (0, NumScanKeywords - 1);
    while lo <= hi {
        let mid = (lo + hi) / 2;
        let kw = SCANKEYWORDS_KW_STRING
            .as_ptr()
            .add(SCANKEYWORDS_KW_OFFSETS[mid as usize] as usize);
        let mut cmp: i32 = 0;
        let mut j = 0usize;
        loop {
            let kc: u8 = if j < keylen {
                let c = *key.add(j);
                if c >= b'A' && c <= b'Z' { c + (b'a' - b'A') } else { c }
            } else { 0 };
            let wc = *kw.add(j);
            if kc != wc { cmp = kc as i32 - wc as i32; break; }
            if kc == 0 { break; }
            j += 1;
        }
        if cmp == 0 { return mid; }
        else if cmp < 0 { hi = mid - 1; } else { lo = mid + 1; }
    }
    -1
}

/// The standard SQL keyword list (parser/kwlist.h).
pub static ScanKeywords: ScanKeywordList = ScanKeywordList {
    kw_string: SCANKEYWORDS_KW_STRING.as_ptr() as *const c_char,
    kw_offsets: SCANKEYWORDS_KW_OFFSETS.as_ptr(),
    hash: Some(sql_keyword_hash),
    num_keywords: 494,
    max_kw_len: 17,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kwlookup::ScanKeywordLookup;
    #[test]
    fn lookup_sql_keywords() {
        unsafe {
            // case-insensitive hits
            assert!(ScanKeywordLookup(c"select".as_ptr(), &ScanKeywords) >= 0);
            assert!(ScanKeywordLookup(c"SELECT".as_ptr(), &ScanKeywords) >= 0);
            assert!(ScanKeywordLookup(c"WhErE".as_ptr(), &ScanKeywords) >= 0);
            // a reserved keyword has category RESERVED_KEYWORD
            let i = ScanKeywordLookup(c"select".as_ptr(), &ScanKeywords);
            assert_eq!(ScanKeywordCategories[i as usize] as c_int, RESERVED_KEYWORD);
            // non-keyword misses
            assert_eq!(ScanKeywordLookup(c"notakeyword_xyz".as_ptr(), &ScanKeywords), -1);
            assert_eq!(ScanKeywordLookup(c"mytable".as_ptr(), &ScanKeywords), -1);
        }
    }
}
