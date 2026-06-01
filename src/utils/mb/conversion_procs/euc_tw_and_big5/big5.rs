/*
 * conversion between BIG5 and Mule Internal Code(CNS 116643-1992
 * plane 1 and plane 2).
 * This program is partially copied from lv(Multilingual file viewer)
 * and slightly modified. lv is written and copyrighted by NARITA Tomio
 * (nrt@web.ad.jp).
 *
 * 1999/1/15 Tatsuo Ishii
 *
 * src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c
 */

/* can be used in either frontend or backend */

use crate::mb::pg_wchar::{LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, LC_CNS11643_4};

#[derive(Clone, Copy)]
struct codes_t {
    code: u16,
    peer: u16,
}

// helper to keep the table literals close to the C struct-initializer form
const fn c(code: u16, peer: u16) -> codes_t {
    codes_t { code, peer }
}

/* map Big5 Level 1 to CNS 11643-1992 Plane 1 */
static big5Level1ToCnsPlane1: [codes_t; 25] = [
    /* range */
    c(0xA140, 0x2121),
    c(0xA1F6, 0x2258),
    c(0xA1F7, 0x2257),
    c(0xA1F8, 0x2259),
    c(0xA2AF, 0x2421),
    c(0xA3C0, 0x4221),
    c(0xa3e1, 0x0000),
    c(0xA440, 0x4421),
    c(0xACFE, 0x5753),
    c(0xacff, 0x0000),
    c(0xAD40, 0x5323),
    c(0xAFD0, 0x5754),
    c(0xBBC8, 0x6B51),
    c(0xBE52, 0x6B50),
    c(0xBE53, 0x6F5C),
    c(0xC1AB, 0x7536),
    c(0xC2CB, 0x7535),
    c(0xC2CC, 0x7737),
    c(0xC361, 0x782E),
    c(0xC3B9, 0x7865),
    c(0xC3BA, 0x7864),
    c(0xC3BB, 0x7866),
    c(0xC456, 0x782D),
    c(0xC457, 0x7962),
    c(0xc67f, 0x0000),
];

/* map CNS 11643-1992 Plane 1 to Big5 Level 1 */
static cnsPlane1ToBig5Level1: [codes_t; 26] = [
    /* range */
    c(0x2121, 0xA140),
    c(0x2257, 0xA1F7),
    c(0x2258, 0xA1F6),
    c(0x2259, 0xA1F8),
    c(0x234f, 0x0000),
    c(0x2421, 0xA2AF),
    c(0x2571, 0x0000),
    c(0x4221, 0xA3C0),
    c(0x4242, 0x0000),
    c(0x4421, 0xA440),
    c(0x5323, 0xAD40),
    c(0x5753, 0xACFE),
    c(0x5754, 0xAFD0),
    c(0x6B50, 0xBE52),
    c(0x6B51, 0xBBC8),
    c(0x6F5C, 0xBE53),
    c(0x7535, 0xC2CB),
    c(0x7536, 0xC1AB),
    c(0x7737, 0xC2CC),
    c(0x782D, 0xC456),
    c(0x782E, 0xC361),
    c(0x7864, 0xC3BA),
    c(0x7865, 0xC3B9),
    c(0x7866, 0xC3BB),
    c(0x7962, 0xC457),
    c(0x7d4c, 0x0000),
];

/* map Big5 Level 2 to CNS 11643-1992 Plane 2 */
static big5Level2ToCnsPlane2: [codes_t; 48] = [
    /* range */
    c(0xC940, 0x2121),
    c(0xc94a, 0x0000),
    c(0xC94B, 0x212B),
    c(0xC96C, 0x214D),
    c(0xC9BE, 0x214C),
    c(0xC9BF, 0x217D),
    c(0xC9ED, 0x224E),
    c(0xCAF7, 0x224D),
    c(0xCAF8, 0x2439),
    c(0xD77A, 0x3F6A),
    c(0xD77B, 0x387E),
    c(0xDBA7, 0x3F6B),
    c(0xDDFC, 0x4176),
    c(0xDDFD, 0x4424),
    c(0xE8A3, 0x554C),
    c(0xE976, 0x5723),
    c(0xEB5B, 0x5A29),
    c(0xEBF1, 0x554B),
    c(0xEBF2, 0x5B3F),
    c(0xECDE, 0x5722),
    c(0xECDF, 0x5C6A),
    c(0xEDAA, 0x5D75),
    c(0xEEEB, 0x642F),
    c(0xEEEC, 0x6039),
    c(0xF056, 0x5D74),
    c(0xF057, 0x6243),
    c(0xF0CB, 0x5A28),
    c(0xF0CC, 0x6337),
    c(0xF163, 0x6430),
    c(0xF16B, 0x6761),
    c(0xF16C, 0x6438),
    c(0xF268, 0x6934),
    c(0xF269, 0x6573),
    c(0xF2C3, 0x664E),
    c(0xF375, 0x6762),
    c(0xF466, 0x6935),
    c(0xF4B5, 0x664D),
    c(0xF4B6, 0x6962),
    c(0xF4FD, 0x6A4C),
    c(0xF663, 0x6A4B),
    c(0xF664, 0x6C52),
    c(0xF977, 0x7167),
    c(0xF9C4, 0x7166),
    c(0xF9C5, 0x7234),
    c(0xF9C6, 0x7240),
    c(0xF9C7, 0x7235),
    c(0xF9D2, 0x7241),
    c(0xf9d6, 0x0000),
];

/* map CNS 11643-1992 Plane 2 to Big5 Level 2 */
static cnsPlane2ToBig5Level2: [codes_t; 49] = [
    /* range */
    c(0x2121, 0xC940),
    c(0x212B, 0xC94B),
    c(0x214C, 0xC9BE),
    c(0x214D, 0xC96C),
    c(0x217D, 0xC9BF),
    c(0x224D, 0xCAF7),
    c(0x224E, 0xC9ED),
    c(0x2439, 0xCAF8),
    c(0x387E, 0xD77B),
    c(0x3F6A, 0xD77A),
    c(0x3F6B, 0xDBA7),
    c(0x4424, 0x0000),
    c(0x4176, 0xDDFC),
    c(0x4177, 0x0000),
    c(0x4424, 0xDDFD),
    c(0x554B, 0xEBF1),
    c(0x554C, 0xE8A3),
    c(0x5722, 0xECDE),
    c(0x5723, 0xE976),
    c(0x5A28, 0xF0CB),
    c(0x5A29, 0xEB5B),
    c(0x5B3F, 0xEBF2),
    c(0x5C6A, 0xECDF),
    c(0x5D74, 0xF056),
    c(0x5D75, 0xEDAA),
    c(0x6039, 0xEEEC),
    c(0x6243, 0xF057),
    c(0x6337, 0xF0CC),
    c(0x642F, 0xEEEB),
    c(0x6430, 0xF163),
    c(0x6438, 0xF16C),
    c(0x6573, 0xF269),
    c(0x664D, 0xF4B5),
    c(0x664E, 0xF2C3),
    c(0x6761, 0xF16B),
    c(0x6762, 0xF375),
    c(0x6934, 0xF268),
    c(0x6935, 0xF466),
    c(0x6962, 0xF4B6),
    c(0x6A4B, 0xF663),
    c(0x6A4C, 0xF4FD),
    c(0x6C52, 0xF664),
    c(0x7166, 0xF9C4),
    c(0x7167, 0xF977),
    c(0x7234, 0xF9C5),
    c(0x7235, 0xF9C7),
    c(0x7240, 0xF9C6),
    c(0x7241, 0xF9D2),
    c(0x7245, 0x0000),
];

/* Big Five Level 1 Correspondence to CNS 11643-1992 Plane 4 */
static b1c4: [[u16; 2]; 4] = [
    [0xC879, 0x2123],
    [0xC87B, 0x2124],
    [0xC87D, 0x212A],
    [0xC8A2, 0x2152],
];

/* Big Five Level 2 Correspondence to CNS 11643-1992 Plane 3 */
static b2c3: [[u16; 2]; 7] = [
    [0xF9D6, 0x4337],
    [0xF9D7, 0x4F50],
    [0xF9D8, 0x444E],
    [0xF9D9, 0x504A],
    [0xF9DA, 0x2C5D],
    [0xF9DB, 0x3D7E],
    [0xF9DC, 0x4B5C],
];

fn BinarySearchRange(array: &[codes_t], high: i32, code: u16) -> u16 {
    let mut low: i32;
    let mut mid: i32;
    let mut distance: i32;
    let mut tmp: i32;
    let mut high = high;

    low = 0;
    mid = high >> 1;

    while low <= high {
        let m = mid as usize;
        if (array[m].code <= code) && (array[m + 1].code > code) {
            if 0 == array[m].peer {
                return 0;
            }
            if code >= 0xa140u16 {
                /* big5 to cns */
                tmp = (((code as i32) & 0xff00) - ((array[m].code as i32) & 0xff00)) >> 8;
                high = (code as i32) & 0x00ff;
                low = (array[m].code as i32) & 0x00ff;

                /*
                 * NOTE: big5 high_byte: 0xa1-0xfe, low_byte: 0x40-0x7e,
                 * 0xa1-0xfe (radicals: 0x00-0x3e, 0x3f-0x9c) big5 radix is
                 * 0x9d.                     [region_low, region_high] We
                 * should remember big5 has two different regions (above).
                 * There is a bias for the distance between these regions.
                 * 0xa1 - 0x7e + bias = 1 (Distance between 0xa1 and 0x7e is
                 * 1.) bias = - 0x22.
                 */
                distance = tmp * 0x9d + high - low
                    + if high >= 0xa1 {
                        if low >= 0xa1 { 0 } else { -0x22 }
                    } else if low >= 0xa1 {
                        0x22
                    } else {
                        0
                    };

                /*
                 * NOTE: we have to convert the distance into a code point.
                 * The code point's low_byte is 0x21 plus mod_0x5e. In the
                 * first, we extract the mod_0x5e of the starting code point,
                 * subtracting 0x21, and add distance to it. Then we calculate
                 * again mod_0x5e of them, and restore the final codepoint,
                 * adding 0x21.
                 */
                tmp = ((array[m].peer as i32) & 0x00ff) + distance - 0x21;
                tmp = ((array[m].peer as i32) & 0xff00) + ((tmp / 0x5e) << 8)
                    + 0x21 + tmp % 0x5e;
                return tmp as u16;
            } else {
                /* cns to big5 */
                tmp = (((code as i32) & 0xff00) - ((array[m].code as i32) & 0xff00)) >> 8;

                /*
                 * NOTE: ISO charsets ranges between 0x21-0xfe (94charset).
                 * Its radix is 0x5e. But there is no distance bias like big5.
                 */
                distance = tmp * 0x5e
                    + (((code as i32) & 0x00ff) - ((array[m].code as i32) & 0x00ff));

                /*
                 * NOTE: Similar to big5 to cns conversion, we extract
                 * mod_0x9d and restore mod_0x9d into a code point.
                 */
                low = (array[m].peer as i32) & 0x00ff;
                tmp = low + distance - if low >= 0xa1 { 0x62 } else { 0x40 };
                low = tmp % 0x9d;
                tmp = ((array[m].peer as i32) & 0xff00) + ((tmp / 0x9d) << 8)
                    + if low > 0x3e { 0x62 } else { 0x40 } + low;
                return tmp as u16;
            }
        } else if array[m].code > code {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
        mid = (low + high) >> 1;
    }

    0
}

pub fn BIG5toCNS(big5: u16, lc: *mut u8) -> u16 {
    let mut cns: u16 = 0;
    let mut i: usize;

    if big5 < 0xc940u16 {
        /* level 1 */

        i = 0;
        while i < b1c4.len() {
            if b1c4[i][0] == big5 {
                unsafe { *lc = LC_CNS11643_4 as u8 };
                return b1c4[i][1] | 0x8080u16;
            }
            i += 1;
        }

        cns = BinarySearchRange(&big5Level1ToCnsPlane1, 23, big5);
        if cns > 0 {
            unsafe { *lc = LC_CNS11643_1 as u8 };
        }
    } else if big5 == 0xc94au16 {
        /* level 2 */
        unsafe { *lc = LC_CNS11643_1 as u8 };
        cns = 0x4442;
    } else {
        /* level 2 */
        i = 0;
        while i < b2c3.len() {
            if b2c3[i][0] == big5 {
                unsafe { *lc = LC_CNS11643_3 as u8 };
                return b2c3[i][1] | 0x8080u16;
            }
            i += 1;
        }

        cns = BinarySearchRange(&big5Level2ToCnsPlane2, 46, big5);
        if cns > 0 {
            unsafe { *lc = LC_CNS11643_2 as u8 };
        }
    }

    if 0 == cns {
        /* no mapping Big5 to CNS 11643-1992 */
        unsafe { *lc = 0 };
        return b'?' as u16;
    }

    cns | 0x8080
}

pub fn CNStoBIG5(cns: u16, lc: u8) -> u16 {
    let mut i: usize;
    let mut big5: u16 = 0;

    let cns = cns & 0x7f7f;

    match lc as i32 {
        LC_CNS11643_1 => {
            big5 = BinarySearchRange(&cnsPlane1ToBig5Level1, 24, cns);
        }
        LC_CNS11643_2 => {
            big5 = BinarySearchRange(&cnsPlane2ToBig5Level2, 47, cns);
        }
        LC_CNS11643_3 => {
            i = 0;
            while i < b2c3.len() {
                if b2c3[i][1] == cns {
                    return b2c3[i][0];
                }
                i += 1;
            }
        }
        LC_CNS11643_4 => {
            i = 0;
            while i < b1c4.len() {
                if b1c4[i][1] == cns {
                    return b1c4[i][0];
                }
                i += 1;
            }
        }
        _ => {}
    }
    big5
}
