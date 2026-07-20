// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};

use crate::buf_gen::{
    buf_acct_num, buf_branch, buf_digits, buf_email, buf_medicare, buf_office_phone, buf_pan,
    buf_passport, buf_phone, buf_ssn, buf_ssn_last4, build_string_array, bytes_strings,
};
use crate::context::Context;
use crate::rng::Rng;

// ── Type alias ────────────────────────────────────────────────────────────

pub type TemplateFn = fn(usize, &mut Rng, &Context) -> ArrayRef;

// ── Helpers ───────────────────────────────────────────────────────────────

/// Write `val` zero-padded to `width` digits into `buf`. If `val` needs
/// more than `width` digits, the high digits are silently dropped (only
/// the low `width` digits are written) — flag that loudly instead of
/// letting it corrupt data unnoticed.
pub(crate) fn write_zpad(buf: &mut Vec<u8>, val: usize, width: usize) {
    let start = buf.len();
    buf.resize(start + width, b'0');
    let mut v = val;
    for i in (start..start + width).rev() {
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    debug_assert!(
        v == 0,
        "write_zpad: value {val} overflows width {width} digits (high digits truncated)"
    );
    if v != 0 {
        log::warn!(
            "write_zpad: value {val} overflows width {width} digits (high digits truncated)"
        );
    }
}

// ── Template generators ───────────────────────────────────────────────────

fn gen_email(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    buf_email(n, rng)
}

fn gen_phone(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_phone(n, rng, ctx)
}

fn gen_ssn(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_ssn(n, rng, ctx)
}

fn gen_pan(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_pan(n, rng, ctx)
}

fn gen_medicare(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_medicare(n, rng, ctx)
}

fn gen_office_phone(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_office_phone(n, rng, ctx)
}

fn gen_passport(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_passport(n, rng, ctx)
}

fn gen_ssn_last4(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    buf_ssn_last4(n, rng)
}

fn gen_acct_num(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    buf_acct_num(n, rng, ctx)
}

fn gen_branch(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    buf_branch(n, rng)
}

fn gen_street(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let streets = ctx
        .pool_store
        .get("street_names")
        .expect("pool 'street_names' not found");
    let suffixes = [
        "St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Cir",
    ];
    build_string_array(n, 32, |buf| {
        let num = rng.next_usize(9900) + 100; // 100-9999
        write_zpad(buf, num, 4);
        buf.push(b' ');
        let street = streets[rng.next_usize(streets.len())].as_str();
        buf.extend_from_slice(street.as_bytes());
        buf.push(b' ');
        let suffix = suffixes[rng.next_usize(suffixes.len())];
        buf.extend_from_slice(suffix.as_bytes());
    })
}

fn gen_url(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let first = ctx
        .pool_store
        .get("first_name")
        .expect("pool 'first_name' not found");
    let last = ctx
        .pool_store
        .get("last_name")
        .expect("pool 'last_name' not found");
    // Index the union of the two pools directly instead of materializing a
    // concatenated `Vec<&str>` (a copy of every pointer in both pools) on
    // every call — `k < first.len()` picks the same element `names[k]`
    // would have under `first.iter().chain(last.iter())`.
    let total = first.len() + last.len();
    let tlds = [".com", ".org", ".net", ".io"];
    build_string_array(n, 28, |buf| {
        buf.extend_from_slice(b"www.");
        let k = rng.next_usize(total);
        let name = if k < first.len() {
            first[k].as_bytes()
        } else {
            last[k - first.len()].as_bytes()
        };
        for c in name {
            buf.push(c.to_ascii_lowercase());
        }
        let tld = tlds[rng.next_usize(tlds.len())];
        buf.extend_from_slice(tld.as_bytes());
    })
}

fn gen_username(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let prefixes = ["user", "admin", "guest", "member", "player", "fan"];
    build_string_array(n, 12, |buf| {
        let p = prefixes[rng.next_usize(prefixes.len())];
        buf.extend_from_slice(p.as_bytes());
        let num = rng.next_usize(9900) + 100;
        write_zpad(buf, num, 4);
    })
}

fn gen_version(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 6, |buf| {
        let maj = rng.next_usize(4) + 1;
        let min = rng.next_usize(10);
        let pat = rng.next_usize(10);
        write_zpad(buf, maj, 1);
        buf.push(b'.');
        write_zpad(buf, min, 1);
        buf.push(b'.');
        write_zpad(buf, pat, 1);
    })
}

fn gen_linkedin(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let first = ctx
        .pool_store
        .get("first_name")
        .expect("pool 'first_name' not found");
    let last = ctx
        .pool_store
        .get("last_name")
        .expect("pool 'last_name' not found");
    // See `gen_url` above: index the union directly, no concatenated Vec.
    let total = first.len() + last.len();
    build_string_array(n, 48, |buf| {
        buf.extend_from_slice(b"https://linkedin.com/in/");
        let k = rng.next_usize(total);
        let name = if k < first.len() {
            first[k].as_bytes()
        } else {
            last[k - first.len()].as_bytes()
        };
        for c in name {
            buf.push(c.to_ascii_lowercase());
        }
        buf.push(b'-');
        let num = rng.next_usize(900) + 100;
        write_zpad(buf, num, 3);
    })
}

fn gen_mid_init(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 1, |buf| {
        buf.push(b'A' + rng.next_usize(26) as u8);
    })
}

fn gen_conf_code(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    bytes_strings(b"ABCDEFGHJKLMNPRSTUVWXYZ0123456789", n, 8, rng)
}

fn gen_acct_name(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let prefixes = [
        "Global", "Premier", "Advanced", "First", "United", "National", "Pacific", "Atlantic",
        "Summit", "Meridian",
    ];
    let suffixes = [
        "Solutions",
        "Group",
        "Partners",
        "Industries",
        "Enterprises",
        "Technologies",
        "Services",
        "Consulting",
        "Logistics",
        "Ventures",
    ];
    build_string_array(n, 24, |buf| {
        let p = prefixes[rng.next_usize(prefixes.len())];
        buf.extend_from_slice(p.as_bytes());
        buf.push(b' ');
        let s = suffixes[rng.next_usize(suffixes.len())];
        buf.extend_from_slice(s.as_bytes());
    })
}

fn gen_doc_num(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let prefixes = ctx
        .pool_store
        .get("document_number_prefixes")
        .expect("pool 'document_number_prefixes' not found");
    build_string_array(n, 12, |buf| {
        let p = prefixes[rng.next_usize(prefixes.len())].as_str();
        buf.extend_from_slice(p.as_bytes());
        buf.push(b'-');
        let num = rng.next_usize(9000000) + 1000000;
        write_zpad(buf, num, 7);
    })
}

fn gen_npi(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        buf.push(b'1');
        let num = rng.next_usize(90000000) + 10000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_license(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 8, |buf| {
        buf.push(b'A' + rng.next_usize(20) as u8);
        let num = rng.next_usize(900000) + 100000;
        write_zpad(buf, num, 6);
    })
}

fn gen_ip(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 15, |buf| {
        let o1 = rng.next_usize(254) + 1;
        let o2 = rng.next_usize(254) + 1;
        let o3 = rng.next_usize(254) + 1;
        let o4 = rng.next_usize(254) + 1;
        write_zpad(buf, o1, 1);
        buf.push(b'.');
        write_zpad(buf, o2, 1);
        buf.push(b'.');
        write_zpad(buf, o3, 1);
        buf.push(b'.');
        write_zpad(buf, o4, 1);
    })
}

fn gen_barcode(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x424152);
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.next_usize(9000000000000) as u64 + 1000000000000);
    }
    buf_digits(&nums, 14, Some(wm))
}

fn gen_sku(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 8, |buf| {
        buf.push(b'A' + rng.next_usize(26) as u8);
        buf.push(b'-');
        let num = rng.next_usize(99000) + 1000;
        write_zpad(buf, num, 5);
    })
}

fn gen_cc(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.next_usize(9000) as u64 + 1000);
    }
    buf_digits(&nums, 4, None)
}

fn gen_reg(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        let l1 = b'A' + rng.next_usize(20) as u8;
        let l2 = b'A' + rng.next_usize(20) as u8;
        buf.push(l1);
        buf.push(l2);
        buf.push(b'-');
        let num = rng.next_usize(90000) + 10000;
        write_zpad(buf, num, 5);
    })
}

fn gen_variant(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let sizes = ["Small", "Medium", "Large", "XL", "One Size"];
    let colors = ["Black", "White", "Red", "Blue", "Green", "Gray"];
    build_string_array(n, 14, |buf| {
        let s = sizes[rng.next_usize(sizes.len())];
        let c = colors[rng.next_usize(colors.len())];
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b" / ");
        buf.extend_from_slice(c.as_bytes());
    })
}

fn gen_order_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let prefixes = ["ORD", "INV", "SUB"];
    build_string_array(n, 13, |buf| {
        buf.push(b'#');
        let p = prefixes[rng.next_usize(prefixes.len())];
        buf.extend_from_slice(p.as_bytes());
        buf.push(b'-');
        let num = rng.next_usize(900000) + 100000;
        write_zpad(buf, num, 6);
    })
}

fn gen_vin(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    bytes_strings(b"ABCDEFGHJKLMNPRSTUVWXYZ0123456789", n, 17, rng)
}

fn gen_routing(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        let a = rng.next_usize(9) * 10000000 + 21000000;
        let b = rng.next_usize(9000000) + 1000000;
        write_zpad(buf, a + b, 9);
    })
}

fn gen_serial(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 13, |buf| {
        buf.extend_from_slice(b"MTR-");
        let num = rng.next_usize(90000000) + 10000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_inv_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 11, |buf| {
        buf.extend_from_slice(b"INV-");
        let num = rng.next_usize(900000) + 100000;
        write_zpad(buf, num, 6);
    })
}

fn gen_jersey(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.next_usize(99) as u64);
    }
    buf_digits(&nums, 2, None)
}

fn gen_plate(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 8, |buf| {
        for _ in 0..3 {
            buf.push(b'A' + rng.next_usize(26) as u8);
        }
        buf.push(b'-');
        let num = rng.next_usize(9900) + 100;
        write_zpad(buf, num, 4);
    })
}

fn gen_imei(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 15, |buf| {
        let g1 = rng.next_usize(90000000) + 10000000;
        let g2 = rng.next_usize(9000000) + 1000000;
        write_zpad(buf, g1, 8);
        write_zpad(buf, g2, 7);
    })
}

fn gen_imsi(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 15, |buf| {
        let g1 = rng.next_usize(990) + 10;
        let g2 = rng.next_usize(9000000000) + 1000000000;
        buf.extend_from_slice(b"310");
        write_zpad(buf, g1, 3);
        write_zpad(buf, g2, 10);
    })
}

fn gen_iccid(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x494343);
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.next_usize(900000000000000000) as u64 + 100000000000000000);
    }
    buf_digits(&nums, 18, Some(wm))
}

fn gen_policy_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        buf.push(b'A' + rng.next_usize(20) as u8);
        let num = rng.next_usize(90000000) + 10000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_bol(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 13, |buf| {
        buf.extend_from_slice(b"BOL-");
        buf.push(b'A' + rng.next_usize(20) as u8);
        let num = rng.next_usize(90000000) + 10000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_tracking(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let alnum = b"ABCDEFGHJKLMNPRSTUVWXYZ0123456789";
    build_string_array(n, 18, |buf| {
        buf.extend_from_slice(b"1Z");
        for _ in 0..16 {
            buf.push(alnum[rng.next_usize(alnum.len())]);
        }
    })
}

fn gen_scac(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 4, |buf| {
        for _ in 0..4 {
            buf.push(b'A' + rng.next_usize(26) as u8);
        }
    })
}

fn gen_apn(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 12, |buf| {
        let g1 = rng.next_usize(900) + 100;
        let g2 = rng.next_usize(9000) + 1000;
        let g3 = rng.next_usize(900) + 100;
        write_zpad(buf, g1, 3);
        buf.push(b'-');
        write_zpad(buf, g2, 4);
        buf.push(b'-');
        write_zpad(buf, g3, 3);
    })
}

fn gen_mls(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 11, |buf| {
        buf.extend_from_slice(b"MLS");
        let num = rng.next_usize(90000000) + 10000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_upc(n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let wm = ctx.watermark_3digits(0x555043);
    let mut nums = Vec::with_capacity(n);
    for _ in 0..n {
        nums.push(rng.next_usize(900000000000) as u64 + 100000000000);
    }
    buf_digits(&nums, 12, Some(wm))
}

fn gen_case_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 16, |buf| {
        buf.extend_from_slice(b"CASE-2024-");
        let num = rng.next_usize(90000) + 10000;
        write_zpad(buf, num, 5);
    })
}

fn gen_court_room(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let wings = ["A", "B", "C", "D", "E", "N", "S", "W"];
    build_string_array(n, 10, |buf| {
        buf.extend_from_slice(b"Room ");
        let w = wings[rng.next_usize(wings.len())];
        buf.extend_from_slice(w.as_bytes());
        let num = rng.next_usize(400) + 100;
        write_zpad(buf, num, 3);
    })
}

fn gen_orcid(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let alnum = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    build_string_array(n, 19, |buf| {
        buf.extend_from_slice(b"0000-");
        for _ in 0..3 {
            for _ in 0..4 {
                buf.push(alnum[rng.next_usize(alnum.len())]);
            }
            buf.push(b'-');
        }
        for _ in 0..4 {
            buf.push(alnum[rng.next_usize(alnum.len())]);
        }
    })
}

fn gen_doi(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 12, |buf| {
        let prefix = rng.next_usize(9000) + 1000;
        let suffix = rng.next_usize(90000) + 10000;
        buf.extend_from_slice(b"10.");
        write_zpad(buf, prefix, 4);
        buf.push(b'/');
        write_zpad(buf, suffix, 5);
    })
}

fn gen_isbn(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 18, |buf| {
        let g2 = rng.next_usize(90) + 10;
        let g3 = rng.next_usize(900) + 100;
        let g4 = rng.next_usize(90000) + 10000;
        let g5 = rng.next_usize(9);
        buf.extend_from_slice(b"978-");
        write_zpad(buf, g2, 2);
        buf.push(b'-');
        write_zpad(buf, g3, 3);
        buf.push(b'-');
        write_zpad(buf, g4, 5);
        buf.push(b'-');
        write_zpad(buf, g5, 1);
    })
}

fn gen_issn(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        let g1 = rng.next_usize(9000) + 1000;
        let g2 = rng.next_usize(9000) + 1000;
        write_zpad(buf, g1, 4);
        buf.push(b'-');
        write_zpad(buf, g2, 4);
    })
}

fn gen_pages(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        let start = rng.next_usize(499) + 1;
        let span = rng.next_usize(27) + 3;
        let end = start + span;
        write_zpad(buf, start, 1);
        buf.push(b'-');
        write_zpad(buf, end, 1);
    })
}

fn gen_part_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        buf.extend_from_slice(b"PRT-");
        let num = rng.next_usize(90000) + 10000;
        write_zpad(buf, num, 5);
    })
}

fn gen_batch_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 12, |buf| {
        buf.extend_from_slice(b"BATCH-");
        let num = rng.next_usize(90000) + 10000;
        write_zpad(buf, num, 5);
    })
}

fn gen_nct(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 11, |buf| {
        buf.extend_from_slice(b"NCT");
        let num = rng.next_usize(90000000) + 1000000;
        write_zpad(buf, num, 8);
    })
}

fn gen_subj_num(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        buf.extend_from_slice(b"SUB-");
        let num = rng.next_usize(90000) + 10000;
        write_zpad(buf, num, 5);
    })
}

fn gen_dosage(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let units = ["mg", "mcg", "g", "mL", "IU", "mg/kg"];
    build_string_array(n, 10, |buf| {
        let val = rng.next_usize(999) + 1;
        let u = units[rng.next_usize(units.len())];
        write_zpad(buf, val, 1);
        buf.push(b' ');
        buf.extend_from_slice(u.as_bytes());
    })
}

fn gen_lot(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        buf.push(b'L');
        buf.push(b'A' + rng.next_usize(20) as u8);
        buf.push(b'-');
        let num = rng.next_usize(99000) + 1000;
        write_zpad(buf, num, 5);
    })
}

fn gen_po(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        buf.extend_from_slice(b"PO-");
        let num = rng.next_usize(900000) + 100000;
        write_zpad(buf, num, 6);
    })
}

fn gen_duns(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 10, |buf| {
        let g1 = rng.next_usize(90) + 10;
        let g2 = rng.next_usize(900) + 100;
        let g3 = rng.next_usize(9000) + 1000;
        write_zpad(buf, g1, 2);
        buf.push(b'-');
        write_zpad(buf, g2, 3);
        buf.push(b'-');
        write_zpad(buf, g3, 4);
    })
}

fn gen_season(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    build_string_array(n, 9, |buf| {
        let y = rng.next_usize(5) + 2020;
        write_zpad(buf, y, 4);
        buf.push(b'-');
        write_zpad(buf, y + 1, 4);
    })
}

fn gen_power(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let choices = [3, 6, 9, 12, 15, 18, 24, 30, 36];
    build_string_array(n, 7, |buf| {
        let v = choices[rng.next_usize(choices.len())];
        write_zpad(buf, v, 1);
        buf.extend_from_slice(b" kVA");
    })
}

fn gen_suffix(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = ["Jr.", "Sr.", "III", "II", "IV", "MD", "PhD", "Esq."];
    let mut builder = StringBuilder::with_capacity(n, n * 4);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_lead_source(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "website",
        "referral",
        "event",
        "cold_call",
        "email_campaign",
        "partner",
        "social_media",
        "webinar",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 14);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_semester(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "Fall 2024",
        "Spring 2025",
        "Summer 2025",
        "Fall 2025",
        "Spring 2026",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 11);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_grade(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", "W", "I", "P",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 2);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_revenue_range(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "$0-$1M",
        "$1M-$10M",
        "$10M-$50M",
        "$50M-$100M",
        "$100M-$500M",
        "$500M-$1B",
        "$1B+",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 12);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_source_system(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = ["CRM", "ERP", "HRIS", "PORTAL", "LEGACY", "EXTERNAL"];
    let mut builder = StringBuilder::with_capacity(n, n * 8);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_os_version(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "iOS 17.4",
        "Android 14",
        "iOS 16.6",
        "Android 13",
        "HarmonyOS 4",
        "iPadOS 17",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 11);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_reviewer_notes(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = [
        "Good performance this quarter",
        "Needs improvement in communication",
        "Exceeds expectations",
        "Meets all targets",
        "Strong technical skills",
        "Leadership potential noted",
        "Areas for growth identified",
        "Consistent performer",
        "Above average contribution",
        "Shows initiative and drive",
    ];
    let mut builder = StringBuilder::with_capacity(n, n * 38);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_currency(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY", "CHF", "CNY"];
    let mut builder = StringBuilder::with_capacity(n, n * 3);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

fn gen_address_type(n: usize, rng: &mut Rng, _ctx: &Context) -> ArrayRef {
    let pool = ["shipping", "billing", "home", "work", "mailing"];
    let mut builder = StringBuilder::with_capacity(n, n * 8);
    for _ in 0..n {
        builder.append_value(pool[rng.next_usize(pool.len())]);
    }
    Arc::new(builder.finish())
}

// ── Registry ──────────────────────────────────────────────────────────────

/// Get the template generator for a column name, or None.
pub fn get_template(name: &str) -> Option<TemplateFn> {
    REGISTRY.get(name).copied()
}

use std::collections::HashMap;

static REGISTRY: LazyLock<HashMap<&'static str, TemplateFn>> = LazyLock::new(|| {
    let mut m: HashMap<&'static str, TemplateFn> = HashMap::new();
    // Email
    for k in &[
        "email",
        "email_address",
        "business_email",
        "personal_email",
        "support_email",
        "owner_email",
    ] {
        m.insert(*k, gen_email);
    }
    // Phone
    for k in &[
        "phone_number",
        "mobile_phone",
        "home_phone",
        "prov_phone",
        "phone",
        "support_phone",
        "destination_number",
    ] {
        m.insert(*k, gen_phone);
    }
    // SSN / tax_id / national_id
    for k in &["ssn", "tax_id", "national_id"] {
        m.insert(*k, gen_ssn);
    }
    // Medicare
    for k in &["medicare_num", "medicaid_num"] {
        m.insert(*k, gen_medicare);
    }
    // Office phone
    m.insert("office_phone", gen_office_phone);
    // Passport
    for k in &["passport_number", "passport"] {
        m.insert(*k, gen_passport);
    }
    // SSN last 4
    m.insert("ssn_last4", gen_ssn_last4);
    // PAN
    m.insert("personal_administrative_number", gen_pan);
    // Account number
    m.insert("account_number", gen_acct_num);
    // Branch code
    m.insert("branch_code", gen_branch);
    // Street address
    for k in &[
        "street_address",
        "address_line1",
        "address",
        "residential_address_line1",
        "registered_address_line1",
        "shipping_address",
        "billing_address",
    ] {
        m.insert(*k, gen_street);
    }
    // URL
    for k in &["website", "url", "website_url", "avatar_url"] {
        m.insert(*k, gen_url);
    }
    // Username
    m.insert("username", gen_username);
    // Version
    m.insert("version", gen_version);
    // LinkedIn
    m.insert("linkedin_url", gen_linkedin);
    // Middle initial
    m.insert("middle_initial", gen_mid_init);
    // Confirmation code
    {
        let k = &"confirmation_code";
        m.insert(*k, gen_conf_code);
    }
    // Account name
    m.insert("account_name", gen_acct_name);
    // Document number
    m.insert("document_number", gen_doc_num);
    // NPI
    m.insert("npi", gen_npi);
    // License / bar number / driver license
    for k in &[
        "bar_number",
        "license_number",
        "drivers_license_number",
        "npn",
    ] {
        m.insert(*k, gen_license);
    }
    // IP address
    m.insert("ip_address", gen_ip);
    // Barcode
    m.insert("barcode", gen_barcode);
    // SKU
    m.insert("sku", gen_sku);
    // CC last 4
    m.insert("cc_last_four", gen_cc);
    // Registration/group number
    for k in &["registration_number", "group_number"] {
        m.insert(*k, gen_reg);
    }
    // Variant title
    m.insert("variant_title", gen_variant);
    // Order number
    {
        let k = &"order_number";
        m.insert(*k, gen_order_num);
    }
    // VIN
    m.insert("vin", gen_vin);
    // Routing number
    m.insert("routing_number", gen_routing);
    // Serial / meter serial
    for k in &["meter_serial", "serial_number"] {
        m.insert(*k, gen_serial);
    }
    // Invoice number
    {
        let k = &"invoice_number";
        m.insert(*k, gen_inv_num);
    }
    // Jersey number
    {
        let k = &"jersey_number";
        m.insert(*k, gen_jersey);
    }
    // License plate
    {
        let k = &"license_plate";
        m.insert(*k, gen_plate);
    }
    // IMEI
    {
        let k = &"imei";
        m.insert(*k, gen_imei);
    }
    // IMSI
    {
        let k = &"imsi";
        m.insert(*k, gen_imsi);
    }
    // ICCID
    {
        let k = &"iccid";
        m.insert(*k, gen_iccid);
    }
    // Policy / permit number
    for k in &["policy_number", "permit_number"] {
        m.insert(*k, gen_policy_num);
    }
    // BOL number
    {
        let k = &"bol_number";
        m.insert(*k, gen_bol);
    }
    // Tracking number
    {
        let k = &"tracking_number";
        m.insert(*k, gen_tracking);
    }
    // SCAC code
    {
        let k = &"scac_code";
        m.insert(*k, gen_scac);
    }
    // Parcel number / APN
    m.insert("parcel_number", gen_apn);
    // MLS number
    {
        let k = &"mls_number";
        m.insert(*k, gen_mls);
    }
    // UPC
    {
        let k = &"upc";
        m.insert(*k, gen_upc);
    }
    // Case number
    {
        let k = &"case_number";
        m.insert(*k, gen_case_num);
    }
    // Court room
    {
        let k = &"court_room";
        m.insert(*k, gen_court_room);
    }
    // ORCID
    {
        let k = &"orcid";
        m.insert(*k, gen_orcid);
    }
    // DOI
    {
        let k = &"doi";
        m.insert(*k, gen_doi);
    }
    // ISBN
    {
        let k = &"isbn";
        m.insert(*k, gen_isbn);
    }
    // ISSN
    {
        let k = &"issn";
        m.insert(*k, gen_issn);
    }
    // Pages
    {
        let k = &"pages";
        m.insert(*k, gen_pages);
    }
    // Part number
    {
        let k = &"part_number";
        m.insert(*k, gen_part_num);
    }
    // Batch number
    {
        let k = &"batch_number";
        m.insert(*k, gen_batch_num);
    }
    // NCT number
    {
        let k = &"nct_number";
        m.insert(*k, gen_nct);
    }
    // Subject number
    {
        let k = &"subject_number";
        m.insert(*k, gen_subj_num);
    }
    // Dosage
    {
        let k = &"dosage";
        m.insert(*k, gen_dosage);
    }
    // Lot number
    {
        let k = &"lot_number";
        m.insert(*k, gen_lot);
    }
    // PO number
    {
        let k = &"po_number";
        m.insert(*k, gen_po);
    }
    // DUNS number
    {
        let k = &"duns_number";
        m.insert(*k, gen_duns);
    }
    // Season
    {
        let k = &"season";
        m.insert(*k, gen_season);
    }
    // Contracted power
    m.insert("contracted_power", gen_power);
    // Suffix
    m.insert("suffix", gen_suffix);
    // Lead source
    m.insert("lead_source", gen_lead_source);
    // Semester
    m.insert("semester", gen_semester);
    // Grade
    m.insert("grade", gen_grade);
    // Revenue range
    m.insert("revenue_range", gen_revenue_range);
    // Source system
    m.insert("source_system", gen_source_system);
    // OS version
    m.insert("os_version", gen_os_version);
    // Reviewer notes
    m.insert("reviewer_notes", gen_reviewer_notes);
    // Currency
    m.insert("currency", gen_currency);
    // Address type
    m.insert("address_type", gen_address_type);
    // Volume
    m.insert("volume", |n, rng, _| {
        let mut builder = StringBuilder::with_capacity(n, n * 2);
        for _ in 0..n {
            builder.append_value((rng.next_usize(50) + 1).to_string());
        }
        Arc::new(builder.finish())
    });
    // Issue
    m.insert("issue", |n, rng, _| {
        let mut builder = StringBuilder::with_capacity(n, n * 2);
        for _ in 0..n {
            builder.append_value((rng.next_usize(12) + 1).to_string());
        }
        Arc::new(builder.finish())
    });
    // Option1, Option2, Option3
    m.insert("option1", |n, rng, _| {
        let pool = ["Small", "Medium", "Large", "XL", "XXL"];
        let mut builder = StringBuilder::with_capacity(n, n * 5);
        for _ in 0..n {
            builder.append_value(pool[rng.next_usize(pool.len())]);
        }
        Arc::new(builder.finish())
    });
    m.insert("option2", |n, rng, _| {
        let pool = ["Black", "White", "Red", "Blue", "Green", "Gray", "Navy"];
        let mut builder = StringBuilder::with_capacity(n, n * 5);
        for _ in 0..n {
            builder.append_value(pool[rng.next_usize(pool.len())]);
        }
        Arc::new(builder.finish())
    });
    m.insert("option3", |n, rng, _| {
        let pool = ["Cotton", "Polyester", "Wool", "Linen", "Silk"];
        let mut builder = StringBuilder::with_capacity(n, n * 10);
        for _ in 0..n {
            builder.append_value(pool[rng.next_usize(pool.len())]);
        }
        Arc::new(builder.finish())
    });
    m
});

use std::sync::LazyLock;

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::Context;
    use arrow::array::AsArray;

    fn test_ctx() -> Context {
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/pools");
        Context::new("kyc", "en", pools_dir.to_str().unwrap()).unwrap()
    }

    fn test_rng() -> Rng {
        Rng::new(42)
    }

    #[test]
    fn test_all_templates_run() {
        let ctx = test_ctx();
        let names: Vec<&str> = REGISTRY.keys().copied().collect();
        assert!(!names.is_empty(), "registry is empty");
        for name in names {
            let mut rng = test_rng();
            let template = get_template(name).expect("template not found");
            let arr = template(10, &mut rng, &ctx);
            assert_eq!(arr.len(), 10, "template '{name}' produced wrong length");
            let s = arr.as_string::<i32>();
            for i in 0..10 {
                let v = s.value(i);
                assert!(!v.is_empty(), "template '{name}' empty at {i}");
            }
        }
    }

    #[test]
    fn test_deterministic() {
        let ctx = test_ctx();
        let name = "phone";
        let template = get_template(name).unwrap();
        let a = template(100, &mut Rng::new(42), &ctx);
        let b = template(100, &mut Rng::new(42), &ctx);
        let a_arr = a.as_string::<i32>();
        let b_arr = b.as_string::<i32>();
        for i in 0..100 {
            assert_eq!(a_arr.value(i), b_arr.value(i), "mismatch at {i}");
        }
    }

    #[test]
    fn test_phone_template() {
        let ctx = test_ctx();
        let template = get_template("phone").unwrap();
        let mut rng = test_rng();
        let arr = template(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 15, "phone[{i}] = {v:?}");
            assert!(v.starts_with("+1-"), "phone[{i}] = {v:?}");
        }
    }

    #[test]
    fn test_email_template() {
        let ctx = test_ctx();
        let template = get_template("email").unwrap();
        let mut rng = test_rng();
        let arr = template(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 18, "email[{i}] = {v:?}");
            assert!(v.contains('@'), "email[{i}] = {v:?}");
        }
    }

    #[test]
    fn test_street_template() {
        let ctx = test_ctx();
        let template = get_template("street_address").unwrap();
        let mut rng = test_rng();
        let arr = template(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert!(!v.is_empty(), "street[{i}] empty");
            // Should start with a digit (house number)
            assert!(v.as_bytes()[0].is_ascii_digit(), "street[{i}] = {v:?}");
        }
    }

    #[test]
    fn test_url_template() {
        let ctx = test_ctx();
        let template = get_template("url").unwrap();
        let mut rng = test_rng();
        let arr = template(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert!(v.starts_with("www."), "url[{i}] = {v:?}");
            assert!(
                v.ends_with(".com")
                    || v.ends_with(".org")
                    || v.ends_with(".net")
                    || v.ends_with(".io"),
                "url[{i}] = {v:?}"
            );
        }
    }

    #[test]
    fn test_vin_template() {
        let ctx = test_ctx();
        let template = get_template("vin").unwrap();
        let mut rng = test_rng();
        let arr = template(5, &mut rng, &ctx);
        let s = arr.as_string::<i32>();
        for i in 0..5 {
            let v = s.value(i);
            assert_eq!(v.len(), 17, "vin[{i}] = {v:?}");
        }
    }
}
