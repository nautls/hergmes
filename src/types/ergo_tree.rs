//! The constants and template of a constant-segregated ErgoTree, read without parsing the
//! tree's expression, and the byte collections a register's constant holds.
//!
//! The node indexes boxes by template hash: `IndexedContractTemplateSerializer.hashTreeTemplate`
//! (node 6.0.8) hashes `ErgoTree.template` with Blake2b256, and the template is what follows
//! the header, the size and the constants (sigma-state 6.0.7,
//! `ErgoTreeSerializer.deserializeHeaderWithTreeBytes`). Parsing the whole tree to get there
//! is too much to do for every box, so this walks only the constants section. A constant is
//! a type then a value: the type says how many bytes the value takes, and those bytes are
//! skipped, not decoded.
//!
//! Only trees with segregated constants are read, and only the constants a tree can segregate,
//! which are the ones a register can hold: no Box or Header, which a contract reads from its
//! context and never embeds. Values are not checked (a group element is not decoded to a point,
//! a length is not range-checked against its type), so a tree the node rejects may still be
//! read here.

use blake2::{Blake2b256, Digest as _};

use crate::types::{HashDigest, ergo::Template};

/// The header bit marking segregated constants (sigma-state 6.0.7,
/// `ErgoTree.ConstantSegregationFlag`).
const CONSTANT_SEGREGATION_FLAG: u8 = 0x10;
/// The header bit marking a size after the header (sigma-state 6.0.7, `ErgoTree.SizeFlag`).
const SIZE_FLAG: u8 = 0x08;

/// How deep types, values and sigma propositions may nest, so a crafted tree can't exhaust
/// the stack. The node's own bound on nested values (sigma-state 6.0.7,
/// `SigmaConstants.MaxTreeDepth`).
const MAX_DEPTH: usize = 110;

/// Type codes (sigma-state 6.0.7, `TypeSerializer` and `SType.typeCode`). A code below
/// `TUPLE` is a constructor times `PRIM_RANGE` plus an embedded primitive, 0 meaning the
/// argument type follows.
const PRIM_RANGE: u8 = 12;
const COLL: u8 = PRIM_RANGE;
const PAIR: u8 = 5 * PRIM_RANGE;
const TUPLE: u8 = 96;
const UNIT: u8 = 98;
const AVL_TREE: u8 = 100;

const BOOLEAN: u8 = 1;
const BYTE: u8 = 2;
const SHORT: u8 = 3;
const INT: u8 = 4;
const LONG: u8 = 5;
const BIG_INT: u8 = 6;
const GROUP_ELEMENT: u8 = 7;
const SIGMA_PROP: u8 = 8;
const UNSIGNED_BIG_INT: u8 = 9;

/// A group element: a compressed point, or 33 zero bytes for the identity (sigma-state 6.0.7,
/// `GroupElementSerializer`).
const GROUP_ELEMENT_LEN: usize = 33;
/// An AVL tree's digest: a hash and the tree's height (sigma-state 6.0.7,
/// `AvlTreeData.DigestSize`).
const AVL_DIGEST_LEN: usize = 33;

/// Sigma proposition codes (sigma-state 6.0.7, `SigmaPropCodes`).
const AND: u8 = 0x96;
const OR: u8 = 0x97;
const AT_LEAST: u8 = 0x98;
const PROVE_DLOG: u8 = 0xcd;
const PROVE_DH_TUPLE: u8 = 0xce;
const FALSE_PROP: u8 = 0xd2;
const TRUE_PROP: u8 = 0xd3;

/// A tree's template: its bytes after the constants. `None` if its constants are not
/// segregated, or one can't be read.
pub fn template(tree: &[u8]) -> Option<&[u8]> {
    let start = walk(tree, |_| ())?;
    Some(&tree[start..])
}

/// The most bytes a template can take: a box is at most 4,096 bytes, its tree included
/// (sigma-state 6.0.7, `ErgoBox.MaxBoxSize`), and the template is part of the tree.
pub const MAX_TEMPLATE_BYTES: usize = 4096;

/// A template's bytes from hex, in either case: 1 to [`MAX_TEMPLATE_BYTES`] bytes.
pub fn parse_template(hex: &str) -> Result<Template, String> {
    let bytes = hex::decode(hex).map_err(|e| e.to_string())?;
    match bytes.len() {
        0 => Err("a template is at least one byte".to_owned()),
        n if n > MAX_TEMPLATE_BYTES => {
            Err(format!("a template is at most {MAX_TEMPLATE_BYTES} bytes, got {n}"))
        }
        _ => Ok(Template::from(bytes)),
    }
}

/// The hash the node indexes a tree's boxes by (node 6.0.8,
/// `IndexedContractTemplateSerializer.hashTreeTemplate`). `None` as for [`template`].
pub fn template_hash(tree: &[u8]) -> Option<HashDigest> {
    template(tree).map(hash_template)
}

/// The hash of a template's bytes, as the node indexes it (node 6.0.8,
/// `IndexedContractTemplateSerializer.hashTreeTemplate`).
pub fn hash_template(template: &[u8]) -> HashDigest {
    crate::types::Digest(Blake2b256::digest(template).into())
}

/// A tree's constants, each as its type and value bytes, as a register holds one. `None` as
/// for [`template`].
pub fn constants(tree: &[u8]) -> Option<Vec<&[u8]>> {
    let mut constants = Vec::new();
    walk(tree, |c| constants.push(c))?;
    Some(constants)
}

/// The bytes of a constant that is a `Coll[Byte]`, as a register holds one. `None` if it is
/// of another type, can't be read, or bytes follow it.
pub fn byte_coll(constant: &[u8]) -> Option<&[u8]> {
    let p = Parser::whole(constant)?;
    let data = p.byte_coll_type(0)?;
    Some(p.byte_coll_value(data)?.0)
}

/// The elements of a constant that is a `(Coll[Byte], Coll[Byte])`, written as a pair or as
/// a 2-tuple. `None` as for [`byte_coll`].
pub fn byte_coll_pair(constant: &[u8]) -> Option<(&[u8], &[u8])> {
    let p = Parser::whole(constant)?;
    let first_type = match p.byte(0)? {
        PAIR => 1,
        TUPLE if p.byte(1)? == 2 => 2,
        _ => return None,
    };
    let data = p.byte_coll_type(p.byte_coll_type(first_type)?)?;
    let (first, pos) = p.byte_coll_value(data)?;
    Some((first, p.byte_coll_value(pos)?.0))
}

/// Reads the header, the size and the constants, handing each constant's bytes to
/// `on_constant`, and returns where the template starts.
fn walk<'a>(tree: &'a [u8], mut on_constant: impl FnMut(&'a [u8])) -> Option<usize> {
    let p = Parser { bytes: tree };
    let header = p.byte(0)?;
    if header & CONSTANT_SEGREGATION_FLAG == 0 {
        return None;
    }
    let mut pos = 1;
    if header & SIZE_FLAG != 0 {
        (_, pos) = p.vlq(pos)?;
    }
    let (count, mut pos) = p.vlq(pos)?;
    // Each constant takes at least a byte, so a count past the bytes left fails here.
    for _ in 0..count {
        let start = pos;
        let end = p.constant(pos)?;
        on_constant(&tree[start..end]);
        pos = end;
    }
    Some(pos)
}

/// A type: a primitive embedded in a type code, or the offset of a type's encoding. Types are
/// read from the tree each time they are needed rather than built, so reading constants
/// allocates nothing.
#[derive(Clone, Copy)]
enum Ty {
    Prim(u8),
    At(usize),
}

/// Positions are offsets into `bytes`; each method returns where what it read ends.
struct Parser<'a> {
    bytes: &'a [u8],
}

impl<'a> Parser<'a> {
    /// A parser over `constant`, if it is one readable constant and nothing more.
    fn whole(constant: &'a [u8]) -> Option<Self> {
        let p = Parser { bytes: constant };
        (p.constant(0)? == constant.len()).then_some(p)
    }

    /// Where a `Coll[Byte]` type read at `pos` ends: its element embedded, or in a byte of its
    /// own. `None` for any other type.
    fn byte_coll_type(&self, pos: usize) -> Option<usize> {
        match self.byte(pos)? {
            code if code == COLL + BYTE => Some(pos + 1),
            COLL if self.byte(pos + 1)? == BYTE => Some(pos + 2),
            _ => None,
        }
    }

    /// A `Coll[Byte]` value read at `pos`, and where it ends.
    fn byte_coll_value(&self, pos: usize) -> Option<(&'a [u8], usize)> {
        let (len, start) = self.vlq(pos)?;
        let end = self.skip(start, len)?;
        Some((&self.bytes[start..end], end))
    }

    fn constant(&self, pos: usize) -> Option<usize> {
        let data = self.skip_type(pos, 0)?;
        self.skip_value(Ty::At(pos), data, 0)
    }

    fn byte(&self, pos: usize) -> Option<u8> {
        self.bytes.get(pos).copied()
    }

    fn skip(&self, pos: usize, len: u64) -> Option<usize> {
        let end = pos.checked_add(usize::try_from(len).ok()?)?;
        (end <= self.bytes.len()).then_some(end)
    }

    /// An unsigned VLQ, as the node reads one: up to ten bytes, bits past 64 dropped
    /// (scorex-util 0.2.1, `VLQReader.getULong`). Signed integers are zigzag-encoded VLQs, so
    /// this skips them too.
    fn vlq(&self, mut pos: usize) -> Option<(u64, usize)> {
        let mut value = 0u64;
        let mut shift = 0;
        while shift < 64 {
            let b = self.byte(pos)?;
            pos += 1;
            value |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some((value, pos));
            }
            shift += 7;
        }
        None
    }

    /// Skips a type's encoding (sigma-state 6.0.7, `TypeSerializer.deserialize`), failing on
    /// a type whose values this can't read.
    fn skip_type(&self, pos: usize, depth: usize) -> Option<usize> {
        if depth > MAX_DEPTH {
            return None;
        }
        let code = self.byte(pos)?;
        let pos = pos + 1;
        if code == 0 {
            return None;
        }
        if code < TUPLE {
            let prim = code % PRIM_RANGE;
            // 1..=4 are collections and options, 5..=7 pairs, triples and quadruples.
            let args = match (code / PRIM_RANGE, prim) {
                (0, _) => 0,
                (1..=4, 0) => 1,
                (1..=4, _) => 0,
                (5, 0) => 2,
                (6, 0) => 3,
                (7, 0) => 4,
                (5 | 6, _) => 1,
                (7, _) => 0,
                _ => unreachable!("code < TUPLE"),
            };
            if prim != 0 && !(BOOLEAN..=UNSIGNED_BIG_INT).contains(&prim) {
                return None;
            }
            return self.skip_types(pos, args, depth + 1);
        }
        match code {
            TUPLE => self.skip_types(pos + 1, self.byte(pos)?.into(), depth + 1),
            UNIT | AVL_TREE => Some(pos),
            // Box, String, Header, functions and the types with no values.
            _ => None,
        }
    }

    fn skip_types(&self, mut pos: usize, n: usize, depth: usize) -> Option<usize> {
        for _ in 0..n {
            pos = self.skip_type(pos, depth)?;
        }
        Some(pos)
    }

    /// Skips a value of type `ty` (sigma-state 6.0.7, `CoreDataSerializer.deserialize`). The
    /// type was read by `skip_type` first, so it is well formed.
    fn skip_value(&self, ty: Ty, pos: usize, depth: usize) -> Option<usize> {
        if depth > MAX_DEPTH {
            return None;
        }
        let depth = depth + 1;
        let t = match ty {
            Ty::Prim(code) => return self.skip_prim(code, pos, depth),
            Ty::At(t) => t,
        };
        let code = self.byte(t)?;
        if code >= TUPLE {
            return match code {
                TUPLE => self.skip_values(t + 2, self.byte(t + 1)?.into(), pos, depth),
                UNIT => Some(pos),
                AVL_TREE => {
                    let pos = self.skip(pos, AVL_DIGEST_LEN as u64 + 1)?; // and the flags
                    let (_, pos) = self.vlq(pos)?; // key length
                    match self.byte(pos)? {
                        0 => Some(pos + 1),
                        _ => self.vlq(pos + 1).map(|(_, end)| end), // value length
                    }
                }
                _ => None,
            };
        }
        let prim = code % PRIM_RANGE;
        let arg = if prim == 0 { Ty::At(t + 1) } else { Ty::Prim(prim) };
        match (code / PRIM_RANGE, prim) {
            (0, _) => self.skip_prim(code, pos, depth),
            (1, _) => self.skip_coll(arg, pos, depth),
            (2, _) => {
                let (len, mut pos) = self.vlq(pos)?;
                for _ in 0..len {
                    pos = self.skip_coll(arg, pos, depth)?;
                }
                Some(pos)
            }
            (3, _) => match self.byte(pos)? {
                0 => Some(pos + 1),
                _ => self.skip_value(arg, pos + 1, depth),
            },
            (4, _) => match self.byte(pos)? {
                0 => Some(pos + 1),
                _ => self.skip_coll(arg, pos + 1, depth),
            },
            (5, 0) => self.skip_values(t + 1, 2, pos, depth),
            (5, _) => {
                let pos = self.skip_prim(prim, pos, depth)?;
                self.skip_value(Ty::At(t + 1), pos, depth)
            }
            (6, 0) => self.skip_values(t + 1, 3, pos, depth),
            (6, _) => {
                let pos = self.skip_value(Ty::At(t + 1), pos, depth)?;
                self.skip_prim(prim, pos, depth)
            }
            (7, 0) => self.skip_values(t + 1, 4, pos, depth),
            (7, _) => {
                let pos = self.skip_prim(prim, pos, depth)?;
                self.skip_prim(prim, pos, depth)
            }
            _ => None,
        }
    }

    /// Skips one value of each of the `n` types encoded from `t` on, as a tuple's.
    fn skip_values(&self, mut t: usize, n: usize, mut pos: usize, depth: usize) -> Option<usize> {
        for _ in 0..n {
            pos = self.skip_value(Ty::At(t), pos, depth)?;
            t = self.skip_type(t, depth)?;
        }
        Some(pos)
    }

    fn skip_coll(&self, elem: Ty, pos: usize, depth: usize) -> Option<usize> {
        let (len, mut pos) = self.vlq(pos)?;
        // An element type may be a primitive in a byte of its own (`0c 01`, not `0d`), and is
        // still laid out as one: the node lays out by type, not by encoding (sigma-state 6.0.7,
        // `CoreDataSerializer.deserializeColl`).
        let elem = match elem {
            Ty::At(t) => match self.byte(t)? {
                code @ BOOLEAN..=UNSIGNED_BIG_INT => Ty::Prim(code),
                _ => elem,
            },
            prim => prim,
        };
        match elem {
            Ty::Prim(BOOLEAN) => self.skip(pos, len.div_ceil(8)),
            Ty::Prim(BYTE) => self.skip(pos, len),
            _ => {
                for _ in 0..len {
                    let end = self.skip_value(elem, pos, depth)?;
                    // A value taking no bytes (a unit, a tuple of units) would let a
                    // collection's length alone spin this loop; the node rejects them too
                    // (sigma-state 6.0.7, `CheckZeroWidthCollection`).
                    if end == pos {
                        return None;
                    }
                    pos = end;
                }
                Some(pos)
            }
        }
    }

    fn skip_prim(&self, code: u8, pos: usize, depth: usize) -> Option<usize> {
        match code {
            BOOLEAN | BYTE => self.skip(pos, 1),
            SHORT | INT | LONG => self.vlq(pos).map(|(_, end)| end),
            BIG_INT | UNSIGNED_BIG_INT => {
                let (len, pos) = self.vlq(pos)?;
                self.skip(pos, len)
            }
            GROUP_ELEMENT => self.skip(pos, GROUP_ELEMENT_LEN as u64),
            SIGMA_PROP => self.skip_sigma(pos, depth),
            _ => None,
        }
    }

    /// Skips a sigma proposition (sigma-state 6.0.7, `SigmaBoolean.serializer.parse`).
    fn skip_sigma(&self, pos: usize, depth: usize) -> Option<usize> {
        if depth > MAX_DEPTH {
            return None;
        }
        let pos = pos + 1;
        let (children, mut pos) = match self.byte(pos - 1)? {
            FALSE_PROP | TRUE_PROP => return Some(pos),
            PROVE_DLOG => return self.skip(pos, GROUP_ELEMENT_LEN as u64),
            PROVE_DH_TUPLE => return self.skip(pos, 4 * GROUP_ELEMENT_LEN as u64),
            AND | OR => self.vlq(pos)?,
            AT_LEAST => {
                let (_, pos) = self.vlq(pos)?; // the threshold
                self.vlq(pos)?
            }
            _ => return None,
        };
        // Each child takes at least its code byte, so the loop ends with the bytes.
        for _ in 0..children {
            pos = self.skip_sigma(pos, depth + 1)?;
        }
        Some(pos)
    }
}

#[cfg(test)]
#[path = "ergo_tree_tests.rs"]
mod tests;
