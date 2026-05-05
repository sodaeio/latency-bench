// Minimal Solana shred header parser, modelled on shredwatch.
//
// Layout we rely on (stable across legacy + Merkle variants):
//   [0..64]  Ed25519 signature
//   [64]     ShredVariant byte
//   [65..73] slot (u64 LE)
//   [73..77] index (u32 LE)
//
// We do NOT decode FEC sets, reassemble entries, or recover transactions.

pub const MIN_SHRED_SIZE: usize = 83;

const VARIANT_LEGACY_DATA: u8 = 0xA5;
const VARIANT_LEGACY_CODE: u8 = 0x5A;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShredType {
    Data,
    Code,
}

#[derive(Clone, Copy, Debug)]
pub struct ShredKey {
    pub slot: u64,
    // Kept for future shred-by-shred matching (cross-source dedup).
    #[allow(dead_code)]
    pub index: u32,
    #[allow(dead_code)]
    pub shred_type: ShredType,
}

fn classify(variant: u8) -> ShredType {
    match variant {
        VARIANT_LEGACY_DATA => ShredType::Data,
        VARIANT_LEGACY_CODE => ShredType::Code,
        // Merkle variants encode type in the high bit.
        v if v & 0x80 != 0 => ShredType::Data,
        _ => ShredType::Code,
    }
}

pub fn parse(buf: &[u8]) -> Option<ShredKey> {
    if buf.len() < MIN_SHRED_SIZE {
        return None;
    }
    let variant = buf[64];
    let slot = u64::from_le_bytes(buf[65..73].try_into().ok()?);
    let index = u32::from_le_bytes(buf[73..77].try_into().ok()?);
    if slot == 0 {
        return None;
    }
    Some(ShredKey {
        slot,
        index,
        shred_type: classify(variant),
    })
}
