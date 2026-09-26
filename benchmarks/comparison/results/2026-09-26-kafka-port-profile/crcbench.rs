use std::time::Instant;
fn table() -> [u32; 256] {
    let mut t = [0u32; 256];
    for i in 0..256u32 { let mut c = i; for _ in 0..8 { c = if c & 1 != 0 { (c >> 1) ^ 0x82F63B78 } else { c >> 1 }; } t[i as usize] = c; }
    t
}
fn sw(t: &[u32; 256], data: &[u8]) -> u32 {           // identical to handlers::crc32c
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data { let idx = ((crc ^ b as u32) & 0xFF) as usize; crc = (crc >> 8) ^ t[idx]; }
    !crc
}
#[target_feature(enable = "sse4.2")]
unsafe fn hw(data: &[u8]) -> u32 {
    use std::arch::x86_64::*;
    let mut crc = 0xFFFF_FFFFu64;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks { crc = _mm_crc32_u64(crc, u64::from_le_bytes(c.try_into().unwrap())); }
    let mut c32 = crc as u32;
    for &b in chunks.remainder() { c32 = _mm_crc32_u8(c32, b); }
    !c32
}
fn main() {
    let t = table();
    let data: Vec<u8> = (0..(64usize << 20)).map(|i| (i * 31 % 251) as u8).collect();
    let s = Instant::now(); let a = sw(&t, &data); let ds = s.elapsed().as_secs_f64();
    let s = Instant::now(); let b = unsafe { hw(&data) }; let dh = s.elapsed().as_secs_f64();
    println!("same result: {}", a == b);
    println!("byte-at-a-time table : {:8.0} MB/s", 64.0 / ds);
    println!("hardware crc32 (sse4.2): {:8.0} MB/s  ({:.0}x)", 64.0 / dh, ds / dh);
    println!("CPU cost per 1 KiB record: table {:.2} us, hardware {:.3} us", ds / 65536.0 * 1e6, dh / 65536.0 * 1e6);
}
