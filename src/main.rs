use clap::Parser;
use memmap2::{Advice, MmapOptions};
use rand::Rng;
use std::fs::File;
use std::io::{self, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Memory-mapped file benchmark
#[derive(Parser)]
struct Args {
    /// Device or file to mmap
    dev: String,

    /// Number of threads
    threads: usize,

    /// Sequential access (1 for sequential, 0 for random)
    seq: usize,

    /// madvise hint (0: NORMAL, 1: RANDOM, 2: SEQUENTIAL)
    hint: usize,
}

// Constants for byte units
const KB: u64 = 1024;
const MB: u64 = 1024 * KB;
const GB: u64 = 1024 * MB;
const PAGE_SIZE: u64 = 4096;
const SCAN_BLOCK_SIZE: u64 = 128 * MB;

fn main() -> io::Result<()> {
    let args = Args::parse();

    let file = File::open(&args.dev)?;
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        eprintln!("File size is zero");
        return Ok(());
    }

    // Memory-map the file
    let mmap = unsafe { MmapOptions::new().len(file_size as usize).map(&file)? };

    // Apply madvise hint using memmap2's advise method
    let advice = match args.hint {
        1 => Advice::Random,
        2 => Advice::Sequential,
        _ => Advice::Normal,
    };
    mmap.advise(advice)?;

    // Shared atomic counters
    let counts = AtomicU64::new(0);
    let sums = AtomicU64::new(0);
    let cpu_work = AtomicU64::new(0);
    let seq_scan_pos = AtomicU64::new(0);

    // Scoped threads using std::thread::scope
    thread::scope(|s| {
        // Spawn worker threads
        spawn_worker_threads(s, &args, &counts, &sums, &seq_scan_pos, &mmap, file_size);

        // CPU work thread
        {
            let cpu_work = &cpu_work;
            s.spawn(move || {
                let mut x: f64 = 0.0;
                loop {
                    for _ in 0..10000 {
                        x = x.ln().exp();
                    }
                    cpu_work.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        // Monitoring loop
        println!("dev,seq,hint,threads,time,workGB,tlb,readGB,CPUwork");
        let start = Instant::now();
        let mut last_shootdowns = read_tlb_shootdown_count().unwrap_or(0);
        let mut last_io_bytes = read_io_bytes().unwrap_or(0);

        loop {
            thread::sleep(Duration::from_secs(1));
            let shootdowns = read_tlb_shootdown_count().unwrap_or(0);
            let io_bytes = read_io_bytes().unwrap_or(0);
            let work_count = counts.swap(0, Ordering::Relaxed);
            let cpu_work_count = cpu_work.swap(0, Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "{},{},{},{},{:.2},{:.2},{},{:.2},{}",
                args.dev,
                args.seq,
                args.hint,
                args.threads,
                elapsed,
                (work_count * PAGE_SIZE) as f64 / GB as f64,
                shootdowns - last_shootdowns,
                (io_bytes - last_io_bytes) as f64 / GB as f64,
                cpu_work_count
            );
            last_shootdowns = shootdowns;
            last_io_bytes = io_bytes;
        }
    });

    Ok(())
}

fn spawn_worker_threads<'scope>(
    s: &'scope thread::Scope<'scope, '_>,
    args: &Args,
    counts: &'scope AtomicU64,
    sums: &'scope AtomicU64,
    seq_scan_pos: &'scope AtomicU64,
    mmap: &'scope [u8],
    file_size: u64,
) {
    for _ in 0..args.threads {
        let counts = counts;
        let sums = sums;
        let seq_scan_pos = seq_scan_pos;
        let mmap = mmap;
        let file_size = file_size;
        let seq = args.seq;

        s.spawn(move || {
            if seq != 0 {
                loop {
                    let pos =
                        seq_scan_pos.fetch_add(SCAN_BLOCK_SIZE, Ordering::Relaxed) % file_size;
                    let end = pos + SCAN_BLOCK_SIZE;
                    let mut j = pos;
                    while j < end {
                        let idx = (j % file_size) as usize;
                        let val = mmap[idx];
                        sums.fetch_add(val as u64, Ordering::Relaxed);
                        counts.fetch_add(1, Ordering::Relaxed);
                        j += PAGE_SIZE;
                    }
                }
            } else {
                let mut rng = rand::thread_rng();
                loop {
                    let pos = rng.gen_range(0..file_size);
                    let idx = pos as usize % mmap.len();
                    let val = mmap[idx];
                    sums.fetch_add(val as u64, Ordering::Relaxed);
                    counts.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}

fn read_tlb_shootdown_count() -> io::Result<u64> {
    let mut contents = String::new();
    File::open("/proc/interrupts")?.read_to_string(&mut contents)?;

    for line in contents.lines() {
        if line.contains("TLB") {
            let count: u64 = line
                .split_whitespace()
                .skip(1)
                .filter_map(|s| s.parse::<u64>().ok())
                .sum();
            return Ok(count);
        }
    }
    Ok(0)
}

fn read_io_bytes() -> io::Result<u64> {
    let mut contents = String::new();
    File::open("/proc/diskstats")?.read_to_string(&mut contents)?;

    let mut sum = 0u64;
    for line in contents.lines() {
        if line.contains("nvme") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() > 6 {
                if let Ok(sectors_read) = parts[5].parse::<u64>() {
                    sum += sectors_read * 512;
                }
            }
        }
    }
    Ok(sum)
}
