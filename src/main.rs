use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::io::AsRawFd;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Arg, Command};
use libc::{madvise, mmap, munmap, stat, sysconf, timespec, MAP_FAILED, MAP_SHARED, O_RDONLY, PROT_READ, SYS_gettimeofday, _SC_PAGESIZE, _SC_PHYS_PAGES, MADV_NORMAL, MADV_RANDOM, MADV_SEQUENTIAL};

fn gettime() -> f64 {
    let mut now = timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut now) };
    now.tv_sec as f64 + now.tv_nsec as f64 / 1_000_000_000.0
}

fn read_tlb_shootdown_count() -> u64 {
    let file = File::open("/proc/interrupts").expect("Unable to open /proc/interrupts");
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line.expect("Unable to read line");
        if line.contains("TLB") {
            let mut count = 0;
            for val in line.split_whitespace().skip(1) {
                if let Ok(c) = val.parse::<u64>() {
                    count += c;
                }
            }
            return count;
        }
    }
    0
}

fn read_io_bytes() -> u64 {
    let file = File::open("/proc/diskstats").expect("Unable to open /proc/diskstats");
    let reader = BufReader::new(file);

    let mut sum = 0;
    for line in reader.lines() {
        let line = line.expect("Unable to read line");
        if line.contains("nvme") {
            let strs: Vec<&str> = line.split_whitespace().collect();
            if let Ok(c) = strs[5].parse::<u64>() {
                sum += c * 512;
            }
        }
    }
    sum
}

fn main() {
    let matches = Command::new("mmapbench")
        .version("1.0")
        .author("Your Name <youremail@example.com>")
        .about("Benchmark using mmap in Rust")
        .arg(Arg::new("dev")
            .help("Device or file to mmap")
            .required(true)
            .index(1))
        .arg(Arg::new("threads")
            .help("Number of threads")
            .required(true)
            .index(2))
        .arg(Arg::new("seq")
            .help("Sequential (1) or Random (0) access")
            .required(true)
            .index(3))
        .arg(Arg::new("hint")
            .help("Madvise hint: 0 for normal, 1 for random, 2 for sequential")
            .required(true)
            .index(4))
        .get_matches();

    let dev = matches.value_of("dev").unwrap();
    let threads: usize = matches.value_of("threads").unwrap().parse().expect("Invalid number of threads");
    let seq: bool = matches.value_of("seq").unwrap().parse::<i32>().expect("Invalid seq value") != 0;
    let hint: i32 = matches.value_of("hint").unwrap().parse().expect("Invalid hint value");

    let fd = File::open(dev).expect("Unable to open file");
    let fd_raw = fd.as_raw_fd();

    let mut sb = stat {
        st_dev: 0,
        st_ino: 0,
        st_mode: 0,
        st_nlink: 0,
        st_uid: 0,
        st_gid: 0,
        st_rdev: 0,
        st_size: 0,
        st_blksize: 0,
        st_blocks: 0,
        st_atime: 0,
        st_atime_nsec: 0,
        st_mtime: 0,
        st_mtime_nsec: 0,
        st_ctime: 0,
        st_ctime_nsec: 0,
    };

    unsafe { libc::fstat(fd_raw, &mut sb) };

    let file_size = 2 * 1024 * 1024 * 1024 * 1024u64;
    let page_size = unsafe { sysconf(_SC_PAGESIZE) } as usize;

    let p = unsafe {
        mmap(
            std::ptr::null_mut(),
            file_size as usize,
            PROT_READ,
            MAP_SHARED,
            fd_raw,
            0,
        )
    };
    assert!(p != MAP_FAILED);

    unsafe {
        madvise(
            p,
            file_size as usize,
            match hint {
                1 => MADV_RANDOM,
                2 => MADV_SEQUENTIAL,
                _ => MADV_NORMAL,
            },
        );
    }

    let counts = Arc::new((0..threads).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let sums = Arc::new((0..threads).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());

    let seq_scan_pos = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(threads);

    for i in 0..threads {
        let counts = Arc::clone(&counts);
        let sums = Arc::clone(&sums);
        let seq_scan_pos = Arc::clone(&seq_scan_pos);
        let p = p as *const u8;
        handles.push(thread::spawn(move || {
            let count = &counts[i];
            let sum = &sums[i];

            if seq {
                loop {
                    let scan_block = 128 * 1024 * 1024;
                    let pos = (seq_scan_pos.fetch_add(scan_block, Ordering::Relaxed) % file_size) as isize;

                    for j in (0..scan_block).step_by(page_size) {
                        unsafe {
                            *sum.get_mut() += *p.offset(pos + j as isize) as u64;
                        }
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                let mut rng = rand::thread_rng();
                let dist = rand::distributions::Uniform::new(0, file_size as usize);

                loop {
                    unsafe {
                        *sum.get_mut() += *p.offset(dist.sample(&mut rng) as isize) as u64;
                    }
                    count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    let cpu_work = Arc::new(AtomicU64::new(0));

    {
        let cpu_work = Arc::clone(&cpu_work);
        handles.push(thread::spawn(move || {
            let mut x = cpu_work.load(Ordering::Relaxed) as f64;
            loop {
                for _ in 0..10000 {
                    x = x.exp().ln();
                }
                cpu_work.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    println!("dev,seq,hint,threads,time,workGB,tlb,readGB,CPUwork");

    let mut last_shootdowns = read_tlb_shootdown_count();
    let mut last_io_bytes = read_io_bytes();
    let start = Instant::now();

    loop {
        thread::sleep(Duration::from_secs(1));

        let shootdowns = read_tlb_shootdown_count();
        let io_bytes = read_io_bytes();

        let work_count: u64 = counts.iter().map(|c| c.swap(0, Ordering::Relaxed)).sum();
        let elapsed = start.elapsed().as_secs_f64();
        let work_gb = (work_count * page_size as u64) as f64 / (1024.0 * 1024.0 * 1024.0);
        let tlb = shootdowns - last_shootdowns;
        let read_gb = (io_bytes - last_io_bytes) as f64 / (1024.0 * 1024.0 * 1024.0);
        let cpu_work_done = cpu_work.swap(0, Ordering::Relaxed);

        println!("{},{},{},{},{},{},{},{},{}", dev, seq, hint, threads, elapsed, work_gb, tlb, read_gb, cpu_work_done);

        last_shootdowns = shootdowns;
        last_io_bytes = io_bytes;
    }

    unsafe {
        munmap(p, file_size as usize);
    }
}
