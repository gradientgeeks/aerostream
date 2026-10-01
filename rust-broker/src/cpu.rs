//! CPU topology helpers for worker-thread placement.
//!
//! The allowed CPU set must come from the process's affinity mask (`sched_getaffinity`), which honours cgroup cpusets
//! (`docker --cpuset-cpus`, Kubernetes static CPU manager). Picking CPUs by `thread_number % cpu_count` breaks as soon as
//! the allowed set is not `0..n`: the pin fails with `EINVAL` for CPUs outside the set and several threads pile onto the
//! same few CPUs.

use std::collections::HashSet;
use std::io;

/// CPUs this process may run on, ascending. Falls back to `0..available_parallelism` if the mask cannot be read.
pub fn allowed_cpus() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: plain syscall writing into a zeroed, correctly sized cpu_set_t.
        let cpus: Vec<usize> = unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
                (0..libc::CPU_SETSIZE as usize).filter(|&c| libc::CPU_ISSET(c, &set)).collect()
            } else {
                Vec::new()
            }
        };
        if !cpus.is_empty() {
            return cpus;
        }
    }
    (0..std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)).collect()
}

/// Parses a kernel CPU list such as `0,4` or `0-3,8-9`.
pub fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in s.trim().split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                    out.extend(a..=b);
                }
            }
            None => {
                if let Ok(c) = part.trim().parse::<usize>() {
                    out.push(c);
                }
            }
        }
    }
    out
}

/// Hardware threads that share a physical core with `cpu` (including itself), from sysfs; `[cpu]` if unknown.
pub fn sysfs_siblings(cpu: usize) -> Vec<usize> {
    std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"))
        .map(|s| parse_cpu_list(&s))
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![cpu])
}

/// Orders `allowed` so that one hardware thread of every physical core comes before any second hardware thread:
/// worker `i` then lands on its own core for as long as cores are available, instead of sharing a core with a sibling.
pub fn spread_order(allowed: &[usize], siblings_of: impl Fn(usize) -> Vec<usize>) -> Vec<usize> {
    let mut seen_core: HashSet<usize> = HashSet::new();
    let (mut first, mut rest) = (Vec::new(), Vec::new());
    for &cpu in allowed {
        let core = siblings_of(cpu).into_iter().min().unwrap_or(cpu);
        if seen_core.insert(core) { first.push(cpu) } else { rest.push(cpu) }
    }
    first.extend(rest);
    first
}

/// Restricts the calling thread to exactly `cpus`.
pub fn set_current_thread_affinity(cpus: &[usize]) -> io::Result<()> {
    if cpus.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty CPU set"));
    }
    // SAFETY: zeroed cpu_set_t filled through CPU_SET, then handed to sched_setaffinity for the current thread.
    let ret = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if ret == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

/// Pins the calling thread to a single CPU.
pub fn pin_current_thread(cpu: usize) -> io::Result<()> {
    set_current_thread_affinity(&[cpu])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kernel_cpu_lists() {
        assert_eq!(parse_cpu_list("0,4\n"), vec![0, 4]);
        assert_eq!(parse_cpu_list("0-3,8-9"), vec![0, 1, 2, 3, 8, 9]);
        assert_eq!(parse_cpu_list("5"), vec![5]);
        assert!(parse_cpu_list("").is_empty());
    }

    #[test]
    fn spread_puts_one_thread_per_core_before_second_threads() {
        // siblings (0,4) (1,5): the layout of the c6id.2xlarge broker cpuset 0,1,4,5
        let sib = |c: usize| vec![c % 4, c % 4 + 4];
        assert_eq!(spread_order(&[0, 1, 4, 5], sib), vec![0, 1, 4, 5]);
        assert_eq!(spread_order(&[0, 1, 2, 3, 4, 5, 6, 7], sib), vec![0, 1, 2, 3, 4, 5, 6, 7]);
        // adjacent siblings (0,1) (2,3): second threads must come last
        let adj = |c: usize| vec![c & !1, c | 1];
        assert_eq!(spread_order(&[0, 1, 2, 3], adj), vec![0, 2, 1, 3]);
        // only one thread of a core is allowed: nothing to reorder
        assert_eq!(spread_order(&[4, 5], sib), vec![4, 5]);
    }

    #[test]
    fn allowed_cpus_is_non_empty_and_pinning_respects_the_allowed_set() {
        let allowed = allowed_cpus();
        assert!(!allowed.is_empty());
        let a2 = allowed.clone();
        std::thread::spawn(move || {
            // pin to an allowed CPU: ok; then widen back to the whole allowed set: ok
            pin_current_thread(a2[0]).unwrap();
            assert_eq!(allowed_cpus(), vec![a2[0]]);
            set_current_thread_affinity(&a2).unwrap();
            assert_eq!(allowed_cpus(), a2);
        })
        .join()
        .unwrap();
        // a CPU outside the allowed set is rejected instead of silently mis-pinning
        let outside = (0..1024).find(|c| !allowed.contains(c)).unwrap();
        std::thread::spawn(move || assert!(pin_current_thread(outside).is_err())).join().unwrap();
    }
}
