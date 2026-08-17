// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

//! Optional opt-in pinning of the process to "performance" cores on
//! hybrid (P-core/E-core) CPUs.
//!
//! On hybrid topologies, Rayon workers that land on an E-core become
//! stragglers relative to their P-core siblings at every parallel sync
//! point, which can degrade throughput disproportionately at high record
//! counts. This restricts the process (and therefore Rayon's default
//! global pool, spawned lazily on first use) to the highest
//! `EfficiencyClass` reported by Windows. No-op, returns `false`, on
//! non-Windows targets or non-hybrid CPUs.

#[cfg(target_os = "windows")]
pub fn pin_to_p_cores() -> bool {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::SystemInformation::{
        GetLogicalProcessorInformationEx, RelationProcessorCore,
        SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
    };
    use windows::Win32::System::Threading::{GetCurrentProcess, SetProcessAffinityMask};

    // First call to learn the required buffer size.
    let mut len: u32 = 0;
    unsafe {
        let _ = GetLogicalProcessorInformationEx(RelationProcessorCore, None, &mut len);
    }
    if len == 0 {
        return false;
    }

    let mut buf: Vec<u8> = vec![0u8; len as usize];
    let ok = unsafe {
        GetLogicalProcessorInformationEx(
            RelationProcessorCore,
            Some(
                buf.as_mut_ptr()
                    .cast::<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(),
            ),
            &mut len,
        )
    };
    if ok.is_err() {
        return false;
    }

    // Walk the variable-length list of entries, each describing one
    // physical core (EfficiencyClass + the logical-processor mask of its
    // hyperthread siblings, group 0 only -- fine below 64 logical CPUs).
    let mut offset = 0usize;
    let mut max_class: u8 = 0;
    let mut entries: Vec<(u8, u64)> = Vec::new();
    while offset < buf.len() {
        let entry = unsafe {
            &*(buf.as_ptr().add(offset) as *const SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)
        };
        let size = entry.Size as usize;
        if size == 0 {
            break;
        }
        let core = unsafe { &entry.Anonymous.Processor };
        let class = core.EfficiencyClass;
        let mask = core.GroupMask[0].Mask as u64;
        max_class = max_class.max(class);
        entries.push((class, mask));
        offset += size;
    }

    // No hybrid topology (single efficiency class) -- nothing to gain.
    if entries.iter().all(|(class, _)| *class == entries[0].0) {
        return false;
    }

    let p_core_mask: u64 = entries
        .iter()
        .filter(|(class, _)| *class == max_class)
        .fold(0u64, |acc, (_, mask)| acc | mask);
    if p_core_mask == 0 {
        return false;
    }

    let handle: HANDLE = unsafe { GetCurrentProcess() };
    unsafe { SetProcessAffinityMask(handle, p_core_mask as usize) }.is_ok()
}

#[cfg(not(target_os = "windows"))]
pub fn pin_to_p_cores() -> bool {
    false
}
