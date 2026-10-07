// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

//! Free-space checks for the volume holding the output directory: an
//! up-front comparison against the run's estimated output size (see
//! `pipeline::estimate_output_bytes`) and a [`DiskGuard`] polled during
//! generation, so a run stops cleanly instead of filling the disk.

use std::path::{Path, PathBuf};

use sysinfo::{DiskRefreshKind, Disks};

/// Free space below which a running generation stops. Comfortably more
/// than one batch's worth of output (a 500K-row IPC batch of the widest
/// schemas is ~200 MB), so the stop happens before a write can fail.
pub const MIN_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// `path`, or its nearest existing ancestor (the output directory may not
/// exist yet), made absolute — without Windows' `\\?\` verbatim prefix,
/// which `Disk::mount_point` (`C:\`) never carries.
fn resolve(path: &Path) -> Option<PathBuf> {
    let mut p = path.to_path_buf();
    loop {
        if let Ok(c) = std::fs::canonicalize(&p) {
            let s = c.to_string_lossy();
            return Some(PathBuf::from(s.strip_prefix(r"\\?\").unwrap_or(&s)));
        }
        if !p.pop() {
            return None;
        }
    }
}

/// Index of the disk whose mount point is the longest prefix of `path`.
fn disk_index(disks: &Disks, path: &Path) -> Option<usize> {
    disks
        .list()
        .iter()
        .enumerate()
        .filter(|(_, d)| path.starts_with(d.mount_point()))
        .max_by_key(|(_, d)| d.mount_point().as_os_str().len())
        .map(|(i, _)| i)
}

/// Free space (bytes) on the volume holding `path`, or `None` if it can't
/// be determined (unusual filesystem, sandbox) — callers then skip the
/// check rather than block a run on a guess.
pub fn available_space(path: &Path) -> Option<u64> {
    let path = resolve(path)?;
    let disks = Disks::new_with_refreshed_list();
    disk_index(&disks, &path).map(|i| disks.list()[i].available_space())
}

pub fn gb(bytes: u64) -> f64 {
    bytes as f64 / 1e9
}

/// Polled during generation: fails once free space on the output volume
/// drops below [`MIN_FREE_BYTES`]. Only that one disk's storage figures are
/// refreshed per check, so polling it once per batch is cheap.
pub struct DiskGuard {
    disks: Disks,
    index: usize,
    path: PathBuf,
}

impl DiskGuard {
    /// `None` when the output volume can't be identified — no guard then.
    pub fn new(output_dir: &Path) -> Option<Self> {
        let path = resolve(output_dir)?;
        let disks = Disks::new_with_refreshed_list();
        let index = disk_index(&disks, &path)?;
        Some(Self { disks, index, path })
    }

    pub fn check(&mut self) -> Result<(), String> {
        let disk = &mut self.disks.list_mut()[self.index];
        disk.refresh_specifics(DiskRefreshKind::nothing().with_storage());
        let free = disk.available_space();
        if free < MIN_FREE_BYTES {
            return Err(format!(
                "stopped: only {:.1} GB left on the disk holding {} (minimum {:.1} GB) — \
                 free some space or choose another --output-dir. Files written so far \
                 are incomplete.",
                gb(free),
                self.path.display(),
                gb(MIN_FREE_BYTES)
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_space_on_temp_dir() {
        // The temp dir's volume is always a real, listed disk.
        let free = available_space(&std::env::temp_dir());
        assert!(free.is_some_and(|f| f > 0));
    }

    #[test]
    fn test_resolve_falls_back_to_existing_ancestor() {
        let missing = std::env::temp_dir().join("dupehell_missing_dir_xyz/sub");
        let resolved = resolve(&missing).unwrap();
        assert!(!resolved.to_string_lossy().starts_with(r"\\?\"));
        assert!(available_space(&missing).is_some());
    }

    #[test]
    fn test_guard_passes_with_space_left() {
        let mut guard = DiskGuard::new(&std::env::temp_dir()).unwrap();
        // CI runners and dev machines keep well over 2 GB free on temp.
        assert!(guard.check().is_ok());
    }
}
