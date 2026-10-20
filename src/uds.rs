//! Unix domain socket binding shared by the inbound and API listeners.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio::net::UnixListener;
use tracing::warn;

/// Binds a listening socket at `path` with the given permission bits.
///
/// An existing socket file is removed only if nothing accepts connections on
/// it, so a running instance never has its socket stolen. Any other kind of
/// file at the path is left alone and reported as an error.
pub fn bind(path: &Path, mode: u32) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("{} is in use by another process", path.display()),
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => fs::remove_file(path)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(path)?;
    set_mode(path, mode)?;
    Ok(listener)
}

pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

/// Removes a socket file we created; a missing file is not an error.
pub fn remove(path: &Path) {
    if let Err(e) = fs::remove_file(path) {
        if e.kind() != io::ErrorKind::NotFound {
            warn!(path = %path.display(), error = %e, "could not remove socket file");
        }
    }
}
