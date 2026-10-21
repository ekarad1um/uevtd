//! Unix domain socket binding shared by the inbound and API listeners.

use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

use tokio::net::{UnixListener, UnixStream};
use tracing::warn;

const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Binds a listening socket at `path` with the given permission bits.
///
/// An existing socket file is removed only if nothing accepts connections on
/// it, so a running instance never has its socket stolen. Any other kind of
/// file at the path is left alone and reported as an error.
pub async fn bind(path: &Path, mode: u32) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            // Linux reports ECONNREFUSED for any non-socket path too, hence
            // the type check above before anything is removed. The probe is
            // bounded: a listener that never completes the connection (a hung
            // process with a full backlog) counts as live.
            match tokio::time::timeout(PROBE_TIMEOUT, UnixStream::connect(path)).await {
                Ok(Ok(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("{} is in use by another process", path.display()),
                    ));
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!(
                            "{} did not answer a connection within {PROBE_TIMEOUT:?}; assuming it is in use",
                            path.display()
                        ),
                    ));
                }
                Ok(Err(e)) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(path)?
                }
                Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => {}
                Ok(Err(e)) => return Err(e),
            }
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a socket", path.display()),
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replaces_stale_sockets_but_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s");
        let live = bind(&path, 0o600).await.unwrap();
        assert_eq!(
            bind(&path, 0o600).await.unwrap_err().kind(),
            io::ErrorKind::AddrInUse
        );
        drop(live);
        assert!(path.exists());
        drop(bind(&path, 0o600).await.unwrap());

        fs::remove_file(&path).unwrap();
        fs::write(&path, b"x").unwrap();
        assert_eq!(
            bind(&path, 0o600).await.unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(&path).unwrap(), b"x");
    }
}
