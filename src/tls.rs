//! Client TLS for the channels: one rustls configuration for MQTT and
//! WebSocket, trusting the platform's certificate store or only a CA file.

use std::path::Path;
use std::sync::{Arc, Mutex};

use rustls_platform_verifier::ConfigVerifierExt;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

/// The configuration for one connection. Without `ca_file` it verifies with
/// the platform's store, built once and shared by every channel. With it,
/// only the certificates in that PEM file are trusted; the file is read for
/// every connection, so a replaced file applies from the next reconnect.
/// Reading may block, so callers run this on the blocking pool.
pub fn client_config(ca_file: Option<&Path>) -> Result<Arc<ClientConfig>, String> {
    match ca_file {
        None => platform(),
        Some(path) => trusting(path).map_err(|e| format!("ca_file {}: {e}", path.display())),
    }
}

fn platform() -> Result<Arc<ClientConfig>, String> {
    static PLATFORM: Mutex<Option<Arc<ClientConfig>>> = Mutex::new(None);
    let mut shared = PLATFORM.lock().unwrap();
    if let Some(config) = shared.as_ref() {
        return Ok(config.clone());
    }
    let config = Arc::new(ClientConfig::with_platform_verifier().map_err(|e| format!("tls: {e}"))?);
    *shared = Some(config.clone());
    Ok(config)
}

fn trusting(path: &Path) -> Result<Arc<ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path).map_err(|e| e.to_string())? {
        roots
            .add(cert.map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    }
    if roots.is_empty() {
        return Err("no certificate".into());
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ca_file_must_hold_a_certificate() {
        let ca = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/ca.pem"));
        assert!(client_config(Some(ca)).is_ok());
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let err = client_config(Some(&empty)).unwrap_err();
        assert!(err.contains("no certificate"), "{err}");
        let err = client_config(Some(&dir.path().join("missing.pem"))).unwrap_err();
        assert!(err.starts_with("ca_file "), "{err}");
        // The platform's configuration is built once.
        assert!(Arc::ptr_eq(&platform().unwrap(), &platform().unwrap()));
    }
}
