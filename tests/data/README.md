Test fixtures only; the key protects nothing.

- `ca.pem`: a self-signed CA, P-256, valid until 2126. Its key was discarded.
- `server.pem`, `server.key`: a server certificate signed by it, for `localhost`
  and `127.0.0.1`, with its PKCS#8 key.

Made with OpenSSL 3: `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256`
for the CA, then a request signed with `openssl x509 -req -days 36500` and the
extensions `subjectAltName=DNS:localhost,IP:127.0.0.1`, `basicConstraints=CA:FALSE`,
`keyUsage=critical,digitalSignature` and `extendedKeyUsage=serverAuth`.
