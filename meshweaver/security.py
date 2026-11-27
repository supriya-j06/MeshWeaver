"""
Security layer
  * UDP (DHT/gossip) : every datagram carries an HMAC-SHA256 made with the cluster's mesh.secret
  * TCP (tasks)      : TLS 1.2+ with a pinned cluster certificate
  * Task requests    : Ed25519-signed envelopes; nodes only run tasks signed by a trusted key
"""
from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import ipaddress
import json
import os
import ssl
import time
from pathlib import Path

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


class SecurityError(Exception):
    pass


class ClusterFiles:
    def __init__(self, root):
        self.root = Path(root)
        self.secret = self.root / "mesh.secret"
        self.cert = self.root / "tls" / "cert.pem"
        self.key = self.root / "tls" / "key.pem"
        self.keys_dir = self.root / "keys"
        self.trusted_dir = self.root / "keys" / "trusted"


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def unb64(s: str) -> bytes:
    return base64.b64decode(s)


# ---------------------------------------------------------------- cluster init
def init_cluster(root) -> ClusterFiles:
    f = ClusterFiles(root)
    if f.secret.exists():
        return f
    f.cert.parent.mkdir(parents=True, exist_ok=True)
    f.trusted_dir.mkdir(parents=True, exist_ok=True)
    f.secret.write_text(os.urandom(32).hex())
    _make_cert(f)
    new_submitter(f, "submitter")
    return f


def _make_cert(f: ClusterFiles):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "meshweaver")])
    now = datetime.datetime.now(datetime.timezone.utc)
    pub = key.public_key()
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name).public_key(pub)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=825))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(
            digital_signature=True, key_cert_sign=True, crl_sign=False, content_commitment=False,
            key_encipherment=False, data_encipherment=False, key_agreement=False,
            encipher_only=False, decipher_only=False), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(pub), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(pub), critical=False)
        .add_extension(x509.SubjectAlternativeName(
            [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False)
        .sign(key, hashes.SHA256())
    )
    f.cert.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    f.key.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    _chmod(f.key)


def new_submitter(f: ClusterFiles, name: str):
    """Create an Ed25519 signing key <name>.key and trust its public half."""
    priv = ed25519.Ed25519PrivateKey.generate()
    f.keys_dir.mkdir(parents=True, exist_ok=True)
    f.trusted_dir.mkdir(parents=True, exist_ok=True)
    path = f.keys_dir / f"{name}.key"
    path.write_bytes(priv.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    _chmod(path)
    raw = priv.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    (f.trusted_dir / f"{name}.pub").write_text(raw.hex())


def _chmod(p: Path):
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass


# ---------------------------------------------------------------- loaders
def _need(path: Path):
    if not path.exists():
        raise SystemExit(f"missing {path}\nrun first:  python -m meshweaver init --dir <cluster-dir>")


def load_mesh_secret(f: ClusterFiles) -> bytes:
    _need(f.secret)
    return bytes.fromhex(f.secret.read_text().strip())


def load_trusted_keys(f: ClusterFiles) -> dict:
    keys = {}
    if f.trusted_dir.exists():
        for p in f.trusted_dir.glob("*.pub"):
            keys[p.stem] = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(p.read_text().strip()))
    return keys


def load_signing_key(f: ClusterFiles, name: str = "submitter"):
    path = f.keys_dir / f"{name}.key"
    _need(path)
    return serialization.load_pem_private_key(path.read_bytes(), password=None)


def server_ssl_context(f: ClusterFiles) -> ssl.SSLContext:
    _need(f.cert)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(str(f.cert), str(f.key))
    return ctx


def client_ssl_context(f: ClusterFiles) -> ssl.SSLContext:
    _need(f.cert)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = False   # nodes are addressed by IP; trust comes from the pinned cert
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(str(f.cert))
    return ctx


# ---------------------------------------------------------------- UDP HMAC
def mac_pack(secret: bytes, obj) -> bytes:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return hmac.new(secret, raw, hashlib.sha256).digest() + raw


def mac_unpack(secret: bytes, data: bytes):
    if len(data) < 33:
        raise SecurityError("short datagram")
    mac, raw = data[:32], data[32:]
    if not hmac.compare_digest(mac, hmac.new(secret, raw, hashlib.sha256).digest()):
        raise SecurityError("bad mac")
    try:
        return json.loads(raw)
    except ValueError as e:
        raise SecurityError("bad json") from e


# ---------------------------------------------------------------- signed requests
def sign_request(priv, kid: str, op: str, **fields) -> dict:
    body = {"op": op, "kid": kid, "ts": time.time(), "nonce": os.urandom(8).hex(), **fields}
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
    return {"env": b64(raw), "sig": b64(priv.sign(raw))}


def verify_request(msg: dict, trusted: dict, replay: dict | None = None,
                   max_skew: float = 300.0) -> dict:
    try:
        raw = unb64(msg["env"])
        sig = unb64(msg["sig"])
        body = json.loads(raw)
        pub = trusted.get(body.get("kid"))
    except (KeyError, ValueError, TypeError) as e:
        raise SecurityError("malformed request") from e
    if pub is None:
        raise SecurityError(f"unknown signer {body.get('kid')!r}")
    try:
        pub.verify(sig, raw)
    except InvalidSignature as e:
        raise SecurityError("bad signature") from e
    now = time.time()
    if abs(now - float(body.get("ts", 0))) > max_skew:
        raise SecurityError("request timestamp outside allowed window")
    if replay is not None:
        for n in [n for n, exp in replay.items() if exp < now]:
            del replay[n]
        if body["nonce"] in replay:
            raise SecurityError("replayed request")
        replay[body["nonce"]] = now + max_skew * 2
    return body
