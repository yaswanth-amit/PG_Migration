"""Developer mode of the page: who may see the table choices, the mapping editor and the history.

The password is NOT in the code. Set one of these in the environment or in .env (never commit .env):
  MIGRATION_DEV_PASSWORD_SHA256   hex SHA-256 of the password (preferred; the .env then holds no password)
  MIGRATION_DEV_PASSWORD          the password itself
If neither is set, developer mode cannot be unlocked on that PC. Make the hash with:
  python -c "import hashlib,getpass;print(hashlib.sha256(getpass.getpass().encode()).hexdigest())"
This is a convenience gate against wrong clicks on a shared site PC, not a security boundary against someone
with access to the PC: that person can already edit files or start main.py.
"""
import hashlib
import hmac
import os

from pipeline import connections


def _load() -> None:
    connections._load_dotenv()                          # fills os.environ from .env without overriding it


def is_configured() -> bool:
    _load()
    return bool(os.environ.get("MIGRATION_DEV_PASSWORD_SHA256") or os.environ.get("MIGRATION_DEV_PASSWORD"))


def check_password(candidate : str) -> bool:
    """True when the candidate matches. Constant-time compare. False when nothing is configured."""
    _load()
    digest = (os.environ.get("MIGRATION_DEV_PASSWORD_SHA256") or "").strip().lower()
    plain = os.environ.get("MIGRATION_DEV_PASSWORD") or ""
    if digest:
        return hmac.compare_digest(hashlib.sha256(candidate.encode("utf-8")).hexdigest(), digest)
    if plain:
        return hmac.compare_digest(candidate.encode("utf-8"), plain.encode("utf-8"))
    return False
