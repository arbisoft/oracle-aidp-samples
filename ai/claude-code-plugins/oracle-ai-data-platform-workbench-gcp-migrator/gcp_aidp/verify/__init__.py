"""Classify a migrate report: PASS / REVIEW / SKIP / FAIL."""
from gcp_aidp.verify.checker import format_verify, verify

__all__ = ["format_verify", "verify"]
