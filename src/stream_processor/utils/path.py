"""Path utilities for safe filesystem operations."""

import re


def sanitize_path_component(value: str | None) -> str:
    """
    Sanitize a string for safe filesystem path usage.

    Replaces any character that is not alphanumeric, underscore, or hyphen with underscore.
    This matches the Java sanitizePathComponent() implementation.

    Args:
        value: The string to sanitize

    Returns:
        Sanitized string safe for filesystem paths
    """
    if not value:
        return "unknown"
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)
