"""Filesystem and database helpers for safe Plex cleanup."""

import os
from urllib.parse import urlparse


ARTWORK_COLUMNS = (
    "user_thumb_url", "user_art_url", "user_banner_url",
    "user_clear_logo_url", "user_square_art_url",
)


def in_use_artwork(connection):
    """Read selected artwork across Plex schemas, failing closed if unsupported."""
    columns = {row[1] for row in connection.execute("PRAGMA table_info(metadata_items)")}
    if not {"user_thumb_url", "user_art_url"}.issubset(columns):
        raise ValueError("Unsupported Plex database schema: metadata_items must contain user_thumb_url and user_art_url; metadata cleanup aborted")
    urls = set()
    for column in ARTWORK_COLUMNS:
        if column in columns:
            rows = connection.execute(
                f"SELECT {column} FROM metadata_items "
                f"WHERE {column} LIKE 'upload://%' OR {column} LIKE 'metadata://%'"
            )
            urls.update(urlparse(row[0]).path.split("/")[-1] for row in rows if row[0])
    return urls


def walk_metadata(metadata_dir):
    """Walk metadata without entering Plex content or uploaded theme directories."""
    for root, directories, files in os.walk(metadata_dir):
        directories[:] = [
            directory for directory in directories
            if directory != "Contents"
            and not (os.path.basename(root) == "Uploads" and directory == "themes")
        ]
        yield root, directories, files


def transcoder_files(transcoder_dir):
    """Yield regular cache files, excluding Synology metadata and symlinks."""
    for root, directories, files in os.walk(transcoder_dir):
        directories[:] = [
            directory for directory in directories
            if directory != "@eaDir" and not os.path.islink(os.path.join(root, directory))
        ]
        for filename in files:
            path = os.path.join(root, filename)
            if "." in filename and not os.path.islink(path) and os.path.isfile(path):
                yield path


def remove_transcoder_file(path):
    """Return recovered bytes, or None if Plex already removed this cache file."""
    try:
        size = os.path.getsize(path)
        os.remove(path)
    except FileNotFoundError:
        return None
    return size
