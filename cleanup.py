"""Filesystem and database helpers for safe Plex cleanup."""

import os


def walk_metadata(metadata_dir):
    """Walk metadata without entering Plex content or uploaded theme directories."""
    for root, directories, files in os.walk(metadata_dir):
        directories[:] = [
            directory for directory in directories
            if directory != "Contents"
            and not (os.path.basename(root) == "Uploads" and directory == "themes")
        ]
        yield root, directories, files
