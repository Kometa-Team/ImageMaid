from pathlib import Path
import sqlite3
import tempfile
import unittest

from cleanup import ARTWORK_COLUMNS, in_use_artwork, walk_metadata


class DatabaseTests(unittest.TestCase):
    def test_selected_artwork_is_preserved_across_schema_versions(self):
        for count in (2, 3, 4, 5):
            with self.subTest(columns=count), sqlite3.connect(":memory:") as connection:
                columns = ARTWORK_COLUMNS[:count]
                connection.execute("CREATE TABLE metadata_items (" + ", ".join(f"{column} TEXT" for column in columns) + ")")
                connection.execute("INSERT INTO metadata_items VALUES (" + ", ".join("?" for _ in columns) + ")",
                                   [f"{'upload' if index % 2 else 'metadata'}://posters/hash{index}" for index in range(count)])
                self.assertEqual(in_use_artwork(connection), {f"hash{index}" for index in range(count)})

    def test_external_and_empty_urls_are_not_cleanup_references(self):
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE metadata_items (user_thumb_url TEXT, user_art_url TEXT)")
            connection.executemany("INSERT INTO metadata_items VALUES (?, ?)",
                                   [(None, ""), ("https://example.com/image", "upload://posters/selected")])
            self.assertEqual(in_use_artwork(connection), {"selected"})

    def test_unsupported_schema_aborts_instead_of_returning_no_references(self):
        for schema in (None, "user_square_art_url TEXT", "user_thumb_url TEXT"):
            with self.subTest(schema=schema), sqlite3.connect(":memory:") as connection:
                if schema:
                    connection.execute(f"CREATE TABLE metadata_items ({schema})")
                with self.assertRaisesRegex(ValueError, "metadata cleanup aborted"):
                    in_use_artwork(connection)


class MetadataTests(unittest.TestCase):
    def test_uploaded_themes_are_never_cleanup_candidates(self):
        with tempfile.TemporaryDirectory() as temporary:
            metadata = Path(temporary) / "Metadata"
            bundle = metadata / "Movies" / "a" / "example.bundle"
            paths = [
                bundle / "Uploads" / "themes" / "themehash",
                bundle / "Uploads" / "themes" / "nested" / "anotherhash",
                bundle / "Uploads" / "posters" / "posterhash",
                bundle / "Uploads" / "art" / "arthash",
                bundle / "Contents" / "posters" / "plexhash",
            ]
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"fixture")
            candidates = {
                Path(root) / filename
                for root, _, files in walk_metadata(metadata)
                for filename in files
            }
            self.assertEqual(candidates, {paths[2], paths[3]})
            self.assertTrue(all(path.is_file() for path in paths))


if __name__ == "__main__":
    unittest.main()
