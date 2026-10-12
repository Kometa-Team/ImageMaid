from pathlib import Path
import tempfile
import unittest

from cleanup import walk_metadata


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
