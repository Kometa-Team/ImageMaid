import unittest
from unittest.mock import patch, Mock
from gotify import GotifyNotifier, _summary, _short_error

CONFIG = {"GOTIFY_ENABLED": "True", "GOTIFY_URL": "https://gotify.example.com/", "GOTIFY_TOKEN": "secret", "GOTIFY_NOTIFY_ERROR": "True"}

class GotifyTests(unittest.TestCase):
    def test_report_summary(self):
        report = [[("Database", "")], [("", "Downloaded New Database")], [("Downloaded", "0:03:09"), ("Query", "0:00:00")], [("Reporting Bloat Images", "")], [("", "173.99 MBs of Potential Recovery Reporting 503 Files")], [("Scan Time", "0:00:30")], [("Total Runtime", "0:03:40")]]
        body = _summary("Running in Report Mode", report)
        self.assertIn("**Bloat Images Found:** 503", body)
        self.assertIn("**Potential Recovery:** 173.99 MBs", body)
        self.assertIn("**Total Runtime:** 0:03:40", body)
        self.assertIn("No images removed", body)
    def test_short_mode_error(self):
        original = "Mode Error: invalid-test Invalid. Options: \n\t" + "many options " * 100
        self.assertEqual(_short_error(original), "Invalid execution mode: invalid-test")
        body = _summary("Running in Invalid-test Mode", [], original)
        self.assertIn("Invalid execution mode: invalid-test", body)
        self.assertNotIn("many options", body)

    def test_other_error_is_bounded(self):
        self.assertLessEqual(len(_short_error("X" * 2000)), 503)

    @patch("gotify.requests.post")
    def test_start_and_end_independent(self, post):
        cfg = {**CONFIG, "GOTIFY_NOTIFY_START": "False"}
        notifier = GotifyNotifier(cfg)
        self.assertFalse(notifier.send("started", "message", event="start"))
        self.assertTrue(notifier.send("completed", "message", event="end"))
        self.assertEqual(post.call_count, 1)

    @patch("gotify.requests.post")
    def test_send(self, post):
        post.return_value = Mock()
        notifier = GotifyNotifier(CONFIG)
        self.assertTrue(notifier.send_run_summary("Running in Report Mode", [], None))
        self.assertEqual(post.call_args.kwargs["json"]["title"], "ImageMaid - Run Completed")
        self.assertEqual(post.call_args.kwargs["json"]["extras"]["client::display"]["contentType"], "text/markdown")
    @patch("gotify.requests.post")
    def test_error_notification(self, post):
        self.assertTrue(GotifyNotifier(CONFIG).send_run_summary("Running in Report Mode", [], "Example failure"))
        self.assertEqual(post.call_args.kwargs["json"]["title"], "ImageMaid - Run Failed")
    @patch("gotify.requests.post")
    def test_disabled(self, post):
        self.assertFalse(GotifyNotifier({**CONFIG, "GOTIFY_ENABLED": "False"}).send("x", "y"))
        post.assert_not_called()
    @patch("gotify.requests.post")
    def test_error_flag_disabled(self, post):
        self.assertFalse(GotifyNotifier({**CONFIG, "GOTIFY_NOTIFY_ERROR": "False"}).send("x", "y", "error"))
        post.assert_not_called()
    @patch("gotify.requests.post", side_effect=__import__("requests").RequestException("secret"))
    def test_network_failure_nonfatal(self, post):
        self.assertFalse(GotifyNotifier(CONFIG).send("x", "y"))

if __name__ == "__main__":
    unittest.main()
