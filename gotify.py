"""Optional Gotify notifications for ImageMaid (experimental)."""
import re
import requests


def _enabled(value):
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _escape(value):
    return str(value).replace("\\", "\\\\").replace("*", "\\*").replace("_", "\\_").replace("`", "\\`")


def _short_error(error):
    """Keep verbose validation details in ImageMaid logs, not push messages."""
    text = str(error).strip().splitlines()[0]
    if text.startswith("Mode Error:") and " Invalid." in text:
        mode = text[len("Mode Error:"):].split(" Invalid.", 1)[0].strip()
        return f"Invalid execution mode: {mode}"
    return text[:500] + ("..." if len(text) > 500 else "")


def _summary(description, report, error=None):
    """Format existing ImageMaid report rows without changing cleanup logic."""
    lines = [f"**{_escape(description)}**", "", "### Results"]
    metrics = []
    sections = []
    for row in report:
        for name, value in row:
            name = str(name or "").strip()
            value = str(value or "").strip()
            if name and value:
                metrics.append((name, value))
            elif name:
                sections.append(name)
            elif value:
                # ImageMaid emits a combined string for bloat image counts/space.
                match = re.fullmatch(
                    r"(.+?) of (Potential Recovery|Space Recovered) "
                    r"(?:Reporting|Removing|Moving) (\d+) Files", value,
                    flags=re.IGNORECASE,
                )
                if match:
                    metrics.extend([
                        ("Bloat Images Found" if "report" in description.lower() else "Bloat Images Processed", match.group(3)),
                        (match.group(2), match.group(1)),
                    ])
                else:
                    metrics.append(("Details", value))
    for key, value in metrics:
        lines.append(f"- **{_escape(key)}:** {_escape(value)}")
    if not metrics:
        lines.append("- No report metrics available")
    if error:
        lines.extend(["", "### Error", _escape(_short_error(error))])
    elif "report mode" in description.lower():
        lines.extend(["", "Report mode — No images removed."])
    return "\n".join(lines)


class GotifyNotifier:
    def __init__(self, config=None):
        self.config = config if config is not None else {}
        self.enabled = _enabled(self.config.get("GOTIFY_ENABLED", "False"))
        self.url = str(self.config.get("GOTIFY_URL", "")).strip().rstrip("/")
        self.token = str(self.config.get("GOTIFY_TOKEN", "")).strip()
        try:
            self.priority = int(self.config.get("GOTIFY_PRIORITY", 5))
        except (TypeError, ValueError):
            self.priority = 5

    def send(self, title, message, event="end"):
        if not self.enabled or not self.url or not self.token:
            return False
        if not _enabled(self.config.get(f"GOTIFY_NOTIFY_{event.upper()}", "True")):
            return False
        if not self.url.startswith(("https://", "http://")):
            print("ImageMaid: Gotify URL must begin with http:// or https://")
            return False
        try:
            response = requests.post(
                f"{self.url}/message",
                headers={"X-Gotify-Key": self.token},
                json={
                    "title": title,
                    "message": str(message),
                    "priority": self.priority,
                    "extras": {"client::display": {"contentType": "text/markdown"}},
                },
                timeout=10,
            )
            response.raise_for_status()
            return True
        except requests.RequestException:
            print("ImageMaid: Could not send Gotify notification")
            return False

    def send_run_summary(self, description, report, error=None):
        return self.send(
            "ImageMaid - Run Failed" if error else "ImageMaid - Run Completed",
            _summary(description, report, error),
            event="error" if error else "end",
        )
