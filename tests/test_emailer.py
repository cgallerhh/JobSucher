import os
import unittest
from unittest.mock import patch

from job_search.emailer import build_empty_html, build_html, send_email


class EmailContentTests(unittest.TestCase):
    def test_manual_review_has_consistent_label_and_action(self):
        html = build_html(
            [
                {
                    "title": "Business Development Manager - Health",
                    "company": "FREENOW",
                    "location": "Hamburg, Germany",
                    "source": "Zielunternehmen",
                    "score": 65,
                    "manual_review": True,
                    "ai_action": "Ueberspringen",
                    "ai_reason": "Gesundheitsfokus ohne direkten GKV-Bezug.",
                    "ai_concerns": ["Kein spezifischer GKV-Fokus"],
                    "url": "https://example.com/job",
                }
            ],
            "Christian Galler",
        )

        self.assertIn("Stelle zur Prüfung", html)
        self.assertIn("Explizit zur manuellen Prüfung aufgenommen", html)
        self.assertIn("Manuell prüfen", html)
        self.assertNotIn("Ueberspringen", html)
        self.assertNotIn("Warum passend", html)

    def test_empty_report_describes_automated_jobsucher_run(self):
        html = build_empty_html("Christian Galler")

        self.assertIn("automatisierte JobSucher-Lauf", html)
        self.assertNotIn("GitHub-Lauf", html)
        self.assertIn("Projektstatus", html)


class EmailTransportTests(unittest.TestCase):
    @patch("job_search.emailer.smtplib.SMTP_SSL")
    def test_separate_smtp_sender_overrides_legacy_gmail_settings(self, smtp_ssl):
        server = smtp_ssl.return_value.__enter__.return_value
        environment = {
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": "465",
            "SMTP_USER": "jobsucher@example.com",
            "SMTP_PASSWORD": "app-password",
            "SMTP_FROM_EMAIL": "bot@example.com",
            "SMTP_FROM_NAME": "JobSucher",
            "GMAIL_USER": "legacy@gmail.com",
            "GMAIL_APP_PASSWORD": "legacy-password",
        }

        with patch.dict(os.environ, environment, clear=True):
            send_email("christian@example.com", "Test", "<p>Test</p>")

        smtp_ssl.assert_called_once_with("smtp.example.com", 465)
        server.login.assert_called_once_with("jobsucher@example.com", "app-password")
        sender, recipients, message = server.sendmail.call_args.args
        self.assertEqual(sender, "bot@example.com")
        self.assertEqual(recipients, ["christian@example.com"])
        self.assertIn("From: JobSucher <bot@example.com>", message)


if __name__ == "__main__":
    unittest.main()
