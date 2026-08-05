import base64
import unittest
from unittest.mock import Mock, patch
from urllib.parse import quote

from job_search.scrapers.arbeitsagentur import ArbeitsagenturScraper


class ArbeitsagenturV6Tests(unittest.TestCase):
    def test_v6_response_is_normalised(self):
        response = Mock(ok=True)
        response.json.return_value = {
            "ergebnisliste": [
                {
                    "stellenangebotsTitel": "Enterprise Account Executive Cloud",
                    "firma": "Beispiel Cloud GmbH",
                    "referenznummer": "REF-1",
                    "externeURL": "https://example.com/job/1",
                    "stellenlokationen": [
                        {"adresse": {"ort": "Hamburg", "region": "HAMBURG"}}
                    ],
                    "hauptberuf": "Account-Manager/in",
                    "gehaltsspanneVon": 95000,
                    "gehaltsspanneBis": 120000,
                    "homeofficemoeglich": True,
                    "datumErsteVeroeffentlichung": "2026-08-03",
                }
            ]
        }

        scraper = ArbeitsagenturScraper()
        scraper._api_session.get = Mock(return_value=response)
        with patch("job_search.scrapers.arbeitsagentur.time.sleep"):
            jobs = scraper.fetch(["Enterprise Account Executive Cloud"], "Hamburg")

        self.assertEqual(len(jobs), 1)
        result = jobs[0]
        self.assertEqual(result["title"], "Enterprise Account Executive Cloud")
        self.assertEqual(result["company"], "Beispiel Cloud GmbH")
        self.assertEqual(result["location"], "Hamburg, HAMBURG")
        self.assertEqual(result["salary_min"], 95000)
        self.assertEqual(result["salary_max"], 120000)
        self.assertIn("Jahresgehalt 95000 bis 120000 EUR", result["description"])
        self.assertEqual(result["posted_date"], "2026-08-03")

        request_params = scraper._api_session.get.call_args.kwargs["params"]
        self.assertEqual(request_params["veroeffentlichtseit"], "14")
        self.assertEqual(request_params["angebotsart"], "1")
        self.assertEqual(request_params["pav"], "false")
        self.assertEqual(request_params["zeitarbeit"], "false")

    def test_candidate_is_enriched_with_v4_details(self):
        response = Mock()
        response.raise_for_status = Mock()
        response.json.return_value = {
            "stellenangebotsBeschreibung": (
                "Strategische Key Accounts, komplexe SAP- und Cloud-Sales-Cycles"
            ),
            "homeofficeprozent": 100,
        }
        scraper = ArbeitsagenturScraper()
        scraper._api_session.get = Mock(return_value=response)

        result = scraper.enrich_details({
            "id": "REF-DETAIL",
            "title": "Senior Sales Manager SAP",
            "location": "Hamburg, HAMBURG",
            "description": "ERP-Berater/in",
        })

        self.assertIn("SAP- und Cloud-Sales-Cycles", result["description"])
        self.assertIn("Remote Deutschland", result["location"])
        self.assertTrue(result["detail_enriched"])
        encoded_ref = quote(
            base64.b64encode(b"REF-DETAIL").decode("ascii"), safe=""
        )
        self.assertIn(encoded_ref, scraper._api_session.get.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
