import unittest
from datetime import date, timedelta
from unittest.mock import Mock

from job_search.scrapers.it_dienstleister import ITDienstleisterScraper


def response(payload):
    result = Mock()
    result.json.return_value = payload
    result.raise_for_status.return_value = None
    return result


class HostedBoardTests(unittest.TestCase):
    def setUp(self):
        self.scraper = ITDienstleisterScraper()

    def test_ashby_public_board_is_normalised(self):
        self.scraper.session.get = Mock(return_value=response({
            "jobs": [{
                "id": "ashby-1",
                "title": "Enterprise Account Executive Public Sector",
                "location": "Germany",
                "isRemote": True,
                "isListed": True,
                "jobUrl": "https://jobs.ashbyhq.com/example/ashby-1",
                "descriptionPlain": "Cloud platform for public services",
                "publishedAt": "2026-08-03T10:00:00Z",
            }]
        }))

        jobs = self.scraper._from_hosted_board_api(
            "DeepL", "https://jobs.ashbyhq.com/deepl"
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], "ashby-1")
        self.assertEqual(jobs[0]["location"], "Germany / Remote")
        self.assertEqual(jobs[0]["company"], "DeepL")

    def test_greenhouse_public_board_is_normalised(self):
        self.scraper.session.get = Mock(return_value=response({
            "jobs": [{
                "id": 42,
                "title": "Senior Business Development Manager Health",
                "location": {"name": "Hamburg"},
                "absolute_url": "https://job-boards.greenhouse.io/example/jobs/42",
                "content": "<p>Healthcare partnerships &amp; tenders</p>",
                "updated_at": "2026-08-03T11:00:00Z",
            }]
        }))

        jobs = self.scraper._from_hosted_board_api(
            "FREENOW", "https://job-boards.greenhouse.io/freenow"
        )

        self.assertEqual(jobs[0]["id"], "42")
        self.assertEqual(jobs[0]["description"], "Healthcare partnerships & tenders")

    def test_greenhouse_entity_escaped_html_is_cleaned(self):
        self.scraper.session.get = Mock(return_value=response({
            "jobs": [{
                "id": 8016144,
                "title": "Business Development Manager - Health",
                "location": {"name": "Hamburg, Germany"},
                "absolute_url": "https://example.com/health",
                "content": "&lt;p&gt;Patiententransport für Arztpraxen&lt;/p&gt;",
                "updated_at": "2026-08-05T08:00:00Z",
            }]
        }))

        jobs = self.scraper._from_greenhouse("FREENOW", "freenow")

        self.assertEqual(jobs[0]["description"], "Patiententransport für Arztpraxen")

    def test_lever_annual_eur_salary_is_exposed(self):
        self.scraper.session.get = Mock(return_value=response([{
            "id": "lever-1",
            "text": "Senior Account Executive Germany",
            "categories": {"location": "Germany"},
            "workplaceType": "remote",
            "descriptionPlain": "Enterprise SaaS",
            "hostedUrl": "https://jobs.lever.co/example/lever-1",
            "salaryRange": {
                "currency": "EUR",
                "interval": "year",
                "min": 95000,
                "max": 120000,
            },
        }]))

        jobs = self.scraper._from_hosted_board_api(
            "AppZen", "https://jobs.lever.co/appzen"
        )

        self.assertEqual(jobs[0]["salary_min"], 95000)
        self.assertEqual(jobs[0]["salary_max"], 120000)
        self.assertEqual(jobs[0]["location"], "Germany / Remote")

    def test_workday_public_board_is_paginated_and_normalised(self):
        first = response({
            "total": 2,
            "jobPostings": [{
                "title": "Senior Account Executive - Public Sector",
                "externalPath": "/job/Frankfurt/Senior-Account-Executive_R001",
                "locationsText": "Frankfurt, Germany / Flexible",
                "postedOn": "Posted 3 Days Ago",
                "bulletFields": ["Sales", "Public Sector"],
            }],
        })
        second = response({
            "total": 2,
            "jobPostings": [{
                "title": "Enterprise Account Executive Healthcare",
                "externalPath": "/job/Germany/Enterprise-Account-Executive_R002",
                "locationsText": "Germany - Remote",
                "postedOn": "Posted Today",
                "bulletFields": ["Enterprise", "Healthcare"],
            }],
        })
        self.scraper.session.post = Mock(side_effect=[first, second])

        jobs = self.scraper._from_hosted_board_api(
            "Genesys", "https://genesys.wd1.myworkdayjobs.com/Genesys"
        )

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["company"], "Genesys")
        self.assertEqual(jobs[0]["location"], "Frankfurt, Germany / Flexible")
        self.assertEqual(
            jobs[0]["posted_date"],
            (date.today() - timedelta(days=3)).isoformat(),
        )
        self.assertEqual(jobs[1]["posted_date"], date.today().isoformat())

    def test_sap_successfactors_rows_are_normalised(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("""
            <table><tr class="data-row">
              <td><a class="jobTitle-link" href="/job/Hamburg/Senior-Account-Manager/1/">
                Senior Account Manager Public Sector
              </a></td>
              <td class="jobLocation">Hamburg, DE</td>
            </tr></table>
        """, "lxml")

        jobs = self.scraper._from_sap(
            soup, "https://jobs.sap.com/go/Germany/8806101/", "SAP"
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["location"], "Hamburg, DE")
        self.assertEqual(
            jobs[0]["url"],
            "https://jobs.sap.com/job/Hamburg/Senior-Account-Manager/1/",
        )

    def test_sap_all_result_pages_are_followed(self):
        from bs4 import BeautifulSoup

        first = BeautifulSoup("""
            <table><tr class="data-row">
              <td><a class="jobTitle-link" href="/job/Hamburg/First/1/">First Role</a></td>
              <td class="jobLocation">Hamburg, DE</td>
            </tr></table>
            <a class="paginationItemLast" href="/go/Germany/8806101/50/?q=">Last</a>
        """, "lxml")
        page_2 = Mock(text="""
            <table><tr class="data-row">
              <td><a class="jobTitle-link" href="/job/Germany/Second/2/">Second Role</a></td>
              <td class="jobLocation">Germany</td>
            </tr></table>
        """)
        page_3 = Mock(text="""
            <table><tr class="data-row">
              <td><a class="jobTitle-link" href="/job/Germany/Third/3/">Third Role</a></td>
              <td class="jobLocation">Germany</td>
            </tr></table>
        """)
        self.scraper.get = Mock(side_effect=[page_2, page_3])

        jobs = self.scraper._from_sap_pages(
            first, "https://jobs.sap.com/go/Germany/8806101/", "SAP"
        )

        self.assertEqual([item["title"] for item in jobs], ["First Role", "Second Role", "Third Role"])
        self.assertEqual(self.scraper.get.call_count, 2)

    def test_generic_html_job_class_string_is_parsed(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("""
            <div class="portlet job-offer">
              <a class="job-offer-content" href="/de?companyId=x&id=42">
                <h2>Senior Business Development Manager Healthcare</h2>
              </a>
              <div class="location">Hamburg / Remote Germany</div>
            </div>
        """, "lxml")

        jobs = self.scraper._from_html(
            soup,
            "https://jobapplication.hrworks.de/de?companyId=x",
            "ZOTZ|KLIMAS",
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["location"], "Hamburg / Remote Germany")
        self.assertIn("id=42", jobs[0]["url"])

    def test_hrworks_location_and_work_model_are_normalised(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup("""
            <div class="portlet light bordered">
              <a class="job-offer-content" href="/de?companyId=x&id=42"
                 title="Senior Business Development Manager Healthcare">
                <h2>Senior Business Development Manager Healthcare</h2>
              </a>
              <a href="https://maps.example/dusseldorf">
                <i class="icon icomoon-location"></i>
                <span>Düsseldorf, Deutschland</span>
              </a>
              <a href=""><i class="icon icomoon-home"></i><span>Hybrides Arbeiten</span></a>
            </div>
        """, "lxml")

        jobs = self.scraper._from_hrworks(
            soup,
            "https://jobapplication.hrworks.de/de?companyId=x",
            "ZOTZ|KLIMAS",
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["location"], "Düsseldorf, Deutschland / Hybrides Arbeiten")
        self.assertIn("id=42", jobs[0]["url"])


if __name__ == "__main__":
    unittest.main()
