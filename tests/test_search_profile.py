import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from job_search.config import PROFILE_VERSION
from job_search.filter import location_gate, relevance_gate, score_job


def job(**overrides):
    base = {
        "id": "test-job",
        "title": "",
        "company": "Test GmbH",
        "location": "Hamburg",
        "description": "",
        "matched_query": "",
        "source": "StepStone",
    }
    base.update(overrides)
    return base


def gate(candidate):
    return relevance_gate(candidate, score_job(candidate))


class SearchTrackTests(unittest.TestCase):
    def test_gkv_account_role_passes(self):
        candidate = job(
            title="Senior Account Manager GKV",
            description="Verantwortung fuer Krankenkassen und komplexe Ausschreibungen",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_healthcare_enterprise_role_passes(self):
        candidate = job(
            title="Senior Account Executive Healthcare",
            company="SAP SE",
            location="Germany (Remote)",
            description="Enterprise transformation for healthcare customers",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_enterprise_tech_transfer_passes(self):
        candidate = job(
            title="Enterprise Account Executive Data & AI",
            company="Public Cloud Group",
            location="Remote - Germany",
            description="Enterprise cloud platform and managed services",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_public_sector_requires_technology_context(self):
        candidate = job(
            title="Enterprise Account Executive Public Sector",
            company="Beispiel GmbH",
            description="Verkauf von Bueroausstattung an Verwaltungen",
        )
        self.assertEqual(gate(candidate), (False, "missing_search_track"))

    def test_priority_tech_public_sector_role_passes(self):
        candidate = job(
            title="Senior Account Executive - Public Sector",
            company="Genesys",
            location="Germany - Remote",
            description="Public services sales",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_senior_account_title_supplies_enterprise_scope(self):
        candidate = job(
            title="Senior Account Executive",
            company="DeepL",
            location="Germany - Remote",
            description="SaaS platform sales",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_generic_business_development_is_not_transfer_track(self):
        candidate = job(
            title="Business Development Manager SaaS",
            location="Remote Germany",
            description="Generischer Mittelstandsvertrieb",
        )
        self.assertEqual(gate(candidate), (False, "missing_search_track"))

    def test_freenow_business_development_health_passes(self):
        candidate = job(
            title="Business Development Manager - Health",
            company="FREENOW",
            location="Hamburg, Germany",
            description=(
                "Expansion im Bereich Patiententransport für Krankenhäuser und "
                "Arztpraxen und andere medizinische Einrichtungen; mehr als "
                "5 Jahre Vertrieb."
            ),
            source="Zielunternehmen",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_explicitly_confirmed_job_survives_low_ai_score(self):
        from job_search.main import email_gate, prepare_email_job

        candidate = job(
            id="8016144",
            title="Business Development Manager - Health",
            company="FREENOW",
            location="Hamburg, Germany",
            description="Patiententransport für Arztpraxen und Krankenhäuser",
            source="Zielunternehmen",
            score=20,
            keyword_score=35,
        )
        include, reason = email_gate(candidate)
        prepared = prepare_email_job(candidate, reason)

        self.assertEqual((include, reason), (True, "manual_review"))
        self.assertTrue(prepared["manual_review"])
        self.assertEqual(prepared["ai_action"], "Manuell prüfen")

    def test_ntt_senior_sales_manager_sap_passes_from_ba_summary(self):
        candidate = job(
            title="NTT DATA Deutschland SE: Senior Sales Manager SAP (w/m/x)",
            company="NTT DATA Deutschland SE",
            location="Hamburg, HAMBURG",
            description="ERP-Berater/in | Homeoffice moeglich; Umfang unklar",
            source="Arbeitsagentur",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_ntt_ai_gtm_insurance_dach_lead_passes_from_ba_summary(self):
        candidate = job(
            title="NTT DATA Deutschland SE: AI Go-to-Market Insurance DACH Lead (w/m/x)",
            company="NTT DATA Deutschland SE",
            location="Hamburg, HAMBURG",
            description="KI-Manager/in | Homeoffice moeglich; Umfang unklar",
            source="Arbeitsagentur",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_payor_contract_alternative_passes(self):
        candidate = job(
            title="Vertragsverhandler Gesundheit",
            company="rehaVital",
            description="Vertragsmanagement mit Krankenkassen und Kostentraegern",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_strategic_internal_gkv_role_passes(self):
        candidate = job(
            title="Leiter Digitalisierung",
            company="BARMER Krankenkasse",
            source="GKV Karriere",
            description="Digitale Transformation und IT-Strategie",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))


class CompanyAndLocationGateTests(unittest.TestCase):
    def test_exxeta_is_closed(self):
        candidate = job(
            title="Senior Account Manager Public Healthcare",
            company="Exxeta AG",
            description="Cloud und Public Sector",
        )
        self.assertEqual(gate(candidate), (False, "excluded_company"))

    def test_acture_is_not_reported_again(self):
        candidate = job(
            title="Key Account Manager Krankenkassen",
            company="Acture Germany GmbH",
            description="Ausschreibungen und Vergabe",
        )
        self.assertEqual(gate(candidate), (False, "already_applied"))

    def test_bitmarck_context_at_other_employer_remains_valid(self):
        candidate = job(
            title="Senior Account Manager GKV",
            company="Neue Health IT GmbH",
            description="Krankenkassen im BITMARCK-Umfeld, Cloud und Ausschreibungen",
        )
        self.assertEqual(gate(candidate), (True, "relevant"))

    def test_atacama_oscare_requirement_is_conditional_exclusion(self):
        candidate = job(
            title="Senior Account Manager GKV",
            company="atacama Software GmbH",
            description="Zwingend mehrjaehrige OSCARE-Erfahrung erforderlich",
        )
        self.assertEqual(gate(candidate), (False, "excluded_required_experience"))

    def test_berlin_hybrid_is_not_remote(self):
        candidate = job(
            title="Enterprise Account Executive Cloud",
            company="Cloud Tech GmbH",
            location="Berlin (Hybrid)",
            description="Enterprise SaaS platform",
        )
        self.assertEqual(location_gate(candidate), (False, "outside_hamburg_or_remote"))

    def test_germany_remote_is_allowed(self):
        candidate = job(location="Berlin / Remote Germany")
        self.assertEqual(location_gate(candidate), (True, "location_ok"))

    def test_remote_in_description_is_allowed_for_non_hamburg_location(self):
        candidate = job(
            location="Berlin",
            description="Die Position ist fully remote innerhalb Deutschlands.",
        )
        self.assertEqual(location_gate(candidate), (True, "location_ok"))

    def test_remote_without_germany_scope_is_not_allowed(self):
        candidate = job(location="Remote", description="Global enterprise role")
        self.assertEqual(location_gate(candidate), (False, "remote_country_unknown"))

    def test_us_remote_role_is_rejected(self):
        candidate = job(
            title="Enterprise Account Executive - US",
            location="Remote",
            description="Enterprise SaaS",
        )
        self.assertEqual(location_gate(candidate), (False, "foreign_location"))

    def test_junior_title_is_hard_excluded(self):
        candidate = job(
            title="Junior Account Executive Healthcare",
            description="Cloud platform",
        )
        self.assertEqual(gate(candidate), (False, "hard_exclude_title"))

    def test_known_salary_range_below_floor_is_excluded(self):
        candidate = job(
            title="Senior Account Manager GKV",
            description="Krankenkassen und Ausschreibungen",
            salary_min=60000,
            salary_max=80000,
        )
        self.assertEqual(gate(candidate), (False, "below_salary_floor"))


class SeenStateTests(unittest.TestCase):
    def test_all_evaluated_jobs_are_marked_seen(self):
        from job_search import main

        seen = {"previous-id"}
        main.mark_evaluated_jobs_seen(
            seen,
            [
                {"id": "reported-id"},
                {"id": "filtered-id"},
                {"id": ""},
                {},
            ],
        )

        self.assertEqual(seen, {"previous-id", "reported-id", "filtered-id"})

    def test_legacy_state_is_re_evaluated_and_saved_with_profile_version(self):
        from job_search import main

        with tempfile.TemporaryDirectory() as tmp:
            seen_file = Path(tmp) / "seen_jobs.json"
            seen_file.write_text(json.dumps(["old-id"]))
            with patch.object(main, "SEEN_FILE", seen_file):
                self.assertEqual(main.load_seen(), set())
                main.save_seen({"new-id"})
            state = json.loads(seen_file.read_text())
            self.assertEqual(state["profile_version"], PROFILE_VERSION)
            self.assertEqual(state["job_ids"], ["new-id"])

    def test_seen_file_can_live_outside_the_git_checkout(self):
        from importlib import reload
        from job_search import main

        with tempfile.TemporaryDirectory() as tmp:
            seen_file = Path(tmp) / "runtime" / "seen_jobs.json"
            with patch.dict(os.environ, {"JOBSUCHER_SEEN_FILE": str(seen_file)}):
                reloaded_main = reload(main)
                reloaded_main.save_seen({"cron-id"})
                self.assertEqual(reloaded_main.load_seen(), {"cron-id"})
        reload(main)


if __name__ == "__main__":
    unittest.main()
