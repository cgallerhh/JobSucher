import unittest

from job_search.ai_scorer import _normalise_ai_result


class AIScorerValidationTests(unittest.TestCase):
    def test_unsupported_location_claim_is_removed(self):
        job = {
            "title": "Business Development Manager - Health",
            "location": "Hamburg, Germany",
        }
        result = {
            "reason": "Gesundheitsfokus mit Ausbau von Kundenbeziehungen.",
            "strengths": ["Expansion im Gesundheitssektor"],
            "concerns": [
                "Standort Berlin nicht ideal",
                "Kein spezifischer GKV-Fokus",
            ],
            "action": "Ueberspringen",
        }

        normalised = _normalise_ai_result(job, result, 65)

        self.assertEqual(normalised["ai_concerns"], ["Kein spezifischer GKV-Fokus"])
        self.assertEqual(normalised["ai_action"], "Ueberspringen")

    def test_action_is_derived_from_score_not_model_text(self):
        job = {"title": "Senior Account Executive Healthcare", "location": "Hamburg"}
        result = {
            "reason": "Starke Healthcare-Passung.",
            "strengths": [],
            "concerns": [],
            "action": "Ueberspringen",
        }

        normalised = _normalise_ai_result(job, result, 82)

        self.assertEqual(normalised["ai_action"], "Sofort bewerben")


if __name__ == "__main__":
    unittest.main()
