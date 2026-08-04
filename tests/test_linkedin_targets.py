import unittest

from job_search.scrapers.linkedin import _target_company


class LinkedInTargetTests(unittest.TestCase):
    def test_current_priority_company_is_recognised(self):
        self.assertEqual(_target_company("DeepL SE"), "DeepL")

    def test_closed_company_is_not_a_linkedin_target(self):
        self.assertEqual(_target_company("Exxeta AG"), "")


if __name__ == "__main__":
    unittest.main()
