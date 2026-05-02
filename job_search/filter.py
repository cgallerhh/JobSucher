"""Relevance scoring and hard relevance gates for job listings."""
from .config import MIN_SCORE, NEGATIVE_KEYWORDS, POSITIVE_KEYWORDS

HARD_EXCLUDE_TITLE_KEYWORDS = [
    "sachbearbeiter",
    "sachbearbeitung",
    "kundenberater",
    "kundenberatung",
    "kundenservice",
    "servicecenter",
    "call center",
    "sozialversicherungsfachangestell",
    "leistungssachbearbeiter",
    "fallmanager",
    "case manager",
    "pflege",
    "arzt",
    "ärztin",
    "medizinische fachangestellte",
    "therapeut",
    "buchhaltung",
    "controller",
    "recruiter",
]

HARD_EXCLUDE_TEXT_KEYWORDS = [
    "automotive",
    "automobil",
    "autohaus",
    "autohandel",
    "fahrzeug",
    "fahrzeuge",
    "fleet management",
    "flottenmanagement",
    "leasing",
    "maschinenbau",
    "produktion",
    "logistik",
    "lager",
]

SALES_ROLE_KEYWORDS = [
    "account manager",
    "key account",
    "sales manager",
    "sales director",
    "account executive",
    "business development",
    "client partner",
    "partner manager",
    "alliance manager",
    "commercial lead",
    "go-to-market",
    "vertrieb",
    "neukundengewinnung",
    "großkunden",
    "enterprise sales",
]

STRATEGIC_ROLE_KEYWORDS = [
    "leiter",
    "bereichsleiter",
    "head of",
    "director",
    "chief",
    "cdo",
    "lead",
    "principal",
    "strategie",
    "transformation",
    "digitalisierung",
    "innovation",
    "it-steuerung",
    "it-strategie",
    "it-governance",
    "demand management",
    "portfolio",
    "programmleiter",
    "produkt",
    "procurement",
    "sourcing",
    "vendor manager",
    "vergabemanagement",
    "tender manager",
    "dienstleistersteuerung",
]

DOMAIN_KEYWORDS = [
    "gkv",
    "gesetzliche krankenversicherung",
    "krankenkasse",
    "krankenkassen",
    "bkk",
    "ikk",
    "dak",
    "tk ",
    "public sector",
    "öffentlicher sektor",
    "behörde",
    "behörden",
    "ögd",
    "sozialversicherung",
    "sgb v",
    "ehealth",
    "digital health",
    "healthcare",
    "healthcare it",
    "health it",
    "gesundheit",
    "gesundheitswesen",
    "gesundheits-it",
    "telematikinfrastruktur",
    "ti 2.0",
    "bitmarck",
    "iskv",
]

INTERNAL_GKV_STRATEGIC_TITLE_KEYWORDS = [
    "leiter",
    "bereichsleiter",
    "head of",
    "director",
    "chief",
    "cdo",
    "digital",
    "digitalisierung",
    "e-health",
    "ehealth",
    "it-strategie",
    "it-steuerung",
    "it-governance",
    "it-portfolio",
    "cloud",
    "innovation",
    "strategie",
    "unternehmensentwicklung",
    "vorstandsstab",
    "chief of staff",
    "vergabemanagement",
    "tender",
    "sourcing",
    "procurement",
    "dienstleistersteuerung",
    "vendor",
    "produkt",
    "omnichannel",
]

INTERNAL_GKV_COMPANY_KEYWORDS = [
    "krankenkasse",
    "bkk",
    "ikk",
    "aok",
    "dak",
    "techniker krankenkasse",
    "tk ",
    "hkk",
    "barmer",
    "kaufmännische krankenkasse",
]


def _text(job: dict) -> str:
    return " ".join(
        [
            job.get("title", ""),
            job.get("description", ""),
            job.get("company", ""),
            job.get("location", ""),
            job.get("matched_query", ""),
        ]
    ).lower()


def _job_text(job: dict) -> str:
    return " ".join(
        [
            job.get("title", ""),
            job.get("description", ""),
            job.get("company", ""),
            job.get("location", ""),
        ]
    ).lower()


def _title(job: dict) -> str:
    return job.get("title", "").lower()


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def score_job(job: dict) -> int:
    """Return a relevance score 0–100 for a job dict."""
    text = _text(job)

    score = 0

    for keyword, points in POSITIVE_KEYWORDS.items():
        if keyword.lower() in text:
            score += points

    for keyword, penalty in NEGATIVE_KEYWORDS.items():
        if keyword.lower() in text:
            score += penalty  # penalty is already negative

    return max(0, min(100, score))


def is_relevant(score: int) -> bool:
    return score >= MIN_SCORE


def relevance_gate(job: dict, score: int) -> tuple[bool, str]:
    """Strictly decide whether a scored job should be shown in the email."""
    text = _text(job)
    job_text = _job_text(job)
    query_text = job.get("matched_query", "").lower()
    title = _title(job)
    source = job.get("source", "")

    if _contains_any(title, HARD_EXCLUDE_TITLE_KEYWORDS):
        return False, "hard_exclude_title"
    if _contains_any(text, HARD_EXCLUDE_TEXT_KEYWORDS):
        return False, "hard_exclude_domain"

    if source == "GKV Karriere":
        if not _contains_any(title, INTERNAL_GKV_STRATEGIC_TITLE_KEYWORDS):
            return False, "internal_gkv_not_strategic"
        return is_relevant(score), "below_score" if not is_relevant(score) else "relevant"

    company = job.get("company", "").lower()
    if _contains_any(company, INTERNAL_GKV_COMPANY_KEYWORDS):
        if not _contains_any(title, INTERNAL_GKV_STRATEGIC_TITLE_KEYWORDS):
            return False, "internal_gkv_not_strategic"

    has_sales_role = _contains_any(job_text, SALES_ROLE_KEYWORDS)
    has_strategic_role = _contains_any(title, STRATEGIC_ROLE_KEYWORDS)
    has_query_role = _contains_any(query_text, SALES_ROLE_KEYWORDS + STRATEGIC_ROLE_KEYWORDS)
    has_role = has_sales_role or has_strategic_role or has_query_role
    has_domain = _contains_any(job_text, DOMAIN_KEYWORDS)
    has_query_domain = _contains_any(query_text, DOMAIN_KEYWORDS)
    has_effective_domain = has_domain or has_query_domain

    if not has_role:
        return False, "missing_role"
    if not has_effective_domain:
        return False, "missing_domain"
    if not is_relevant(score):
        return False, "below_score"

    return True, "relevant"
