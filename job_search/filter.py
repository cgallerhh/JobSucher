"""Deterministisches Scoring und harte Gates fuer das Master-Suchprofil."""

from __future__ import annotations

import re
import unicodedata

from .config import (
    APPLIED_COMPANIES,
    CONDITIONAL_COMPANY_EXCLUSIONS,
    EXCLUDED_COMPANIES,
    MIN_SCORE,
    NEGATIVE_KEYWORDS,
    POSITIVE_KEYWORDS,
    PROFILE,
    PRIORITY_COMPANIES,
)


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
    "werkstudent",
    "praktikant",
    "trainee",
    "junior",
]

HARD_EXCLUDE_ROLE_CONTEXT = [
    "automotive",
    "automobil",
    "autohaus",
    "autohandel",
    "fleet management",
    "flottenmanagement",
    "maschinenbau",
    "produktion",
    "logistik",
    "lager",
]

COMMERCIAL_TITLE_KEYWORDS = [
    "strategic account manager",
    "senior account manager",
    "key account manager",
    "senior sales manager",
    "sales director",
    "enterprise account executive",
    "senior account executive",
    "named account executive",
    "business development manager",
    "client partner",
    "partner manager",
    "alliance manager",
    "commercial lead",
    "go-to-market",
    "head of sales",
    "head of business development",
    "leiter vertrieb",
]

ENTERPRISE_TRANSFER_TITLE_KEYWORDS = [
    "strategic account manager",
    "senior account manager",
    "senior sales manager",
    "sales director",
    "enterprise account executive",
    "senior account executive",
    "named account executive",
    "client partner",
    "commercial lead",
    "head of sales",
]

ALTERNATIVE_COMMERCIAL_TITLE_KEYWORDS = [
    "market access",
    "payer partnership",
    "payor partnership",
    "strategic partnership",
    "vertragsmanager",
    "vertragsreferent",
    "vertragsverhandler",
    "partnermanager",
]

INTERNAL_GKV_STRATEGIC_TITLE_KEYWORDS = [
    "leiter digital",
    "leiter it-strategie",
    "leiter it steuerung",
    "leiter it-steuerung",
    "leiter unternehmensentwicklung",
    "leiter vergabemanagement",
    "leiter einkauf",
    "bereichsleiter digital",
    "head of digital",
    "head of it strategy",
    "head of it governance",
    "head of procurement",
    "chief digital officer",
    "cdo",
]

INTERNAL_GKV_COMPANY_KEYWORDS = [
    "krankenkasse",
    "bkk",
    "ikk",
    "dak",
    "techniker krankenkasse",
    "barmer",
    "hkk",
    "kaufmännische krankenkasse",
    "kaufmaennische krankenkasse",
]

GKV_DOMAIN_KEYWORDS = [
    "gkv",
    "gesetzliche krankenversicherung",
    "krankenkasse",
    "krankenkassen",
    "bkk",
    "ikk",
    "sozialversicherung",
    "sgb v",
    "payer",
    "payor",
    "kostenträger",
    "kostentraeger",
    "bitmarck",
    "iskv",
]

HEALTH_DOMAIN_KEYWORDS = [
    "healthcare",
    "health care",
    "health it",
    "health-it",
    "gesundheitswesen",
    "gesundheits-it",
    "ehealth",
    "e-health",
    "digital health",
    "public healthcare",
    "gesundheitssektor",
    "patiententransport",
    "medizinische einrichtung",
    "krankenhaus",
    "dialysezentrum",
    "telematikinfrastruktur",
]

PUBLIC_DOMAIN_KEYWORDS = [
    "public sector",
    "public services",
    "öffentlicher sektor",
    "oeffentlicher sektor",
    "government",
    "behörde",
    "behoerde",
    "verwaltung",
    "sozialversicherung",
]

TECH_CONTEXT_KEYWORDS = [
    "software",
    "saas",
    "cloud",
    "managed services",
    "cybersecurity",
    "cyber security",
    "security",
    "compliance",
    "kritis",
    "nis2",
    "data & ai",
    "data and ai",
    "data/ai",
    "artificial intelligence",
    "künstliche intelligenz",
    "kuenstliche intelligenz",
    "genai",
    "plattform",
    "platform",
    "digital transformation",
    "digitale transformation",
    "it-system",
    "infrastruktur",
    "sap",
]

ENTERPRISE_SCOPE_KEYWORDS = [
    "enterprise",
    "senior account",
    "strategic account",
    "named account",
    "major account",
    "large account",
    "large enterprise",
    "großkunden",
    "grosskunden",
    "c-level",
    "buying committee",
    "complex sales",
    "large deal",
    "sales director",
    "senior sales manager",
]

PAYER_COMMERCIAL_KEYWORDS = [
    "ausschreibung",
    "vergabe",
    "tender",
    "vertrag",
    "market access",
    "payer",
    "payor",
    "kostenträger",
    "kostentraeger",
    "versicherung",
    "insurer",
]

ENTERPRISE_TECH_COMPANIES = [
    "DeepL",
    "SAP",
    "Genesys",
    "Veeam",
    "Salesforce",
    "SoftwareOne",
    "Amazon Web Services",
    "Camunda",
    "Public Cloud Group",
    "AppZen",
    "Gartner",
    "SnapLogic",
    "Ashby",
    "360Learning",
    "Conceptboard",
    "NTT DATA",
]

REMOTE_LOCATION_KEYWORDS = [
    "remote",
    "fully remote",
    "100% homeoffice",
    "100 % homeoffice",
    "work from anywhere",
]

ALLOWED_LOCATION_KEYWORDS = [
    "hamburg",
    "norderstedt",
    "ahrensburg",
    "pinneberg",
    "wedel",
    "reinbek",
    "glinde",
    "barsbüttel",
    "barsbuettel",
    "schenefeld",
    "quickborn",
    "halstenbek",
    "bönningstedt",
    "boenningstedt",
    "neu wulmstorf",
    "seevetal",
    "buchholz",
    "winsen",
    "geesthacht",
    "elmshorn",
    "stade",
]

COUNTRYWIDE_LOCATION_KEYWORDS = [
    "deutschland",
    "germany",
    "bundesweit",
    "deutschlandweit",
]

FOREIGN_LOCATION_KEYWORDS = [
    "united states",
    "usa",
    "u.s.",
    "united kingdom",
    "london",
    "england",
    "ireland",
    "france",
    "paris",
    "spain",
    "madrid",
    "italy",
    "milan",
    "netherlands",
    "amsterdam",
    "switzerland",
    "zürich",
    "zurich",
    "austria",
    "wien",
    "vienna",
    "poland",
    "warsaw",
    "remote us",
    "remote - us",
    "remote usa",
    "remote - usa",
    "europe",
    "emea",
    "worldwide",
    "global remote",
]

US_STATE_PATTERN = re.compile(
    r"(,\s?(al|ak|az|ar|ca|co|ct|fl|ga|hi|ia|id|il|in|ks|ky|la|ma|md|me|"
    r"mi|mn|mo|ms|mt|nc|nd|ne|nh|nj|nm|nv|ny|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|"
    r"va|vt|wa|wi|wv|wy)\b)|\b(united states|usa|u\.s\.)\b"
)


def _normalise(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().replace("&", " und ")
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def _company_matches(company: str, candidates: list[str]) -> bool:
    company_norm = f" {_normalise(company)} "
    if not company_norm.strip():
        return False
    for candidate in candidates:
        candidate_norm = _normalise(candidate)
        if candidate_norm and f" {candidate_norm} " in company_norm:
            return True
    return False


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _title(job: dict) -> str:
    return (job.get("title") or "").lower()


def _job_text(job: dict) -> str:
    return " ".join(
        [
            job.get("title", ""),
            job.get("description", ""),
            job.get("company", ""),
            job.get("location", ""),
        ]
    ).lower()


def _combined_text(job: dict) -> str:
    return f"{_job_text(job)} {job.get('matched_query', '')}".lower()


def company_gate(job: dict) -> tuple[bool, str]:
    """Unterdruecke geschlossene Firmen und bereits laufende Bewerbungen."""
    company = job.get("company", "")
    if _company_matches(company, APPLIED_COMPANIES):
        return False, "already_applied"
    if _company_matches(company, EXCLUDED_COMPANIES):
        return False, "excluded_company"

    role_text = f"{_title(job)} {job.get('description', '')}".lower()
    for company_name, requirements in CONDITIONAL_COMPANY_EXCLUSIONS.items():
        if _company_matches(company, [company_name]) and _contains_any(role_text, requirements):
            return False, "excluded_required_experience"
    return True, "company_ok"


def location_gate(job: dict) -> tuple[bool, str]:
    """Erlaube Hamburg/Umkreis und echte bundesweite Remote-Rollen."""
    location = (job.get("location") or "").lower().strip()
    title = (job.get("title") or "").lower()
    description = (job.get("description") or "").lower()
    remote_context = f"{location} {description}"
    country_context = f" {title} {location} {description} "
    if not location:
        return True, "location_unknown"
    if re.search(r"(^|[\s/,(\-])us($|[\s/),\-])", f"{title} {location}"):
        return False, "foreign_location"
    if US_STATE_PATTERN.search(location) or _contains_any(location, FOREIGN_LOCATION_KEYWORDS):
        return False, "foreign_location"
    if _contains_any(location, ALLOWED_LOCATION_KEYWORDS):
        return True, "location_ok"
    if _contains_any(remote_context, REMOTE_LOCATION_KEYWORDS):
        if _contains_any(country_context, COUNTRYWIDE_LOCATION_KEYWORDS + ["dach"]):
            return True, "location_ok"
        return False, "remote_country_unknown"
    if location in COUNTRYWIDE_LOCATION_KEYWORDS:
        return True, "location_ok"
    if _contains_any(location, ["bundesweit", "deutschlandweit"]):
        return True, "location_ok"
    return False, "outside_hamburg_or_remote"


def score_job(job: dict) -> int:
    """Berechne einen transparenten Keyword-Score von 0 bis 100."""
    text = _job_text(job)
    score = 0

    for keyword, points in POSITIVE_KEYWORDS.items():
        if keyword.lower() in text:
            score += points
    for keyword, penalty in NEGATIVE_KEYWORDS.items():
        # "Arzt" as a plain substring incorrectly penalises Healthcare-Sales
        # descriptions containing "Arztpraxen". Medical job titles remain a
        # hard exclusion; in free text this short term must be a complete word.
        if keyword.casefold() == "arzt":
            matched = bool(re.search(r"\barzt\b", text))
        else:
            matched = keyword.lower() in text
        if matched:
            score += penalty

    if _company_matches(job.get("company", ""), PRIORITY_COMPANIES):
        score += 8

    return max(0, min(100, score))


def is_relevant(score: int) -> bool:
    return score >= MIN_SCORE


def relevance_gate(job: dict, score: int) -> tuple[bool, str]:
    """Pruefe Rollen-, Branchen-, Senioritaets-, Firmen- und Standort-Fit."""
    title = _title(job)
    combined = _combined_text(job)
    company = job.get("company", "")
    source = job.get("source", "")

    passes_company, company_reason = company_gate(job)
    if not passes_company:
        return False, company_reason
    if _contains_any(title, HARD_EXCLUDE_TITLE_KEYWORDS):
        return False, "hard_exclude_title"
    title_query = f"{title} {job.get('matched_query', '')}".lower()
    if _contains_any(title_query, HARD_EXCLUDE_ROLE_CONTEXT):
        return False, "hard_exclude_domain"

    passes_location, location_reason = location_gate(job)
    if not passes_location:
        return False, location_reason

    try:
        advertised_salary_max = float(job.get("salary_max") or 0)
    except (TypeError, ValueError):
        advertised_salary_max = 0
    if advertised_salary_max and advertised_salary_max < PROFILE["salary_min"]:
        return False, "below_salary_floor"

    has_commercial_title = _contains_any(title, COMMERCIAL_TITLE_KEYWORDS)
    has_transfer_title = _contains_any(title, ENTERPRISE_TRANSFER_TITLE_KEYWORDS)
    has_alternative_title = _contains_any(title, ALTERNATIVE_COMMERCIAL_TITLE_KEYWORDS)
    has_internal_gkv_title = _contains_any(title, INTERNAL_GKV_STRATEGIC_TITLE_KEYWORDS)

    has_gkv = _contains_any(combined, GKV_DOMAIN_KEYWORDS)
    # "Health" is a common vertical label in otherwise German job ads. Keep
    # the generic English word title-scoped so benefit text such as "health
    # insurance" cannot turn an unrelated role into a healthcare match.
    has_health = _contains_any(combined, HEALTH_DOMAIN_KEYWORDS) or bool(
        re.search(r"\bhealth\b", title)
    )
    has_public = _contains_any(combined, PUBLIC_DOMAIN_KEYWORDS)
    has_tech = _contains_any(combined, TECH_CONTEXT_KEYWORDS) or bool(
        re.search(r"\bai\b", combined)
    )
    is_gtm_lead = "go-to-market" in title and "lead" in title
    has_enterprise_scope = (
        _contains_any(combined, ENTERPRISE_SCOPE_KEYWORDS) or is_gtm_lead
    )
    has_payer_commercial = _contains_any(combined, PAYER_COMMERCIAL_KEYWORDS)
    is_priority_tech_company = _company_matches(company, ENTERPRISE_TECH_COMPANIES)
    is_gkv_employer = source == "GKV Karriere" or _company_matches(
        company, INTERNAL_GKV_COMPANY_KEYWORDS
    )

    is_gkv_commercial = has_commercial_title and has_gkv
    is_healthcare_commercial = has_commercial_title and has_health
    is_public_tech_commercial = (
        has_commercial_title
        and has_public
        and (has_tech or is_priority_tech_company)
    )
    is_enterprise_tech_transfer = (
        (has_transfer_title or is_gtm_lead)
        and has_enterprise_scope
        and (has_tech or is_priority_tech_company)
    )
    is_payor_alternative = (
        has_alternative_title
        and (has_gkv or has_health)
        and has_payer_commercial
    )
    is_internal_gkv_strategy = is_gkv_employer and has_internal_gkv_title

    if source == "GKV Karriere" and not has_internal_gkv_title:
        return False, "internal_gkv_not_strategic"
    if not (has_commercial_title or has_alternative_title or has_internal_gkv_title):
        return False, "missing_target_role"
    if not (
        is_gkv_commercial
        or is_healthcare_commercial
        or is_public_tech_commercial
        or is_enterprise_tech_transfer
        or is_payor_alternative
        or is_internal_gkv_strategy
    ):
        return False, "missing_search_track"
    if not is_relevant(score):
        return False, "below_score"

    return True, "relevant"
