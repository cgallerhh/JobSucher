"""
Job search configuration – Christian Galler
Senior Sales Manager | GKV & Public Sector IT | Hamburg
"""

PROFILE = {
    "name": "Christian Galler",
    "email": "christian.galler@gmail.de",
    "location": "Hamburg",
    "radius_km": 50,
    "remote_ok": True,
    "salary_min": 90000,
}

# Locations to search – first entry = local, "Deutschland" = nationwide/remote
SEARCH_LOCATIONS = ["Hamburg", "Deutschland"]

# Queries for external job boards (Arbeitsagentur, LinkedIn)
# → suche nach Sales-/BD-Rollen bei IT-Unternehmen im GKV-Umfeld
EXTERNAL_QUERIES = [
    "Senior Account Manager GKV",
    "Sales Manager Gesundheitswesen IT",
    "Key Account Manager Public Sector IT",
    "Business Development Manager Healthcare IT",
    "Account Executive eHealth",
    "Senior Sales Manager Krankenkasse",
    "Account Manager IT Consulting Gesundheit",
]

# Queries for GKV career pages (GKV Karriere scraper)
# → suche nach internen Führungsrollen direkt bei Krankenkassen
# Wird als Titelfilter verwendet (Job erscheint nur wenn mind. ein Begriff im Titel steckt)
GKV_QUERIES = [
    "Leiter",
    "Head of",
    "Manager",
    "Director",
    "CDO",
    "Chief",
    "Digitalisierung",
    "eHealth",
    "IT",
    "Strategie",
    "Vertrieb",
    "Einkauf",
    "Vergabe",
    "Innovation",
]

# Backwards-compat alias used by existing imports
SEARCH_QUERIES = EXTERNAL_QUERIES

# Queries für IT-Dienstleister-Karriereseiten (Titelfilter, kürzere Begriffe)
# → breiter als EXTERNAL_QUERIES, da Stellentitel bei Dienstleistern selten "GKV" enthalten
IT_DIENSTLEISTER_QUERIES = [
    "Account Manager",
    "Sales Manager",
    "Key Account",
    "Business Development",
    "Vertrieb",
    "Manager",
    "Director",
    "Head of",
    "Consulting",
    "Consultant",
    "Healthcare",
    "Gesundheit",
    "eHealth",
    "GKV",
    "Public Sector",
]

# Keywords that BOOST relevance score (keyword → points)
POSITIVE_KEYWORDS = {
    # Core domain – highest weight
    "GKV": 20,
    "gesetzliche Krankenversicherung": 20,
    "Krankenkasse": 18,
    "Krankenkassen": 18,
    "BKK": 15,
    "IKK": 15,
    "DAK": 15,
    "AOK": 15,
    "TK ": 10,
    "Public Sector": 15,
    "öffentlicher Sektor": 15,
    "Behörden": 10,
    "ÖGD": 12,
    # Sales roles
    "Account Manager": 15,
    "Sales Manager": 15,
    "Key Account": 15,
    "Business Development": 12,
    "Vertrieb": 10,
    "Neukundengewinnung": 10,
    "Großkunden": 10,
    "Enterprise Sales": 12,
    # IT & Health IT
    "eHealth": 15,
    "Gesundheitswesen": 12,
    "Healthcare IT": 15,
    "Health IT": 15,
    "IT-Consulting": 12,
    "IT Consulting": 12,
    "Digitalisierung": 8,
    "Digital Health": 14,
    # Tender / Bid
    "Ausschreibung": 15,
    "Vergabe": 12,
    "BID ": 10,
    "Tender": 10,
    "BAFO": 15,
    "Vergabeverfahren": 14,
    # Regulatory
    "KRITIS": 12,
    "NIS2": 12,
    "SGB V": 15,
    "TI 2.0": 12,
    "Telematikinfrastruktur": 12,
    # Technology
    "Cloud": 8,
    "BITMARCK": 15,
    "iskv": 12,
    "RPA": 8,
    "GenAI": 8,
    # Seniority / level
    "Senior": 8,
    "Lead ": 6,
    "Principal": 6,
    "Director": 5,
    "Head of": 5,
    # Location
    "Hamburg": 5,
    "Remote": 5,
    "Hybrid": 5,
    "Homeoffice": 5,
    # GKV internal leadership roles – Digitalisierung & Transformation
    "Chief Digital Officer": 22,
    "CDO": 15,
    "Leiter Digital": 22,        # covers Leiter Digitalisierung, Leiter Digitale *
    "Bereichsleiter Digital": 22,
    "Head of Digital": 22,
    "Leiter E-Health": 22,
    "Leiter eHealth": 22,
    "Leiter Unternehmensentwicklung": 20,
    "Leiter Innovation": 20,     # covers Leiter Innovation + Leiter Innovationsmanagement
    "Head of Customer Experience": 20,
    # GKV internal leadership roles – IT-Strategie & IT-Steuerung
    "Leiter IT-Strategie": 22,
    "Head of IT Strategy": 22,
    "Leiter IT-Steuerung": 22,
    "Head of IT Governance": 22,
    "IT-Portfolio": 18,
    "IT Portfolio": 18,
    "IT-Programmleiter": 20,
    "Demand Management IT": 18,
    "Head of Cloud Transformation": 20,
    "Leiter Anwendungsstrategie": 20,
    "Leiter Plattformstrategie": 20,
    # GKV internal leadership roles – Einkauf, Vergabe & Sourcing
    "Leiter Einkauf": 20,        # covers Leiter Einkauf IT + Leiter Strategischer Einkauf
    "Head of Procurement": 20,
    "Vergabemanagement": 18,
    "Tender Manager": 18,
    "Sourcing Manager": 18,
    "Vendor Manager": 18,
    "Dienstleistersteuerung": 18,
    "Leiter Partnermanagement": 18,
    # GKV internal leadership roles – Vertrieb & Markt
    "Leiter Vertrieb": 20,
    "Leiter Kundenmanagement": 20,
    "Leiter Firmenkunden": 20,
    "Leiter Bestandskunden": 18,
    "Leiter Partnervertrieb": 20,
    # GKV internal leadership roles – Produkt & Versorgung
    "Leiter Digitale Produkte": 22,
    "Head of Digital Products": 22,
    "Leiter App": 18,
    "Omnichannel": 15,
    "Leiter Versorgungsprogramme": 20,
    "Leiter Versorgungslösungen": 20,
    "eHealth-Produkte": 18,
    # GKV internal leadership roles – Vorstand & Stabsfunktionen
    "Leiter Strategie": 20,
    "Leiter Vorstandsstab": 20,
    "Chief of Staff": 18,
    "Referent Vorstand": 18,
    # Known companies in the ecosystem
    "adesso": 8,
    "CGI": 8,
    "IBM": 6,
    "Sopra Steria": 8,
    "Capgemini": 8,
    "msg": 8,
    "Deloitte": 6,
    "PwC": 6,
    "KPMG": 6,
    "Accenture": 6,
    "T-Systems": 8,
    "Atruvia": 8,
}

# Keywords that REDUCE score (keyword → negative points)
NEGATIVE_KEYWORDS = {
    "Zeitarbeit": -30,
    "Zeitarbeitnehmer": -30,
    "Leiharbeit": -30,
    "Werkstudent": -40,
    "Praktikum": -40,
    "Praktikant": -40,
    "Trainee": -20,
    "Junior": -25,
    "Berufseinsteiger": -30,
    "Quereinsteiger": -20,
    "Minijob": -40,
    "450 ": -30,
    "Pflegefachkraft": -40,
    "Pflegekraft": -40,
    "Pflegehelfer": -40,
    "Arzt": -40,
    "Ärztin": -40,
    "Medizin": -20,
    "Krankenpflege": -40,
    "Physiotherap": -40,
    "Zahnarzt": -40,
    "Produktion": -30,
    "Lager ": -30,
    "Logistik": -20,
    "Fahrer": -35,
    "LKW": -35,
    "Monteur": -35,
    "Hausmeister": -40,
    "Reinigung": -40,
    "Buchhaltung": -20,
    "Steuerberater": -30,
}

# Minimum score to include in the daily email
MIN_SCORE = 25

# Max jobs fetched per query per source (avoid hammering)
MAX_JOBS_PER_QUERY = 25
