"""Verbindliches Suchprofil fuer Christian Gallers Bewerbungspipeline.

Quelle: LLM-Wiki/BEWERBUNGEN, Masterdokument
"Chancen, Rollen und Bewerbungsstrategien", Stand 03.08.2026.
"""

PROFILE_VERSION = "2026-08-03-master-v1"

# Diese drei Originalanzeigen hat der Nutzer am 05.08.2026 ausdrücklich als
# prüfenswert bestätigt. Sie werden genau einmal gemeldet, auch wenn die KI
# wegen verkürzter Quelltexte unter 70 Punkten bleibt; der Seen-State verhindert
# danach jede Wiederholung.
MANUAL_REVIEW_JOB_IDS = {
    "8016144",                         # FREENOW – BDM Health
    "19913-3014101785362401-S",       # NTT DATA – Senior Sales Manager SAP
    "19913-3042021785362400-S",       # NTT DATA – AI GTM Insurance DACH Lead
}

PROFILE = {
    "name": "Christian Galler",
    "email": "christian.galler@gmail.com",
    "headline": "Strategic Enterprise Sales | GKV, Healthcare, Public Sector",
    "location": "Hamburg",
    "radius_km": 50,
    "remote_ok": True,
    "salary_target": 100000,
    "salary_min": 90000,
    "availability": "kurzfristig verfuegbar",
}

# Die drei Suchspuren muessen gemeinsam abgedeckt werden. Spur 3 ist bewusst
# eine Transferstrecke und verlangt im Filter einen klaren Enterprise-/Senior-
# Titel sowie Cloud-, Security-, Data-/AI-, Plattform- oder SaaS-Kontext.
SEARCH_TRACKS = {
    "gkv_payor": "GKV, Krankenkassen, Sozialversicherung, Payer/Payor, Vergabe",
    "health_public": "Healthcare, Health IT, Digital Health, Public Healthcare, Public Sector",
    "enterprise_tech": "Enterprise Tech Sales: Cloud, Managed Services, Security, Data/AI, Plattformen, SaaS",
}

# Lokal plus bundesweit fuer echte Remote-Rollen. Der Standortfilter verhindert,
# dass ein blosses Suchergebnis aus Berlin/Muenchen ohne Remote-Modell durchkommt.
SEARCH_LOCATIONS = ["Hamburg", "Deutschland"]

# Praezise Suchanfragen fuer Arbeitsagentur, Indeed, StepStone und LinkedIn.
EXTERNAL_QUERIES = [
    # Spur 1: GKV / Sozialversicherung / Payor
    "Strategic Account Manager GKV",
    "Senior Account Manager Krankenkassen",
    "Key Account Manager Krankenkassen Vergabe",
    "Enterprise Account Executive Sozialversicherung",
    "Business Development Manager Healthcare Payer",
    # Spur 2: Healthcare / Public Sector
    "Senior Account Executive Healthcare",
    "Enterprise Account Executive Public Sector",
    "Senior Sales Manager Health IT",
    "Business Development Manager Digital Health",
    "Client Partner Healthcare",
    "Partner Manager Healthcare",
    # Spur 3: gehaltsstarker Enterprise-Tech-Transfer
    "Enterprise Account Executive Cloud Germany",
    "Enterprise Account Executive Data AI Germany",
    "Senior Account Executive Cybersecurity Germany",
    "Strategic Account Manager Managed Services",
    "Named Account Executive Insurance Germany",
    "Enterprise Account Executive SaaS Germany",
    # Bewusste Alternativrollen im Payor-/Vertragsumfeld
    "Market Access Manager Krankenkassen",
    "Vertragsmanager Krankenkassen Gesundheitswesen",
]

# Die BA-v6-Suche behandelt lange Phrasen deutlich strenger als die anderen
# Jobboersen. Dort wird deshalb ueber Senior-/Zieltitel breit eingesammelt und
# erst anschliessend mit demselben harten Masterprofil gefiltert.
BA_QUERIES = [
    "Strategic Account Manager",
    "Senior Account Manager",
    "Key Account Manager",
    "Senior Sales Manager",
    "Enterprise Account Executive",
    "Senior Account Executive",
    "Named Account Executive",
    "Business Development Manager",
    "Client Partner",
    "Partner Manager",
    "Market Access Manager",
    "Vertragsmanager Gesundheitswesen",
]

# Strategische interne Rollen bei Krankenkassen bleiben eine schmale
# Alternativspur. Operative Kassenrollen werden im Relevanz-Gate ausgeschlossen.
GKV_QUERIES = [
    "Leiter Digitalisierung",
    "Leiter IT-Strategie",
    "Leiter Unternehmensentwicklung",
    "Leiter Vergabemanagement",
    "Head of Digital",
    "Head of IT Strategy",
    "Head of Procurement",
    "Chief Digital Officer",
    "Partner Manager",
    "Vertragsmanager",
]

# Titelfilter fuer die direkt beobachteten Zielunternehmen.
DIRECT_COMPANY_QUERIES = [
    "Strategic Account Manager",
    "Senior Account Manager",
    "Key Account Manager",
    "Senior Sales Manager",
    "Sales Director",
    "Senior Account Executive",
    "Enterprise Account Executive",
    "Named Account Executive",
    "Business Development Manager",
    "Client Partner",
    "Partner Manager",
    "Commercial Lead",
    "Go-to-Market",
    "Market Access",
    "Vertragsmanager",
    "Vertragsverhandler",
]

# Rueckwaertskompatible Aliase fuer bestehende Imports.
SEARCH_QUERIES = EXTERNAL_QUERIES
IT_DIENSTLEISTER_QUERIES = DIRECT_COMPANY_QUERIES

# Diese Firmen erhalten im Scoring einen kleinen Prioritaetsbonus. Der fachliche
# und regionale Fit bleibt trotzdem zwingend.
PRIORITY_COMPANIES = [
    "DeepL",
    "SAP",
    "ZOTZ|KLIMAS",
    "FREENOW",
    "Genesys",
    "Veeam",
    "Salesforce",
    "Thieme",
    "SoftwareOne",
    "Amazon Web Services",
    "Camunda",
    "Public Cloud Group",
    "AppZen",
    "Gartner",
    "SnapLogic",
    "NTT DATA",
]

# Bereits beworben: nicht erneut als neue Chance melden.
APPLIED_COMPANIES = ["Acture"]

# Verbindliches Ausschlussregister aus dem Master und Target-Companies.
# Der Abgleich erfolgt ausschliesslich gegen das Arbeitgeberfeld, damit z. B.
# eine Referenz auf BITMARCK im Text einer ansonsten passenden Stelle erlaubt ist.
EXCLUDED_COMPANIES = [
    "adesso",
    "HBSN Consulting",
    "INIT",
    "Exxeta",
    "HMM",
    "HMM Deutschland",
    "hcVISION",
    "puntus",
    "BITMARCK",
    "Arvato Systems",
    "d.velop",
    "FERCHAU",
    "Faktor D",
    "x-tention",
    "IQVIA",
    "DAVASO",
    "COMLINE",
    "act digital",
    "Cloudflight",
    "KWSoft",
    "NICE",
    "Cognigy",
    "smart2success",
    "aquinet",
    "IPSWAYS",
    "GKV Informatik",
    "Convista",
    "AGORUM",
    "AOK",
]

# Diese Firmen sind nur dann ausgeschlossen, wenn die konkrete Rolle die
# dokumentierte Kompetenzluecke explizit verlangt.
CONDITIONAL_COMPANY_EXCLUSIONS = {
    "atacama": ["oscare"],
}

# Keywords, die den deterministischen Score erhoehen.
POSITIVE_KEYWORDS = {
    # Spur 1: GKV / Payor / Sozialversicherung
    "GKV": 20,
    "gesetzliche Krankenversicherung": 20,
    "Krankenkasse": 18,
    "Krankenkassen": 18,
    "Sozialversicherung": 18,
    "Payer": 14,
    "Payor": 14,
    "Kostenträger": 14,
    "BKK": 14,
    "IKK": 14,
    "SGB V": 14,
    "BITMARCK": 10,
    "iskv": 10,
    # Spur 2: Healthcare / Public Sector
    "Healthcare": 14,
    "Gesundheitswesen": 14,
    "Health IT": 15,
    "Healthcare IT": 15,
    "Digital Health": 14,
    "eHealth": 14,
    "Public Healthcare": 16,
    "Public Sector": 16,
    "Insurance": 8,
    "öffentlicher Sektor": 16,
    "Government": 10,
    "regulierte Branchen": 10,
    "regulated industries": 10,
    # Zielrollen / Senioritaet
    "Strategic Account Manager": 18,
    "Senior Account Manager": 18,
    "Key Account Manager": 16,
    "Senior Sales Manager": 18,
    "Sales Director": 16,
    "Enterprise Account Executive": 20,
    "Senior Account Executive": 18,
    "Named Account Executive": 18,
    "Business Development Manager": 14,
    "Client Partner": 15,
    "Partner Manager": 12,
    "Commercial Lead": 12,
    "Go-to-Market": 10,
    "Enterprise Sales": 14,
    "Großkunden": 10,
    "Neukundengewinnung": 8,
    # Alternativrollen mit Markt-/Vertragshebel
    "Market Access": 15,
    "Payer Partnerships": 15,
    "Strategic Partnerships": 12,
    "Vertragsmanager": 14,
    "Vertragsverhandler": 14,
    "Vertragsreferent": 12,
    # Strategische interne GKV-Alternativen
    "Leiter Digitalisierung": 22,
    "Leiter IT-Strategie": 22,
    "Leiter Unternehmensentwicklung": 20,
    "Leiter Vergabemanagement": 20,
    "Head of Digital": 20,
    "Head of IT Strategy": 20,
    "Head of Procurement": 20,
    "Chief Digital Officer": 22,
    # Ausschreibung / Large Deal / Buying Center
    "Ausschreibung": 15,
    "Vergabe": 14,
    "Tender": 12,
    "BAFO": 14,
    "Vergabeverfahren": 14,
    "C-Level": 10,
    "Buying Committee": 8,
    "Large Deal": 10,
    # Spur 3: Enterprise Tech
    "Enterprise": 10,
    "Cloud": 10,
    "Managed Services": 12,
    "Cybersecurity": 12,
    "Security": 8,
    "Compliance": 10,
    "KRITIS": 12,
    "NIS2": 10,
    "Data & AI": 12,
    "Data and AI": 12,
    "Künstliche Intelligenz": 10,
    "GenAI": 10,
    "SaaS": 10,
    "Plattform": 8,
    "Platform": 8,
    "Software": 6,
    # Arbeitsmodell
    "Hamburg": 5,
    "Remote": 5,
    "Homeoffice": 5,
    "Home Office": 5,
    "Hybrid": 3,
    # Aktuell priorisierte Arbeitgeber
    "DeepL": 8,
    "SAP": 8,
    "ZOTZ": 8,
    "FREENOW": 8,
    "Genesys": 8,
    "Veeam": 8,
    "Salesforce": 8,
    "Thieme": 8,
    "SoftwareOne": 6,
    "Camunda": 6,
    "Public Cloud Group": 6,
    "AppZen": 5,
    "NTT DATA": 6,
}

# Rollen- und Branchenmerkmale, die den Score reduzieren. Firmenausschluesse
# stehen bewusst separat und werden als hartes Gate behandelt.
NEGATIVE_KEYWORDS = {
    "Zeitarbeit": -40,
    "Leiharbeit": -40,
    "Arbeitnehmerüberlassung": -40,
    "Sachbearbeiter": -100,
    "Sachbearbeitung": -100,
    "Kundenberater": -80,
    "Kundenservice": -80,
    "Sozialversicherungsfachangestell": -100,
    "Werkstudent": -100,
    "Praktikum": -100,
    "Praktikant": -100,
    "Trainee": -100,
    "Junior": -100,
    "Berufseinsteiger": -100,
    "Quereinsteiger": -40,
    "Minijob": -100,
    "Pflegefachkraft": -100,
    "Pflegekraft": -100,
    "Arzt": -80,
    "Ärztin": -80,
    "Krankenpflege": -100,
    "Physiotherap": -100,
    "Produktion": -60,
    "Lager ": -60,
    "Logistik": -40,
    "Automotive": -100,
    "Automobil": -100,
    "Autohaus": -100,
    "Fahrer": -80,
    "Monteur": -80,
    "Hausmeister": -100,
    "Reinigung": -100,
}

MIN_SCORE = 25
MIN_EMAIL_SCORE = 70
MAX_JOBS_PER_QUERY = 25
MAX_JOB_AGE_DAYS = 14
