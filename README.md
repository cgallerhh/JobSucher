# Job Search Automation

Täglich um 9:00 Uhr (Wochentage) durchsucht dieser Bot automatisch
**Indeed**, **StepStone** und **LinkedIn** nach passenden Stellen für
**Christian Galler** und liefert eine sortierte E-Mail-Übersicht.

---

## Einmalige Einrichtung (5 Minuten)

### 1. Gmail App-Passwort erstellen

1. Öffne [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
   (benötigt aktivierte 2-Schritt-Verifizierung)
2. App: **E-Mail** | Gerät: **Anderes Gerät** → Name z. B. „Job Bot"
3. Generiertes 16-stelliges Passwort kopieren

### 2. GitHub Repository Secrets setzen

Gehe zu: **Repository → Settings → Secrets and variables → Actions → New repository secret**

| Secret-Name | Wert |
|---|---|
| `GMAIL_USER` | Deine Gmail-Adresse (z. B. `christian.galler@gmail.de`) |
| `GMAIL_APP_PASSWORD` | Das 16-stellige App-Passwort aus Schritt 1 |
| `RECIPIENT_EMAIL` | Wohin die E-Mail gesendet wird (kann dieselbe sein) |

### 3. Workflow aktivieren

Nach dem Push ist der Workflow unter **Actions → Daily Job Search** sichtbar.
Klicke auf **Run workflow** für einen ersten Testlauf.

---

## Zeitplan

Der Cron läuft auf `0 7 * * 1-5` (UTC):

| Jahreszeit | UTC | Deutsche Zeit |
|---|---|---|
| Sommer (CEST, UTC+2) | 07:00 | **09:00** |
| Winter (CET, UTC+1) | 07:00 | 08:00 |

Für exakt 9:00 Uhr im Winter: In `.github/workflows/job-search.yml`
den Cron auf `0 8 * * 1-5` ändern (dann ist es im Sommer 10:00 Uhr).

---

## Suchprofil anpassen

Alle Suchparameter befinden sich in `job_search/config.py`:

- **`SEARCH_QUERIES`** – Suchbegriffe für die Jobbörsen
- **`POSITIVE_KEYWORDS`** – Schlüsselwörter die die Relevanz erhöhen (+ Punkte)
- **`NEGATIVE_KEYWORDS`** – Schlüsselwörter die ausschließen (− Punkte)
- **`MIN_SCORE`** – Mindestscore für die E-Mail (Standard: 25)
- **`PROFILE["location"]`** – Suchort (Standard: Hamburg)

---

## Lokaler Testlauf

```bash
# Abhängigkeiten installieren
pip install -r requirements.txt

# .env befüllen (einmalig)
cp .env.example .env
# → .env editieren mit deinen Werten

# Ausführen
export $(cat .env | xargs)
python -m job_search.main
```

---

## Dateistruktur

```
├── .github/workflows/job-search.yml   ← GitHub Actions Cron
├── job_search/
│   ├── config.py                      ← Suchprofil & Keywords
│   ├── filter.py                      ← Relevanz-Scoring
│   ├── emailer.py                     ← HTML-E-Mail & Gmail-Versand
│   ├── main.py                        ← Orchestrierung
│   └── scrapers/
│       ├── indeed.py                  ← Indeed RSS (zuverlässigste Quelle)
│       ├── stepstone.py               ← StepStone HTML-Scraping
│       └── linkedin.py                ← LinkedIn Guest API
├── data/seen_jobs.json                ← Deduplication-State (auto-gepflegt)
├── requirements.txt
└── .env.example
```
