# Job Search Automation

Täglich um 07:00 Uhr deutscher Ortszeit durchsucht dieser Bot per echtem
macOS-Cron automatisch
**Arbeitsagentur**, **Indeed**, **StepStone**, **LinkedIn** sowie direkte
Karriereseiten priorisierter Zielunternehmen nach passenden Stellen für
**Christian Galler** und liefert eine sortierte E-Mail-Übersicht.

Das Suchprofil entspricht dem Bewerbungs-Master vom 03.08.2026 und arbeitet mit
drei Spuren:

1. GKV, Krankenkassen, Sozialversicherung und Payor/Payer
2. Healthcare, Health IT, Digital Health und Public Sector
3. senioriger Enterprise-Tech-Transfer in Cloud, Managed Services,
   Security/Compliance, Data/AI, Plattformen und SaaS

Berücksichtigt werden Hamburg/Umkreis und echte Remote-Rollen in Deutschland.
Geschlossene Firmen sowie bereits beworbene Arbeitgeber werden hart gesperrt.

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
| `GMAIL_USER` | Bestehende Gmail-Absenderadresse (rückwärtskompatibel) |
| `GMAIL_APP_PASSWORD` | Das 16-stellige App-Passwort aus Schritt 1 |
| `RECIPIENT_EMAIL` | `christian.galler+jobsucher@gmail.com` – dasselbe Gmail-Konto mit eigener Zustelladresse |

Wenn Absender und Empfänger dasselbe Gmail-Konto sind, trägt eine Nachricht
gleichzeitig die Labels `SENT` und `INBOX`. Apple Mail kann sie deshalb mit
einer Zählblase „2“ anzeigen, obwohl Gmail nur eine Nachricht gespeichert hat.
Das ist kein zweiter Botlauf. Für eine vollständig getrennte Zustellung wird
ein separates Absenderkonto empfohlen:

| Optionales Secret | Wert |
|---|---|
| `SMTP_HOST` | z. B. `smtp.gmail.com` |
| `SMTP_PORT` | `465` für SMTP über SSL |
| `SMTP_USER` | Login des separaten Absenderkontos |
| `SMTP_PASSWORD` | App-Passwort des separaten Absenderkontos |
| `SMTP_FROM_EMAIL` | sichtbare, beim Provider erlaubte Absenderadresse |
| `SMTP_FROM_NAME` | z. B. `Job Search Bot` |

Sobald `SMTP_USER` und `SMTP_PASSWORD` gesetzt sind, verwendet der Bot diese
anstelle der bisherigen `GMAIL_*`-Werte. `RECIPIENT_EMAIL` kann dann auf die
normale persönliche Adresse zeigen, ohne eine gesendete Eigenkopie zu erzeugen.

### 3. Echten Cron installieren

```bash
./scripts/install_cron.sh
```

Der Installer erhält andere vorhandene Crontab-Einträge und verwaltet nur den
mit `JobSucher real cron` markierten Eintrag. Für einen Rechner, der nachts
schläft, sollte macOS zusätzlich fünf Minuten vorher geweckt werden:

```bash
sudo pmset repeat wakeorpoweron MTWRFSU 06:55:00
```

GitHub Actions bleibt unter **Actions → Daily Job Search** als manueller
Notfall- und Testlauf verfügbar, besitzt aber keinen eigenen Zeitplan mehr.

---

## Zeitplan

Der Benutzer-Cron läuft täglich auf:

```cron
0 7 * * * /bin/zsh /absoluter/pfad/zu/scripts/run_daily_cron.sh
```

Da der Mac auf `Europe/Berlin` eingestellt ist, bleibt der Start bei Sommer-
und Winterzeit um **07:00 Uhr deutscher Ortszeit**. Der Lauf verwendet einen
Lock gegen Doppelstarts. Protokolle und der vom Git-Checkout getrennte
Seen-State liegen unter:

```text
~/Library/Application Support/JobSucher/logs/cron.log
~/Library/Application Support/JobSucher/seen_jobs.json
```

Damit hängen Startzeit und Deduplication nicht mehr von der verzögerten
GitHub-Scheduler-Warteschlange oder von automatischen Commits auf `main` ab.

---

## Suchprofil anpassen

Alle Suchparameter befinden sich in `job_search/config.py`:

- **`EXTERNAL_QUERIES`** – Suchbegriffe für die drei Suchspuren
- **`APPLIED_COMPANIES`** – bereits beworbene Firmen, die nicht erneut erscheinen
- **`EXCLUDED_COMPANIES`** – verbindliches Firmen-Ausschlussregister
- **`POSITIVE_KEYWORDS`** – Schlüsselwörter die die Relevanz erhöhen (+ Punkte)
- **`NEGATIVE_KEYWORDS`** – Schlüsselwörter die ausschließen (− Punkte)
- **`MIN_SCORE`** – Mindestscore für die E-Mail (Standard: 25)
- **`PROFILE["location"]`** – Suchort (Standard: Hamburg)

`PROFILE_VERSION` versioniert den Deduplication-State. Bei einer inhaltlichen
Profiländerung werden aktuelle Stellen einmal neu bewertet, ohne die Historie
manuell löschen zu müssen.

Alle mit der aktuellen Profilversion geprüften Stellen werden im Seen-State
gespeichert, auch wenn sie durch ein Gate oder den Score aussortiert wurden.
Dadurch werden unveränderte Treffer nicht an jedem Folgetag erneut bewertet.
Der Seen-State wird erst gespeichert, nachdem Gmail die Nachricht angenommen
hat; bei einem Versandfehler beendet sich der Cronlauf mit einem Fehler.

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
├── .github/workflows/job-search.yml   ← manueller GitHub-Notfalllauf
├── job_search/
│   ├── config.py                      ← Suchprofil & Keywords
│   ├── filter.py                      ← Relevanz-Scoring
│   ├── emailer.py                     ← HTML-E-Mail & Gmail-Versand
│   ├── main.py                        ← Orchestrierung
│   └── scrapers/
│       ├── arbeitsagentur.py          ← Bundesagentur-für-Arbeit-API
│       ├── gkv_careers.py             ← 31 GKV-Karriereseiten
│       ├── indeed.py                  ← Indeed RSS
│       ├── it_dienstleister.py        ← direkte Seiten der Zielunternehmen
│       ├── stepstone.py               ← StepStone HTML-Scraping
│       └── linkedin.py                ← LinkedIn Guest HTML, Zielunternehmen only
├── data/seen_jobs.json                ← Deduplication-State (auto-gepflegt)
├── context/                           ← Profil, Kriterien, Firmenstatus für KI-Scoring
├── tests/                             ← Tests der Such- und Ausschlusslogik
├── requirements.txt
├── scripts/
│   ├── install_cron.sh                ← installiert den echten 07:00-Cron
│   └── run_daily_cron.sh              ← isolierter Lauf, Lock und Logging
└── .env.example
```
