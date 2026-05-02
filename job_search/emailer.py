"""
Build and send the daily HTML job-search digest via Gmail SMTP.
"""
import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List

logger = logging.getLogger(__name__)

SOURCE_COLORS = {
    "Arbeitsagentur":   "#005b99",
    "GKV Karriere":     "#0f766e",
    "IT Dienstleister": "#7c3aed",
    "LinkedIn":         "#0077b5",
    "Indeed":           "#2557a7",
    "StepStone":        "#e8620b",
}


def _score_meta(score: int):
    if score >= 70:
        return "Sehr hoch", "#16a34a"
    if score >= 50:
        return "Hoch",      "#65a30d"
    if score >= 35:
        return "Mittel",    "#d97706"
    return "Relevant",      "#2563eb"


_ACTION_STYLE = {
    "Sofort bewerben": "background:#16a34a;color:#fff;",
    "Prüfen":          "background:#d97706;color:#fff;",
    "Überspringen":    "background:#e5e7eb;color:#6b7280;",
}


def _count_rows(counts: Dict[str, int]) -> str:
    if not counts:
        return '<tr><td style="padding:4px 0;color:#6b7280;">Keine</td><td></td></tr>'
    rows = []
    for key, value in sorted(counts.items()):
        rows.append(
            f'<tr><td style="padding:4px 12px 4px 0;color:#374151;">{key}</td>'
            f'<td style="padding:4px 0;text-align:right;font-weight:700;color:#111827;">{value}</td></tr>'
        )
    return "\n".join(rows)


def _job_row(job: Dict) -> str:
    score                = job.get("score", 0)
    label, score_color   = _score_meta(score)
    source               = job.get("source", "")
    source_color         = SOURCE_COLORS.get(source, "#6b7280")
    title                = job.get("title", "")
    company              = job.get("company") or "—"
    location             = job.get("location", "")
    url                  = job.get("url", "#")
    ai_reason            = job.get("ai_reason", "")
    ai_strengths         = job.get("ai_strengths", [])
    ai_concerns          = job.get("ai_concerns", [])
    ai_action            = job.get("ai_action", "")
    posted               = (job.get("posted_date") or "")[:10]

    # AI-Bewertungsblock: Zusammenfassung + Stärken + Bedenken + Handlungsempfehlung
    ai_parts = []
    if ai_reason:
        ai_parts.append(
            f'<div style="margin-top:6px;font-size:12px;color:#6366f1;font-style:italic;">'
            f'&#129302; {ai_reason}</div>'
        )
    if ai_strengths:
        badges = "&nbsp;".join(
            f'<span style="background:#dcfce7;color:#166534;border-radius:3px;'
            f'padding:2px 6px;font-size:10px;font-weight:600;">&#10003; {s}</span>'
            for s in ai_strengths
        )
        ai_parts.append(f'<div style="margin-top:4px;line-height:1.8;">{badges}</div>')
    if ai_concerns:
        badges = "&nbsp;".join(
            f'<span style="background:#fff7ed;color:#9a3412;border-radius:3px;'
            f'padding:2px 6px;font-size:10px;font-weight:600;">&#9888; {c}</span>'
            for c in ai_concerns
        )
        ai_parts.append(f'<div style="margin-top:3px;line-height:1.8;">{badges}</div>')
    if ai_action:
        style = _ACTION_STYLE.get(ai_action, "background:#e5e7eb;color:#6b7280;")
        ai_parts.append(
            f'<div style="margin-top:5px;">'
            f'<span style="{style}border-radius:4px;padding:2px 8px;'
            f'font-size:10px;font-weight:700;">{ai_action}</span></div>'
        )
    ai_html = "\n".join(ai_parts)

    posted_html = (
        f'<span style="color:#9ca3af;font-size:11px;"> &middot; {posted}</span>'
        if posted else ""
    )

    return f"""
<tr style="border-bottom:1px solid #e5e7eb;">
  <td style="padding:12px 10px 12px 0;vertical-align:top;white-space:nowrap;width:1%;">
    <div style="background:{score_color};color:#fff;border-radius:6px;
                padding:5px 9px;text-align:center;min-width:52px;">
      <div style="font-size:15px;font-weight:800;line-height:1.1;">{score}</div>
      <div style="font-size:10px;font-weight:600;opacity:.9;">{label}</div>
    </div>
  </td>
  <td style="padding:12px 14px;vertical-align:top;">
    <div style="font-size:15px;font-weight:700;color:#111827;line-height:1.3;">
      {title}
    </div>
    <div style="font-size:13px;color:#374151;margin-top:2px;">
      {company}
      {posted_html}
    </div>
    <div style="font-size:12px;color:#6b7280;margin-top:2px;">
      &#128205; {location}
      &nbsp;
      <span style="background:{source_color};color:#fff;border-radius:3px;
                   padding:1px 6px;font-size:10px;font-weight:700;">
        {source}
      </span>
    </div>
    {ai_html}
  </td>
  <td style="padding:12px 0 12px 10px;vertical-align:middle;white-space:nowrap;width:1%;">
    <a href="{url}"
       style="display:inline-block;background:#2563eb;color:#fff;
              padding:8px 14px;border-radius:7px;text-decoration:none;
              font-size:13px;font-weight:600;">
      Bewerben &#8594;
    </a>
  </td>
</tr>"""


def build_html(jobs: List[Dict], name: str) -> str:
    today      = datetime.now().strftime("%d.%m.%Y")
    first_name = name.split()[0]
    count      = len(jobs)

    source_counts: Dict[str, int] = {}
    for j in jobs:
        s = j.get("source", "?")
        source_counts[s] = source_counts.get(s, 0) + 1
    sources_str = " &middot; ".join(
        f"{s}: {n}" for s, n in sorted(source_counts.items())
    )

    rows_html = "\n".join(_job_row(j) for j in jobs)

    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Stellenübersicht {today}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f6;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">

  <div style="max-width:680px;margin:0 auto;padding:24px 16px;">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1e3a8a 0%,#2563eb 100%);
                border-radius:14px;padding:24px 28px;margin-bottom:20px;">
      <h1 style="margin:0 0 4px;font-size:20px;color:#fff;font-weight:800;">
        &#128269; Deine Stellen-Übersicht &mdash; {today}
      </h1>
      <p style="margin:0;font-size:13px;color:#bfdbfe;">
        <strong style="color:#fff;">{count} relevante Stelle{"n" if count != 1 else ""}</strong>
        &nbsp;&middot;&nbsp; {sources_str}
      </p>
    </div>

    <!-- Legende -->
    <div style="background:#fff;border-radius:10px;padding:10px 16px;
                margin-bottom:16px;font-size:11px;color:#6b7280;
                border:1px solid #e5e7eb;">
      Score:&nbsp;
      <strong style="color:#16a34a;">70–100 Sehr hoch</strong> &nbsp;|&nbsp;
      <strong style="color:#65a30d;">50–69 Hoch</strong> &nbsp;|&nbsp;
      <strong style="color:#d97706;">35–49 Mittel</strong> &nbsp;|&nbsp;
      <strong style="color:#2563eb;">25–34 Relevant</strong>
    </div>

    <!-- Tabelle -->
    <div style="background:#fff;border-radius:12px;padding:8px 20px 4px;
                box-shadow:0 1px 4px rgba(0,0,0,0.07);border:1px solid #e5e7eb;">
      <table style="width:100%;border-collapse:collapse;">
        {rows_html}
      </table>
    </div>

    <!-- Footer -->
    <p style="text-align:center;margin-top:20px;font-size:11px;color:#9ca3af;">
      Job-Search-Bot &middot; Quellen: Arbeitsagentur &middot; LinkedIn &middot; GKV-Karriereseiten<br>
      <a href="https://github.com/cgallerhh/Jobsucher/actions"
         style="color:#93c5fd;text-decoration:none;">Workflow-Status</a>
    </p>

  </div>
</body>
</html>"""


def build_empty_html(name: str, diagnostics: Dict | None = None) -> str:
    today = datetime.now().strftime("%d.%m.%Y")
    first_name = name.split()[0]
    diagnostics = diagnostics or {}

    raw_total = diagnostics.get("raw_total", 0)
    new_total = diagnostics.get("new_total", 0)
    keyword_candidates = diagnostics.get("keyword_candidates", 0)
    final_relevant = diagnostics.get("final_relevant", 0)
    raw_by_source = diagnostics.get("raw_by_source", {})
    new_by_source = diagnostics.get("new_by_source", {})
    rejected_by_reason = diagnostics.get("rejected_by_reason", {})

    source_rows = _count_rows(raw_by_source)
    new_rows = _count_rows(new_by_source)
    rejected_rows = _count_rows(rejected_by_reason)

    return f"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Keine neuen Stellen {today}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f6;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="max-width:680px;margin:0 auto;padding:24px 16px;">
    <div style="background:linear-gradient(135deg,#1e3a8a 0%,#2563eb 100%);
                border-radius:14px;padding:24px 28px;margin-bottom:20px;">
      <h1 style="margin:0 0 4px;font-size:20px;color:#fff;font-weight:800;">
        &#128269; Nullmeldung &mdash; {today}
      </h1>
      <p style="margin:0;font-size:13px;color:#bfdbfe;">
        Heute wurden keine wirklich passenden Stellen gefunden.
      </p>
    </div>

    <div style="background:#fff;border-radius:12px;padding:24px 28px;
                box-shadow:0 1px 4px rgba(0,0,0,0.07);border:1px solid #e5e7eb;">
      <p style="margin:0 0 12px;font-size:15px;color:#111827;">Hi {first_name},</p>
      <p style="margin:0 0 12px;font-size:14px;color:#374151;line-height:1.6;">
        der tägliche GitHub-Lauf war erfolgreich. Ich zeige dir weiterhin nur Rollen,
        die sowohl zur Senior-Sales-/Strategie-Ebene als auch zur GKV-, Public-Sector-
        oder Healthcare-IT-Domäne passen.
      </p>
      <div style="margin-top:18px;border-top:1px solid #e5e7eb;padding-top:16px;">
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <tr>
            <td style="padding:4px 12px 4px 0;color:#374151;">Gefundene Jobs insgesamt</td>
            <td style="padding:4px 0;text-align:right;font-weight:700;color:#111827;">{raw_total}</td>
          </tr>
          <tr>
            <td style="padding:4px 12px 4px 0;color:#374151;">Davon neu nach Dublettenprüfung</td>
            <td style="padding:4px 0;text-align:right;font-weight:700;color:#111827;">{new_total}</td>
          </tr>
          <tr>
            <td style="padding:4px 12px 4px 0;color:#374151;">Nach strengem Relevanzfilter</td>
            <td style="padding:4px 0;text-align:right;font-weight:700;color:#111827;">{keyword_candidates}</td>
          </tr>
          <tr>
            <td style="padding:4px 12px 4px 0;color:#374151;">Final relevante Jobs</td>
            <td style="padding:4px 0;text-align:right;font-weight:700;color:#111827;">{final_relevant}</td>
          </tr>
        </table>
      </div>
      <div style="margin-top:16px;">
        <div style="font-size:12px;font-weight:800;color:#111827;margin-bottom:6px;">Quellenlauf</div>
        <table style="width:100%;border-collapse:collapse;font-size:12px;">{source_rows}</table>
      </div>
      <div style="margin-top:16px;">
        <div style="font-size:12px;font-weight:800;color:#111827;margin-bottom:6px;">Neu gefunden</div>
        <table style="width:100%;border-collapse:collapse;font-size:12px;">{new_rows}</table>
      </div>
      <div style="margin-top:16px;">
        <div style="font-size:12px;font-weight:800;color:#111827;margin-bottom:6px;">Warum nichts angezeigt wurde</div>
        <table style="width:100%;border-collapse:collapse;font-size:12px;">{rejected_rows}</table>
      </div>
    </div>

    <p style="text-align:center;margin-top:20px;font-size:11px;color:#9ca3af;">
      Job-Search-Bot &middot; tägliche Nullmeldung aktiviert<br>
      <a href="https://github.com/cgallerhh/Jobsucher/actions"
         style="color:#93c5fd;text-decoration:none;">Workflow-Status</a>
    </p>
  </div>
</body>
</html>"""


def send_email(to: str, subject: str, html: str) -> None:
    """Send HTML email via Gmail SMTP SSL (port 465)."""
    user     = os.environ.get("GMAIL_USER", "")
    password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not user or not password:
        raise RuntimeError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set. "
            "Use a Google App Password (not your regular password)."
        )

    msg           = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Job Search Bot <{user}>"
    msg["To"]      = to
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(user, password)
        server.sendmail(user, [to], msg.as_string())
    logger.info("Email sent to %s", to)
