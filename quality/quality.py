"""
Qualité des données — Contrôles sur complétude, cohérence, validité
Tests :
  - Article sans titre
  - Date manquante
  - Contenu trop court
  - URL invalide
  - Doublon
  - Source inconnue
"""

import re
import json
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from collections import defaultdict

logger = logging.getLogger("DataQuality")

KNOWN_SOURCES = {
    "Hespress", "Akhbarona", "Lakom", "Barlamane",
    "Al Jazeera", "BBC News", "Reuters", "CNN",
}

MIN_CONTENT_WORDS = 30
MIN_CONTENT_CHARS = 100
URL_PATTERN = re.compile(r"^https?://[^\s/$.?#].[^\s]*$")


# ─────────────────────────────────────────────
# Checks individuels
# ─────────────────────────────────────────────

def check_has_title(article: Dict) -> Tuple[bool, str]:
    title = article.get("title", "")
    if not title or len(title.strip()) < 3:
        return False, "MISSING_TITLE"
    return True, ""

def check_has_content(article: Dict) -> Tuple[bool, str]:
    content = article.get("content", "")
    if not content or len(content) < MIN_CONTENT_CHARS:
        return False, f"CONTENT_TOO_SHORT (< {MIN_CONTENT_CHARS} chars)"
    if len(content.split()) < MIN_CONTENT_WORDS:
        return False, f"CONTENT_TOO_SHORT (< {MIN_CONTENT_WORDS} words)"
    return True, ""

def check_has_date(article: Dict) -> Tuple[bool, str]:
    pub = article.get("published_at") or article.get("scraped_at")
    if not pub:
        return False, "MISSING_DATE"
    return True, ""

def check_valid_url(article: Dict) -> Tuple[bool, str]:
    url = article.get("url", "")
    if not url:
        return False, "MISSING_URL"
    if not URL_PATTERN.match(url):
        return False, f"INVALID_URL: {url[:80]}"
    return True, ""

def check_has_source(article: Dict) -> Tuple[bool, str]:
    source = article.get("source", "")
    if not source:
        return False, "MISSING_SOURCE"
    return True, ""

def check_known_source(article: Dict) -> Tuple[bool, str]:
    source = article.get("source", "")
    if source not in KNOWN_SOURCES:
        return False, f"UNKNOWN_SOURCE: {source}"
    return True, ""

def check_has_language(article: Dict) -> Tuple[bool, str]:
    lang = article.get("language", "")
    if not lang or lang == "unknown":
        return False, "MISSING_LANGUAGE"
    return True, ""

def check_date_not_future(article: Dict) -> Tuple[bool, str]:
    pub = article.get("published_at")
    if not pub:
        return True, ""
    try:
        dt = datetime.fromisoformat(pub[:19])
        if dt > datetime.utcnow():
            return False, f"FUTURE_DATE: {pub}"
    except Exception:
        return False, f"INVALID_DATE_FORMAT: {pub}"
    return True, ""


# ─────────────────────────────────────────────
# Quality dimensions
# ─────────────────────────────────────────────

COMPLETENESS_CHECKS = [
    ("title",     check_has_title),
    ("content",   check_has_content),
    ("date",      check_has_date),
    ("url",       check_valid_url),
    ("source",    check_has_source),
    ("language",  check_has_language),
]

VALIDITY_CHECKS = [
    ("date_not_future", check_date_not_future),
    ("known_source",    check_known_source),
]


# ─────────────────────────────────────────────
# DataQuality class
# ─────────────────────────────────────────────

class DataQuality:
    """
    Exécute tous les contrôles qualité sur un batch d'articles.
    Retourne un rapport détaillé avec score et liste des anomalies.
    """

    def run_checks(self, articles: List[Dict]) -> Dict:
        if not articles:
            return {"summary": {"total": 0, "score": 0}, "details": [], "anomalies": []}

        total = len(articles)
        anomalies = []
        passed_all = 0

        completeness_failures = defaultdict(int)
        validity_failures = defaultdict(int)

        # Déduplication
        seen_ids = set()
        duplicates = 0

        for art in articles:
            art_id = art.get("article_id", art.get("url", ""))
            if art_id in seen_ids:
                duplicates += 1
                anomalies.append({
                    "article_id": art_id,
                    "title": art.get("title", "")[:60],
                    "dimension": "coherence",
                    "check": "duplicate",
                    "message": "DUPLICATE_ARTICLE_ID",
                })
                continue
            seen_ids.add(art_id)

            article_ok = True

            # Complétude
            for check_name, check_fn in COMPLETENESS_CHECKS:
                ok, msg = check_fn(art)
                if not ok:
                    completeness_failures[check_name] += 1
                    anomalies.append({
                        "article_id": art_id,
                        "title": art.get("title", "")[:60],
                        "dimension": "completeness",
                        "check": check_name,
                        "message": msg,
                    })
                    article_ok = False

            # Validité
            for check_name, check_fn in VALIDITY_CHECKS:
                ok, msg = check_fn(art)
                if not ok:
                    validity_failures[check_name] += 1
                    anomalies.append({
                        "article_id": art_id,
                        "title": art.get("title", "")[:60],
                        "dimension": "validity",
                        "check": check_name,
                        "message": msg,
                    })
                    article_ok = False

            if article_ok:
                passed_all += 1

        score = round(passed_all / total * 100, 1)

        summary = {
            "total": total,
            "passed": passed_all,
            "failed": total - passed_all,
            "duplicates": duplicates,
            "score": score,
            "completeness_failures": dict(completeness_failures),
            "validity_failures": dict(validity_failures),
            "checked_at": datetime.utcnow().isoformat(),
        }

        # Métriques par dimension
        completeness_score = self._dim_score(completeness_failures, total, len(COMPLETENESS_CHECKS))
        validity_score = self._dim_score(validity_failures, total, len(VALIDITY_CHECKS))
        coherence_score = round((1 - duplicates / total) * 100, 1)

        summary["dimensions"] = {
            "completeness": completeness_score,
            "validity": validity_score,
            "coherence": coherence_score,
        }

        logger.info(
            f"[DQ] Score: {score}% | "
            f"Complétude: {completeness_score}% | "
            f"Validité: {validity_score}% | "
            f"Cohérence: {coherence_score}% | "
            f"Doublons: {duplicates}"
        )

        return {"summary": summary, "anomalies": anomalies}

    def _dim_score(self, failures: Dict, total: int, n_checks: int) -> float:
        total_checks = total * n_checks
        total_failures = sum(failures.values())
        return round((1 - total_failures / max(total_checks, 1)) * 100, 1)

    def generate_report(self, report: Dict, output_path: str = None) -> str:
        summary = report["summary"]
        anomalies = report.get("anomalies", [])

        lines = [
            "=" * 60,
            "RAPPORT QUALITÉ DES DONNÉES",
            f"Généré le : {summary.get('checked_at', '')}",
            "=" * 60,
            f"Total articles     : {summary['total']}",
            f"Articles valides   : {summary['passed']}",
            f"Articles invalides : {summary['failed']}",
            f"Doublons           : {summary['duplicates']}",
            f"Score global       : {summary['score']}%",
            "",
            "Dimensions :",
            f"  Complétude : {summary['dimensions']['completeness']}%",
            f"  Validité   : {summary['dimensions']['validity']}%",
            f"  Cohérence  : {summary['dimensions']['coherence']}%",
            "",
        ]

        if summary.get("completeness_failures"):
            lines.append("Failures (complétude) :")
            for k, v in summary["completeness_failures"].items():
                lines.append(f"  {k}: {v}")
            lines.append("")

        if anomalies:
            lines.append(f"Anomalies ({len(anomalies)}) :")
            for a in anomalies[:20]:
                lines.append(f"  [{a['dimension']}] {a['check']}: {a['message']} | {a['title']}")
            if len(anomalies) > 20:
                lines.append(f"  ... et {len(anomalies) - 20} autres anomalies")

        lines.append("=" * 60)
        text = "\n".join(lines)

        if output_path:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            logger.info(f"Quality report saved to {output_path}")

        return text


if __name__ == "__main__":
    mock_articles = [
        {
            "article_id": "a1", "title": "Article test complet",
            "content": "Contenu suffisamment long pour passer les contrôles qualité. " * 5,
            "published_at": "2026-05-06T10:00:00", "url": "https://hespress.com/article-1",
            "source": "Hespress", "language": "fr", "country": "MA",
        },
        {
            "article_id": "a2", "title": "",  # Pas de titre
            "content": "Court", "published_at": None,
            "url": "not-a-valid-url", "source": "Unknown Source", "language": "",
        },
        {
            "article_id": "a1",  # Doublon
            "title": "Doublon", "content": "X" * 200,
            "published_at": "2026-05-06T10:00:00", "url": "https://hespress.com/article-1",
            "source": "Hespress", "language": "fr",
        },
    ]

    dq = DataQuality()
    report = dq.run_checks(mock_articles)
    print(dq.generate_report(report))
