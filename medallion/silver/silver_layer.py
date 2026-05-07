"""
Couche Silver — Nettoyage, normalisation et enrichissement des articles
Opérations :
  1. Suppression des balises HTML
  2. Normalisation du texte (espaces, encodage, casse)
  3. Détection de la langue
  4. Déduplication par article_id / URL
  5. Validation des champs obligatoires
"""

import re
import json
import logging
import unicodedata
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger("Silver")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def strip_html(text: str) -> str:
    """Supprime toutes les balises HTML et décode les entités."""
    if not text:
        return ""
    # Supprimer balises
    clean = re.sub(r"<[^>]+>", " ", text)
    # Décoder entités HTML courantes
    entities = {
        "&amp;": "&", "&lt;": "<", "&gt;": ">",
        "&quot;": '"', "&#39;": "'", "&nbsp;": " ",
        "&apos;": "'",
    }
    for ent, char in entities.items():
        clean = clean.replace(ent, char)
    return clean.strip()


def normalize_whitespace(text: str) -> str:
    """Compresse les espaces multiples et nettoie les sauts de ligne."""
    return re.sub(r"\s+", " ", text).strip()


def normalize_unicode(text: str) -> str:
    """Normalise les caractères Unicode (NFC)."""
    return unicodedata.normalize("NFC", text)


def clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = strip_html(text)
    text = normalize_unicode(text)
    text = normalize_whitespace(text)
    return text if text else None


def detect_language(text: str, declared_lang: Optional[str] = None) -> str:
    """
    Détection de langue basée sur des heuristiques de mots fréquents.
    Utilise langdetect si disponible, sinon fallback par patterns.
    """
    if not text or len(text) < 30:
        return declared_lang or "unknown"

    try:
        from langdetect import detect, LangDetectException
        detected = detect(text[:500])
        return detected
    except Exception:
        pass

    # Fallback heuristique
    text_lower = text.lower()

    arabic_chars = sum(1 for c in text if "\u0600" <= c <= "\u06ff")
    if arabic_chars / max(len(text), 1) > 0.2:
        return "ar"

    fr_words = ["le", "la", "les", "de", "du", "en", "est", "une", "que", "pour", "dans"]
    en_words = ["the", "a", "an", "is", "was", "are", "for", "in", "of", "and", "to"]

    words = re.findall(r"\b\w+\b", text_lower)
    word_set = set(words[:100])

    fr_score = sum(1 for w in fr_words if w in word_set)
    en_score = sum(1 for w in en_words if w in word_set)

    if fr_score > en_score:
        return "fr"
    elif en_score > 0:
        return "en"

    return declared_lang or "unknown"


def parse_date(date_str: Optional[str]) -> Optional[str]:
    """Tente de normaliser la date en ISO 8601."""
    if not date_str:
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%B %d, %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str[:25].strip(), fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return date_str  # retourne tel quel si parsing échoue


# ─────────────────────────────────────────────
# Silver Layer
# ─────────────────────────────────────────────

class SilverLayer:
    """
    Transforme les articles Bronze en articles Silver propres et normalisés.
    """

    MIN_CONTENT_LENGTH = 100
    MIN_TITLE_LENGTH = 5

    def transform(self, article: Dict) -> Optional[Dict]:
        """
        Applique toutes les transformations sur un article.
        Retourne None si l'article ne passe pas les validations minimales.
        """
        # 1. Nettoyage des champs texte
        title = clean_text(article.get("title"))
        author = clean_text(article.get("author"))
        content = clean_text(article.get("content"))
        category = clean_text(article.get("category"))

        # 2. Validation minimale
        if not title or len(title) < self.MIN_TITLE_LENGTH:
            logger.debug(f"[Silver] Rejected (no title): {article.get('url', '')}")
            return None
        if not content or len(content) < self.MIN_CONTENT_LENGTH:
            logger.debug(f"[Silver] Rejected (content too short): {title}")
            return None

        # 3. Détection de langue
        declared_lang = article.get("language")
        language = detect_language(content, declared_lang)

        # 4. Normalisation de la date
        published_at = parse_date(article.get("published_at"))

        # 5. Construction de l'article Silver
        silver_article = {
            "article_id": article.get("article_id"),
            "title": title,
            "author": author,
            "published_at": published_at,
            "category": category or "Non classé",
            "content": content,
            "content_length": len(content),
            "source": article.get("source"),
            "url": article.get("url"),
            "language": language,
            "country": article.get("country"),
            "scraped_at": article.get("scraped_at"),
            "processed_at": datetime.utcnow().isoformat(),
            "layer": "silver",
        }

        return silver_article

    def transform_batch(self, articles: List[Dict]) -> List[Dict]:
        """
        Transforme un batch d'articles et déduplique par article_id.
        """
        seen_ids = set()
        silver_articles = []

        for art in articles:
            transformed = self.transform(art)
            if transformed is None:
                continue

            art_id = transformed["article_id"]
            if art_id in seen_ids:
                logger.debug(f"[Silver] Duplicate skipped: {art_id}")
                continue

            seen_ids.add(art_id)
            silver_articles.append(transformed)

        rejected = len(articles) - len(silver_articles)
        logger.info(
            f"[Silver] {len(silver_articles)} articles transformed "
            f"({rejected} rejected/deduplicated from {len(articles)})"
        )
        return silver_articles

    def save(self, articles: List[Dict], path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        logger.info(f"[Silver] Saved {len(articles)} articles to {path}")


import os

if __name__ == "__main__":
    # Test avec des données mock
    mock = [
        {
            "article_id": "a1",
            "title": "<h1>Maroc : Réforme économique majeure</h1>",
            "author": "  Ahmed Bennani  ",
            "published_at": "06/05/2026 09:30",
            "category": "Economie",
            "content": "Le gouvernement marocain a annoncé une série de réformes économiques visant à moderniser le secteur industriel et à attirer davantage d'investissements étrangers. Ces mesures incluent des allègements fiscaux et des simplifications administratives.",
            "source": "Hespress",
            "url": "https://fr.hespress.com/article-test-1",
            "language": "fr",
            "scraped_at": "2026-05-06T09:45:00",
            "country": "MA",
        },
        {
            "article_id": "a2",
            "title": "",
            "content": "Trop court",
            "source": "Test",
            "url": "https://example.com/bad",
        },
    ]

    layer = SilverLayer()
    result = layer.transform_batch(mock)
    print(json.dumps(result, ensure_ascii=False, indent=2))
