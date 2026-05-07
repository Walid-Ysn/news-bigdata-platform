"""
Couche Gold — Tables analytiques prêtes pour le Data Warehouse
Agrégations produites :
  1. Tendances news (articles les plus actifs par catégorie)
  2. Top sujets / mots-clés
  3. Nombre d'articles par source
  4. Nombre d'articles par pays
  5. Nombre d'articles par jour
  6. Distribution des langues
"""

import json
import re
import logging
from collections import Counter
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger("Gold")

# Mots vides multilingues (FR + EN + AR communs)
STOPWORDS = {
    "fr": {"le","la","les","de","du","des","un","une","en","et","est","qui","que","dans","pour",
           "sur","par","avec","au","aux","ce","se","sa","son","ses","leur","leurs","il","elle",
           "ils","elles","nous","vous","on","mais","ou","donc","ni","car","ne","pas","plus","très",
           "bien","tout","aussi","comme","même","si","lui","a","y","n","l","d","j","m","s","c"},
    "en": {"the","a","an","and","or","but","in","on","at","to","for","of","with","by","is","was",
           "are","were","be","been","being","have","has","had","do","does","did","will","would",
           "could","should","may","might","shall","this","that","these","those","i","we","you",
           "he","she","it","they","what","which","who","how","when","where","why","not","from","as"},
    "ar": {"في","من","إلى","على","أن","هذا","هذه","التي","الذي","كان","قال","لا","ما","هو","هي"},
}

def get_stopwords(lang: Optional[str]) -> set:
    if lang in STOPWORDS:
        return STOPWORDS[lang]
    result = set()
    for s in STOPWORDS.values():
        result |= s
    return result


def extract_keywords(text: str, lang: Optional[str] = None, top_n: int = 10) -> List[str]:
    """Extrait les mots-clés significatifs d'un texte."""
    if not text:
        return []
    stopwords = get_stopwords(lang)
    words = re.findall(r"\b[a-zA-ZÀ-ÿ\u0621-\u064A]{4,}\b", text.lower())
    filtered = [w for w in words if w not in stopwords]
    counter = Counter(filtered)
    return [word for word, _ in counter.most_common(top_n)]


def parse_date_to_day(date_str: Optional[str]) -> Optional[str]:
    if not date_str:
        return None
    try:
        return date_str[:10]  # "YYYY-MM-DD"
    except Exception:
        return None


# ─────────────────────────────────────────────
# Gold Layer Processor
# ─────────────────────────────────────────────

class GoldLayer:
    """
    Produit les tables analytiques Gold à partir des articles Silver.
    """

    def compute_articles_per_source(self, articles: List[Dict]) -> List[Dict]:
        counter = Counter(art.get("source", "unknown") for art in articles)
        result = [
            {"source": src, "article_count": count, "computed_at": datetime.utcnow().isoformat()}
            for src, count in counter.most_common()
        ]
        logger.info(f"[Gold] articles_per_source: {len(result)} sources")
        return result

    def compute_articles_per_day(self, articles: List[Dict]) -> List[Dict]:
        counter = Counter(
            parse_date_to_day(art.get("published_at")) or parse_date_to_day(art.get("scraped_at"))
            for art in articles
        )
        result = sorted(
            [
                {"date": day, "article_count": count, "computed_at": datetime.utcnow().isoformat()}
                for day, count in counter.items() if day
            ],
            key=lambda x: x["date"],
            reverse=True,
        )
        logger.info(f"[Gold] articles_per_day: {len(result)} days")
        return result

    def compute_articles_per_category(self, articles: List[Dict]) -> List[Dict]:
        counter = Counter(art.get("category", "Non classé") for art in articles)
        result = [
            {"category": cat, "article_count": count, "computed_at": datetime.utcnow().isoformat()}
            for cat, count in counter.most_common()
        ]
        logger.info(f"[Gold] articles_per_category: {len(result)} categories")
        return result

    def compute_articles_per_country(self, articles: List[Dict]) -> List[Dict]:
        counter = Counter(art.get("country", "unknown") for art in articles)
        result = [
            {"country": c, "article_count": count, "computed_at": datetime.utcnow().isoformat()}
            for c, count in counter.most_common()
        ]
        logger.info(f"[Gold] articles_per_country: {len(result)} countries")
        return result

    def compute_language_distribution(self, articles: List[Dict]) -> List[Dict]:
        counter = Counter(art.get("language", "unknown") for art in articles)
        total = sum(counter.values())
        result = [
            {
                "language": lang,
                "article_count": count,
                "percentage": round(count / total * 100, 2),
                "computed_at": datetime.utcnow().isoformat(),
            }
            for lang, count in counter.most_common()
        ]
        logger.info(f"[Gold] language_distribution: {len(result)} languages")
        return result

    def compute_top_keywords(self, articles: List[Dict], top_n: int = 30) -> List[Dict]:
        """Agrège les mots-clés sur tous les articles."""
        all_keywords: Counter = Counter()
        for art in articles:
            lang = art.get("language")
            text = (art.get("title") or "") + " " + (art.get("content") or "")
            keywords = extract_keywords(text, lang, top_n=20)
            all_keywords.update(keywords)

        result = [
            {"keyword": kw, "frequency": freq, "computed_at": datetime.utcnow().isoformat()}
            for kw, freq in all_keywords.most_common(top_n)
        ]
        logger.info(f"[Gold] top_keywords: {len(result)} keywords")
        return result

    def compute_news_trends(self, articles: List[Dict]) -> List[Dict]:
        """
        Identifie les tendances : catégories avec le plus d'articles récents.
        Retourne les Top 10 avec article_count et exemples de titres.
        """
        from collections import defaultdict
        by_category: Dict[str, List] = defaultdict(list)
        for art in articles:
            cat = art.get("category", "Non classé") or "Non classé"
            by_category[cat].append(art)

        trends = []
        for cat, arts in sorted(by_category.items(), key=lambda x: -len(x[1])):
            sample_titles = [a["title"] for a in arts[:3] if a.get("title")]
            trends.append({
                "category": cat,
                "article_count": len(arts),
                "sample_titles": sample_titles,
                "sources": list({a.get("source") for a in arts if a.get("source")}),
                "computed_at": datetime.utcnow().isoformat(),
            })

        logger.info(f"[Gold] news_trends: {len(trends)} categories")
        return trends[:10]

    def compute_all(self, articles: List[Dict]) -> Dict[str, List[Dict]]:
        """Calcule toutes les tables Gold d'un coup."""
        return {
            "articles_per_source": self.compute_articles_per_source(articles),
            "articles_per_day": self.compute_articles_per_day(articles),
            "articles_per_category": self.compute_articles_per_category(articles),
            "articles_per_country": self.compute_articles_per_country(articles),
            "language_distribution": self.compute_language_distribution(articles),
            "top_keywords": self.compute_top_keywords(articles),
            "news_trends": self.compute_news_trends(articles),
        }

    def save_all(self, gold_data: Dict[str, List[Dict]], output_dir: str):
        import os
        os.makedirs(output_dir, exist_ok=True)
        for table_name, rows in gold_data.items():
            path = f"{output_dir}/{table_name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            logger.info(f"[Gold] Saved {table_name} ({len(rows)} rows) → {path}")


if __name__ == "__main__":
    import os
    # Données Silver mock pour test
    mock_silver = [
        {
            "article_id": f"id{i}",
            "title": f"Article test {i} - Economie marocaine",
            "content": "Le Maroc renforce ses infrastructures économiques avec des investissements massifs dans le secteur industriel et numérique.",
            "source": ["Hespress", "BBC News", "Al Jazeera"][i % 3],
            "category": ["Economie", "Politique", "Sport", "Technologie"][i % 4],
            "published_at": f"2026-05-0{(i % 6) + 1}T10:00:00",
            "language": ["fr", "en", "ar"][i % 3],
            "country": ["MA", "GB", "QA"][i % 3],
            "scraped_at": "2026-05-06T12:00:00",
        }
        for i in range(30)
    ]

    gold = GoldLayer()
    results = gold.compute_all(mock_silver)
    gold.save_all(results, "/tmp/gold_output")

    print("\nGold tables generated:")
    for table, rows in results.items():
        print(f"  {table}: {len(rows)} rows")
