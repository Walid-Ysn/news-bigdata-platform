"""
News Scraper - Collecte automatique d'articles de presse
Sources : Hespress, Akhbarona, Al Jazeera, BBC News, Reuters, CNN
"""

import requests
from bs4 import BeautifulSoup
import json
import uuid
import hashlib
from datetime import datetime
import time
import logging
from typing import Optional
from dataclasses import dataclass, asdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("NewsScraper")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8,ar;q=0.7",
}


@dataclass
class Article:
    article_id: str
    title: str
    author: Optional[str]
    published_at: Optional[str]
    category: Optional[str]
    content: str
    source: str
    url: str
    language: Optional[str]
    scraped_at: str
    country: str

    def to_dict(self):
        return asdict(self)

    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def make_article_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def safe_get(url: str, timeout: int = 15) -> Optional[BeautifulSoup]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def text_or_none(tag) -> Optional[str]:
    return tag.get_text(strip=True) if tag else None


# ─────────────────────────────────────────────
# Scraper: Hespress (Maroc - FR/AR)
# ─────────────────────────────────────────────
class HespressScraper:
    BASE_URL = "https://fr.hespress.com"
    SOURCE = "Hespress"
    COUNTRY = "MA"
    LANGUAGE = "fr"

    def get_article_urls(self, category_url: str, max_articles: int = 20):
        soup = safe_get(category_url)
        if not soup:
            return []
        links = soup.select("h3.card-title a")
        urls = list({self.BASE_URL + a["href"] for a in links if a.get("href")})
        return urls[:max_articles]

    def scrape_article(self, url: str) -> Optional[Article]:
        soup = safe_get(url)
        if not soup:
            return None

        title = text_or_none(soup.select_one("h1.post-title")) or \
                text_or_none(soup.select_one("h1"))
        author = text_or_none(soup.select_one("span.author")) or \
                 text_or_none(soup.select_one("a[rel='author']"))
        date_tag = soup.select_one("time") or soup.select_one("span.date")
        published_at = date_tag.get("datetime") if date_tag and date_tag.get("datetime") else text_or_none(date_tag)
        category_tag = soup.select_one("span.cat-links a") or soup.select_one("a.cat-link")
        category = text_or_none(category_tag)
        content_tags = soup.select("div.post-content p") or soup.select("article p")
        content = " ".join(p.get_text(strip=True) for p in content_tags)

        if not title or len(content) < 50:
            return None

        return Article(
            article_id=make_article_id(url),
            title=title,
            author=author,
            published_at=published_at,
            category=category,
            content=content,
            source=self.SOURCE,
            url=url,
            language=self.LANGUAGE,
            scraped_at=datetime.utcnow().isoformat(),
            country=self.COUNTRY,
        )

    def scrape(self, max_articles: int = 20):
        categories = [
            f"{self.BASE_URL}/category/politique",
            f"{self.BASE_URL}/category/societe",
            f"{self.BASE_URL}/category/economie",
        ]
        articles = []
        for cat_url in categories:
            urls = self.get_article_urls(cat_url, max_articles // len(categories))
            for url in urls:
                art = self.scrape_article(url)
                if art:
                    articles.append(art)
                time.sleep(1.5)
        logger.info(f"[{self.SOURCE}] Scraped {len(articles)} articles")
        return articles


# ─────────────────────────────────────────────
# Scraper: Akhbarona (Maroc - AR)
# ─────────────────────────────────────────────
class AkhbaronaScraper:
    BASE_URL = "https://www.akhbarona.com"
    SOURCE = "Akhbarona"
    COUNTRY = "MA"
    LANGUAGE = "ar"

    def get_article_urls(self, max_articles: int = 20):
        soup = safe_get(self.BASE_URL)
        if not soup:
            return []
        links = soup.select("a[href*='/news/']")
        urls = list({
            a["href"] if a["href"].startswith("http") else self.BASE_URL + a["href"]
            for a in links if a.get("href") and "/news/" in a["href"]
        })
        return urls[:max_articles]

    def scrape_article(self, url: str) -> Optional[Article]:
        soup = safe_get(url)
        if not soup:
            return None

        title = text_or_none(soup.select_one("h1")) or text_or_none(soup.select_one(".article-title"))
        author = text_or_none(soup.select_one(".author-name")) or text_or_none(soup.select_one(".author"))
        date_tag = soup.select_one("time") or soup.select_one(".date")
        published_at = date_tag.get("datetime") if date_tag and date_tag.get("datetime") else text_or_none(date_tag)
        category = text_or_none(soup.select_one(".breadcrumb li:last-child"))
        content_tags = soup.select(".article-body p") or soup.select("article p")
        content = " ".join(p.get_text(strip=True) for p in content_tags)

        if not title or len(content) < 50:
            return None

        return Article(
            article_id=make_article_id(url),
            title=title,
            author=author,
            published_at=published_at,
            category=category,
            content=content,
            source=self.SOURCE,
            url=url,
            language=self.LANGUAGE,
            scraped_at=datetime.utcnow().isoformat(),
            country=self.COUNTRY,
        )

    def scrape(self, max_articles: int = 20):
        urls = self.get_article_urls(max_articles)
        articles = []
        for url in urls:
            art = self.scrape_article(url)
            if art:
                articles.append(art)
            time.sleep(1.5)
        logger.info(f"[{self.SOURCE}] Scraped {len(articles)} articles")
        return articles


# ─────────────────────────────────────────────
# Scraper: BBC News (International - EN)
# ─────────────────────────────────────────────
class BBCScraper:
    BASE_URL = "https://www.bbc.com"
    SOURCE = "BBC News"
    COUNTRY = "GB"
    LANGUAGE = "en"

    def get_article_urls(self, max_articles: int = 20):
        soup = safe_get(f"{self.BASE_URL}/news/world")
        if not soup:
            return []
        links = soup.select("a[data-testid='internal-link']")
        urls = []
        seen = set()
        for a in links:
            href = a.get("href", "")
            if "/news/articles/" in href or "/news/world" in href:
                full = href if href.startswith("http") else self.BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        return urls[:max_articles]

    def scrape_article(self, url: str) -> Optional[Article]:
        soup = safe_get(url)
        if not soup:
            return None

        title = text_or_none(soup.select_one("h1[id='main-heading']")) or \
                text_or_none(soup.select_one("h1"))
        author = text_or_none(soup.select_one("div[data-component='byline-block']")) or \
                 text_or_none(soup.select_one(".ssrcss-1i69lhj-Contributor"))
        date_tag = soup.select_one("time")
        published_at = date_tag.get("datetime") if date_tag else None
        category = "World News"
        content_tags = soup.select("div[data-component='text-block'] p")
        content = " ".join(p.get_text(strip=True) for p in content_tags)

        if not title or len(content) < 50:
            return None

        return Article(
            article_id=make_article_id(url),
            title=title,
            author=author,
            published_at=published_at,
            category=category,
            content=content,
            source=self.SOURCE,
            url=url,
            language=self.LANGUAGE,
            scraped_at=datetime.utcnow().isoformat(),
            country=self.COUNTRY,
        )

    def scrape(self, max_articles: int = 20):
        urls = self.get_article_urls(max_articles)
        articles = []
        for url in urls:
            art = self.scrape_article(url)
            if art:
                articles.append(art)
            time.sleep(1.5)
        logger.info(f"[{self.SOURCE}] Scraped {len(articles)} articles")
        return articles


# ─────────────────────────────────────────────
# Scraper: Al Jazeera (International - EN/AR)
# ─────────────────────────────────────────────
class AlJazeeraScraper:
    BASE_URL = "https://www.aljazeera.com"
    SOURCE = "Al Jazeera"
    COUNTRY = "QA"
    LANGUAGE = "en"

    def get_article_urls(self, max_articles: int = 20):
        soup = safe_get(f"{self.BASE_URL}/news/")
        if not soup:
            return []
        links = soup.select("a[href*='/news/']")
        urls = list({
            self.BASE_URL + a["href"] if not a["href"].startswith("http") else a["href"]
            for a in links if a.get("href") and len(a["href"]) > 10
        })
        return [u for u in urls if "/news/20" in u][:max_articles]

    def scrape_article(self, url: str) -> Optional[Article]:
        soup = safe_get(url)
        if not soup:
            return None

        title = text_or_none(soup.select_one("h1"))
        author = text_or_none(soup.select_one(".article-author-name")) or \
                 text_or_none(soup.select_one("a[rel='author']"))
        date_tag = soup.select_one("time") or soup.select_one(".date-simple")
        published_at = date_tag.get("datetime") if date_tag and date_tag.get("datetime") else text_or_none(date_tag)
        category_tag = soup.select_one(".article-topic a") or soup.select_one("nav a.topic")
        category = text_or_none(category_tag) or "News"
        content_tags = soup.select(".wysiwyg p") or soup.select("article p")
        content = " ".join(p.get_text(strip=True) for p in content_tags)

        if not title or len(content) < 50:
            return None

        return Article(
            article_id=make_article_id(url),
            title=title,
            author=author,
            published_at=published_at,
            category=category,
            content=content,
            source=self.SOURCE,
            url=url,
            language=self.LANGUAGE,
            scraped_at=datetime.utcnow().isoformat(),
            country=self.COUNTRY,
        )

    def scrape(self, max_articles: int = 20):
        urls = self.get_article_urls(max_articles)
        articles = []
        for url in urls:
            art = self.scrape_article(url)
            if art:
                articles.append(art)
            time.sleep(1.5)
        logger.info(f"[{self.SOURCE}] Scraped {len(articles)} articles")
        return articles


# ─────────────────────────────────────────────
# Scraper Manager - runs all scrapers
# ─────────────────────────────────────────────
class ScraperManager:
    def __init__(self, max_per_source: int = 20):
        self.max_per_source = max_per_source
        self.scrapers = [
            HespressScraper(),
            AkhbaronaScraper(),
            BBCScraper(),
            AlJazeeraScraper(),
        ]

    def run_all(self):
        all_articles = []
        for scraper in self.scrapers:
            try:
                articles = scraper.scrape(self.max_per_source)
                all_articles.extend(articles)
            except Exception as e:
                logger.error(f"Scraper {scraper.SOURCE} failed: {e}")
        logger.info(f"Total articles scraped: {len(all_articles)}")
        return all_articles

    def save_to_json(self, articles, output_path: str):
        data = [a.to_dict() for a in articles]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(data)} articles to {output_path}")
        return output_path


if __name__ == "__main__":
    manager = ScraperManager(max_per_source=15)
    articles = manager.run_all()
    manager.save_to_json(articles, "/tmp/articles_raw.json")
    print(f"\nDone. {len(articles)} articles collected.")
