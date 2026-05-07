"""
Dashboard de visualisation — Plotly Dash
Tableaux de bord :
  1. Tendances d'actualité (articles par catégorie)
  2. Nombre d'articles par source
  3. Mots-clés les plus fréquents
  4. Distribution des langues
  5. Articles par jour (timeline)
"""

import json
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger("Dashboard")

# ─────────────────────────────────────────────
# Données mock pour le dashboard autonome
# ─────────────────────────────────────────────

def get_mock_data() -> Dict:
    import random
    random.seed(42)

    sources = ["Hespress", "Akhbarona", "BBC News", "Al Jazeera", "Reuters", "CNN", "Barlamane"]
    categories = ["Politique", "Economie", "Sport", "Technologie", "Culture", "International", "Société"]
    keywords = [
        "Maroc", "gouvernement", "économie", "investissement", "réforme",
        "football", "mondial", "Gaza", "Palestine", "technologie",
        "intelligence", "artificielle", "développement", "éducation", "santé",
        "élection", "parlement", "budget", "croissance", "tourisme",
    ]

    # Articles par source
    per_source = [
        {"source": s, "article_count": random.randint(40, 200)}
        for s in sources
    ]

    # Articles par catégorie (tendances)
    trends = [
        {"category": c, "article_count": random.randint(20, 150)}
        for c in categories
    ]
    trends.sort(key=lambda x: -x["article_count"])

    # Top keywords
    top_keywords = [
        {"keyword": kw, "frequency": random.randint(10, 200)}
        for kw in keywords
    ]
    top_keywords.sort(key=lambda x: -x["frequency"])

    # Articles par jour (30 derniers jours)
    base_date = datetime.utcnow()
    per_day = [
        {
            "date": (base_date - timedelta(days=i)).strftime("%Y-%m-%d"),
            "article_count": random.randint(50, 300),
        }
        for i in range(30)
    ]
    per_day.sort(key=lambda x: x["date"])

    # Distribution langues
    lang_dist = [
        {"language": "Français", "article_count": 420, "percentage": 42.0},
        {"language": "Arabe",    "article_count": 350, "percentage": 35.0},
        {"language": "Anglais",  "article_count": 230, "percentage": 23.0},
    ]

    return {
        "per_source": per_source,
        "trends": trends,
        "top_keywords": top_keywords[:15],
        "per_day": per_day,
        "lang_dist": lang_dist,
        "total_articles": sum(x["article_count"] for x in per_source),
        "total_sources": len(sources),
        "total_categories": len(categories),
    }


def load_data_from_dw() -> Dict:
    """Charge les données depuis le Data Warehouse si disponible."""
    try:
        from warehouse.warehouse import DataWarehouse
        dw = DataWarehouse()

        per_source = dw.query("SELECT source, article_count FROM gold_articles_per_source ORDER BY article_count DESC LIMIT 20")
        per_day = dw.query("SELECT date, article_count FROM gold_articles_per_day ORDER BY date DESC LIMIT 30")
        per_category = dw.query("SELECT category, article_count FROM gold_articles_per_category ORDER BY article_count DESC")
        keywords = dw.query("SELECT keyword, frequency FROM gold_top_keywords ORDER BY frequency DESC LIMIT 20")

        if per_source or per_day:
            return {
                "per_source": per_source,
                "trends": per_category,
                "top_keywords": keywords,
                "per_day": sorted(per_day, key=lambda x: x["date"]),
                "lang_dist": [],
                "total_articles": sum(r.get("article_count", 0) for r in per_source),
                "total_sources": len(per_source),
                "total_categories": len(per_category),
            }
    except Exception as e:
        logger.info(f"DW not available ({e}), using mock data")

    return get_mock_data()


# ─────────────────────────────────────────────
# Dashboard Dash
# ─────────────────────────────────────────────

def create_app():
    try:
        import dash
        from dash import dcc, html, Input, Output
        import dash_bootstrap_components as dbc
        import plotly.express as px
        import plotly.graph_objects as go
        import pandas as pd
    except ImportError:
        logger.error("Dash/Plotly not installed. Run: pip install dash dash-bootstrap-components plotly")
        return None

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.DARKLY],
        title="News Big Data Dashboard",
    )

    # ── Layout ──────────────────────────────────────────────────
    app.layout = dbc.Container([
        # Header
        dbc.Row([
            dbc.Col([
                html.H1("📰 News Big Data Platform", className="text-center text-warning mt-3"),
                html.P("Plateforme d'analyse des tendances médiatiques — EMSI IADATA 2025/2026",
                       className="text-center text-muted"),
                html.Hr(className="border-warning"),
            ])
        ]),

        # KPI Cards
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(id="kpi-total", className="text-warning text-center"),
                    html.P("Articles collectés", className="text-center text-muted mb-0"),
                ])
            ], className="mb-3 border-warning"), width=3),

            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(id="kpi-sources", className="text-info text-center"),
                    html.P("Sources actives", className="text-center text-muted mb-0"),
                ])
            ], className="mb-3 border-info"), width=3),

            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(id="kpi-categories", className="text-success text-center"),
                    html.P("Catégories", className="text-center text-muted mb-0"),
                ])
            ], className="mb-3 border-success"), width=3),

            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H4(id="kpi-updated", className="text-secondary text-center"),
                    html.P("Dernière mise à jour", className="text-center text-muted mb-0"),
                ])
            ], className="mb-3"), width=3),
        ]),

        # Refresh + Store
        dcc.Interval(id="interval", interval=300_000, n_intervals=0),  # refresh 5 min
        dcc.Store(id="data-store"),

        # Row 1: Timeline + Sources
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("📅 Articles par jour (30 derniers jours)", className="text-warning"),
                    dbc.CardBody(dcc.Graph(id="chart-timeline")),
                ], className="mb-3 border-secondary")
            ], width=8),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("📊 Distribution des langues", className="text-info"),
                    dbc.CardBody(dcc.Graph(id="chart-lang")),
                ], className="mb-3 border-secondary")
            ], width=4),
        ]),

        # Row 2: Sources + Categories
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🏢 Articles par source", className="text-warning"),
                    dbc.CardBody(dcc.Graph(id="chart-sources")),
                ], className="mb-3 border-secondary")
            ], width=6),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("📈 Tendances par catégorie", className="text-success"),
                    dbc.CardBody(dcc.Graph(id="chart-trends")),
                ], className="mb-3 border-secondary")
            ], width=6),
        ]),

        # Row 3: Keywords
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🔑 Mots-clés les plus fréquents", className="text-info"),
                    dbc.CardBody(dcc.Graph(id="chart-keywords")),
                ], className="mb-3 border-secondary")
            ], width=12),
        ]),

        # Footer
        html.Footer(
            html.P("EMSI Casablanca — Filière IADATA — Architecture de données 2025/2026",
                   className="text-center text-muted mt-2 mb-3"),
        ),

    ], fluid=True, className="bg-dark")

    # ── Callbacks ────────────────────────────────────────────────

    @app.callback(
        Output("data-store", "data"),
        Input("interval", "n_intervals"),
    )
    def refresh_data(n):
        return load_data_from_dw()

    @app.callback(
        [Output("kpi-total", "children"),
         Output("kpi-sources", "children"),
         Output("kpi-categories", "children"),
         Output("kpi-updated", "children")],
        Input("data-store", "data"),
    )
    def update_kpis(data):
        if not data:
            data = get_mock_data()
        return (
            f"{data['total_articles']:,}",
            str(data["total_sources"]),
            str(data["total_categories"]),
            datetime.utcnow().strftime("%H:%M UTC"),
        )

    @app.callback(Output("chart-timeline", "figure"), Input("data-store", "data"))
    def update_timeline(data):
        if not data:
            data = get_mock_data()
        import plotly.graph_objects as go
        df = data["per_day"]
        dates = [r["date"] for r in df]
        counts = [r["article_count"] for r in df]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=counts, mode="lines+markers",
            line=dict(color="#FFC107", width=2),
            marker=dict(size=5),
            fill="tozeroy", fillcolor="rgba(255,193,7,0.1)",
            name="Articles/jour",
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#aaa"), margin=dict(t=10, b=30),
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        return fig

    @app.callback(Output("chart-lang", "figure"), Input("data-store", "data"))
    def update_lang(data):
        if not data:
            data = get_mock_data()
        import plotly.express as px
        df = data["lang_dist"]
        if not df:
            df = [{"language": "N/A", "article_count": 1, "percentage": 100}]
        fig = px.pie(
            df, values="article_count", names="language",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4,
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#aaa"),
            margin=dict(t=10, b=10), showlegend=True,
            legend=dict(orientation="h"),
        )
        return fig

    @app.callback(Output("chart-sources", "figure"), Input("data-store", "data"))
    def update_sources(data):
        if not data:
            data = get_mock_data()
        import plotly.express as px
        df = sorted(data["per_source"], key=lambda x: x["article_count"])
        fig = px.bar(
            df, x="article_count", y="source", orientation="h",
            color="article_count",
            color_continuous_scale="YlOrRd",
            labels={"article_count": "Articles", "source": "Source"},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#aaa"), margin=dict(t=10, b=30),
            coloraxis_showscale=False,
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        return fig

    @app.callback(Output("chart-trends", "figure"), Input("data-store", "data"))
    def update_trends(data):
        if not data:
            data = get_mock_data()
        import plotly.express as px
        df = data["trends"][:8]
        fig = px.bar(
            df, x="category", y="article_count",
            color="article_count", color_continuous_scale="Teal",
            labels={"article_count": "Articles", "category": "Catégorie"},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#aaa"), margin=dict(t=10, b=50),
            coloraxis_showscale=False,
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        return fig

    @app.callback(Output("chart-keywords", "figure"), Input("data-store", "data"))
    def update_keywords(data):
        if not data:
            data = get_mock_data()
        import plotly.graph_objects as go
        kws = data["top_keywords"]
        words = [k["keyword"] for k in kws]
        freqs = [k["frequency"] for k in kws]
        fig = go.Figure(go.Bar(
            x=words, y=freqs,
            marker_color=[f"rgba(0,{150 + i*5},200,0.8)" for i in range(len(words))],
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#aaa"), margin=dict(t=10, b=50),
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        return fig

    return app


if __name__ == "__main__":
    app = create_app()
    if app:
        print("\nDashboard: http://localhost:8050\n")
        app.run(debug=True, host="0.0.0.0", port=8050)
    else:
        print("Install dependencies: pip install dash dash-bootstrap-components plotly")
