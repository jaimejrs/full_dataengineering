import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── CONEXÃO ──────────────────────────────────────────────────────────────────
engine_dm = create_engine(os.getenv("DASHBOARD_DATABASE_URL"))

def carregar_dados() -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT id, ano, mes, qtde_vendida, valor_total_real, valor_total_esperado
        FROM public.vendas_ano_mes_jaime
        ORDER BY ano, mes
    """, engine_dm)
    df["desvio"]     = df["valor_total_real"] - df["valor_total_esperado"]
    df["pct_ating"]  = (df["valor_total_real"] / df["valor_total_esperado"] * 100).round(1)
    df["nome_mes"]   = df["mes"].map({
        1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",
        7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"
    })
    return df

def carregar_localidade() -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT ano, sigla_estado, estado, cidade,
               qtde_vendida, valor_total_real, valor_total_esperado, pct_atingimento
        FROM public.vendas_localidade_jaime
        ORDER BY ano, sigla_estado, cidade
    """, engine_dm)
    return df

DF = carregar_dados()
DF_LOC = carregar_localidade()

# ── PALETA DATADT ─────────────────────────────────────────────────────────────
BG      = "#0F0F1A"
SURFACE = "#17172A"
CARD    = "#1E1E35"
BORDER  = "#2A2A45"
ORANGE  = "#F26522"
ORANGE2 = "#FF8C42"
WHITE   = "#F0F0FA"
MUTED   = "#8888AA"
GREEN   = "#2ECC71"
RED     = "#E74C3C"
YELLOW  = "#F0C040"

# ── HELPERS ───────────────────────────────────────────────────────────────────
def fmt_brl(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"R$ {v/1_000_000:.2f}M"
    if abs(v) >= 1_000:
        return f"R$ {v/1_000:.1f}K"
    return f"R$ {v:,.2f}"

def plot_layout(height=260) -> dict:
    """Configuração base de layout para todos os gráficos."""
    return dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="'DM Sans', 'Segoe UI', sans-serif",
                  color=MUTED, size=11),
        margin=dict(l=8, r=8, t=8, b=8),
        xaxis=dict(
            showgrid=False, zeroline=False,
            linecolor=BORDER, tickfont=dict(size=11, color=MUTED),
        ),
        yaxis=dict(
            gridcolor=BORDER, zeroline=False,
            linecolor=BORDER, tickfont=dict(size=11, color=MUTED),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            orientation="h", y=1.15, x=0,
            font=dict(size=11, color=MUTED),
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=CARD, bordercolor=BORDER,
            font=dict(color=WHITE, size=12),
        ),
    )

def kpi_card(titulo: str, valor: str, sub: str, sub_cor: str, top_cor: str = ORANGE):
    return html.Div(style={
        "background":    CARD,
        "border":        f"1px solid {BORDER}",
        "borderTop":     f"3px solid {top_cor}",
        "borderRadius":  "12px",
        "padding":       "18px 16px 14px",
    }, children=[
        html.Div(titulo, style={
            "fontSize": "10px", "fontWeight": "700", "letterSpacing": "1px",
            "textTransform": "uppercase", "color": MUTED, "marginBottom": "10px",
        }),
        html.Div(valor, style={
            "fontFamily": "'Syne', 'Segoe UI', sans-serif",
            "fontSize": "22px", "fontWeight": "800", "color": WHITE, "lineHeight": "1.1",
        }),
        html.Div(sub, style={
            "fontSize": "11px", "marginTop": "6px",
            "fontWeight": "500", "color": sub_cor,
        }),
    ])

def graph_card(title: str, graph_id: str):
    return html.Div(style={
        "background": CARD, "border": f"1px solid {BORDER}",
        "borderRadius": "12px", "padding": "20px",
    }, children=[
        html.Div(title, style={
            "fontSize": "10px", "fontWeight": "700", "letterSpacing": "1px",
            "textTransform": "uppercase", "color": MUTED, "marginBottom": "14px",
        }),
        dcc.Graph(id=graph_id, config={"displayModeBar": False}),
    ])

# ── APP ───────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="Painel de Vendas",
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Syne:wght@700;800&"
        "family=DM+Sans:wght@300;400;500;600&display=swap"
    ],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

app.layout = html.Div(style={
    "backgroundColor": BG, "minHeight": "100vh",
    "fontFamily": "'DM Sans', 'Segoe UI', sans-serif",
    "color": WHITE,
}, children=[

    # ── HEADER ────────────────────────────────────────────────────────────────
    html.Div(style={
        "background": SURFACE,
        "borderBottom": f"2px solid {ORANGE}",
        "padding": "14px 32px",
        "display": "flex", "alignItems": "center", "justifyContent": "space-between",
    }, children=[

        html.Div(style={"display": "flex", "alignItems": "center", "gap": "14px"}, children=[
            html.Div("DC", style={
                "width": "42px", "height": "42px",
                "background": ORANGE, "borderRadius": "9px",
                "display": "flex", "alignItems": "center", "justifyContent": "center",
                "fontFamily": "'Syne', sans-serif", "fontWeight": "800",
                "fontSize": "17px", "color": "#fff", "letterSpacing": "-1px",
            }),
            html.Div(children=[
                html.Div("Digital College", style={
                    "fontFamily": "'Syne', sans-serif",
                    "fontWeight": "800", "fontSize": "18px", "color": WHITE,
                }),
                html.Div("Painel de Vendas · Data Mart", style={
                    "fontSize": "11px", "color": MUTED,
                }),
            ]),
        ]),

        html.Div(style={"display": "flex", "alignItems": "center", "gap": "12px"}, children=[
            html.Span("Ano", style={
                "fontSize": "11px", "fontWeight": "700",
                "letterSpacing": "1px", "textTransform": "uppercase", "color": MUTED,
            }),
            dcc.Dropdown(
                id="filtro-ano",
                options=[],
                value=None,
                clearable=False,
                style={
                    "width": "110px", "fontSize": "14px",
                    "fontFamily": "'Syne', sans-serif", "fontWeight": "700",
                    "backgroundColor": CARD, "color": WHITE,
                    "border": f"1px solid {ORANGE}", "borderRadius": "20px",
                },
            ),
        ]),
    ]),

    # ── CORPO ─────────────────────────────────────────────────────────────────
    html.Div(style={"padding": "28px 32px"}, children=[

        # 5 CARDS KPI
        html.Div(id="cards-kpi", style={
            "display": "grid",
            "gridTemplateColumns": "repeat(5, 1fr)",
            "gap": "14px",
            "marginBottom": "22px",
        }),

        # LINHA 1: Receita mensal (2/3) + Atingimento (1/3)
        html.Div(style={
            "display": "grid",
            "gridTemplateColumns": "2fr 1fr",
            "gap": "14px", "marginBottom": "14px",
        }, children=[
            graph_card("Receita Real vs Esperada por Mês", "g-receita"),
            graph_card("% Atingimento da Meta",            "g-ating"),
        ]),

        # LINHA 2: Qtde vendida (1/2) + Desvio (1/2)
        html.Div(style={
            "display": "grid",
            "gridTemplateColumns": "1fr 1fr",
            "gap": "14px",
        }, children=[
            graph_card("Quantidade Vendida por Mês", "g-qtde"),
            graph_card("Desvio Real − Esperado",     "g-desvio"),
        ]),

        # LINHA 3: Estado (1/2) + Cidade (1/2)
        html.Div(style={
            "display": "grid",
            "gridTemplateColumns": "1fr 1fr",
            "gap": "14px", "marginTop": "14px",
        }, children=[
            graph_card("Receita Real por Estado (Top 10)",          "g-estado"),
            graph_card("Receita Real vs. Meta — Top 20 Cidades",   "g-cidade"),
        ]),
    ]),
])

# ── CALLBACKS ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("filtro-ano", "options"),
    Output("filtro-ano", "value"),
    Input("filtro-ano", "id"),
)
def popular_anos(_):
    anos = sorted(DF["ano"].unique(), reverse=True)
    opts = [{"label": str(a), "value": a} for a in anos]
    return opts, anos[0]


@app.callback(
    Output("cards-kpi", "children"),
    Output("g-receita",  "figure"),
    Output("g-ating",    "figure"),
    Output("g-qtde",     "figure"),
    Output("g-desvio",   "figure"),
    Output("g-estado",   "figure"),
    Output("g-cidade",   "figure"),
    Input("filtro-ano",  "value"),
)
def atualizar(ano):
    if ano is None:
        raise dash.exceptions.PreventUpdate

    df = DF[DF["ano"] == ano].copy()

    # ── KPIs ─────────────────────────────────────────────────────────────────
    rec_real  = df["valor_total_real"].sum()
    rec_esp   = df["valor_total_esperado"].sum()
    qtde_tot  = df["qtde_vendida"].sum()
    desvio_t  = rec_real - rec_esp
    pct_geral = rec_real / rec_esp * 100 if rec_esp else 0
    ticket    = rec_real / qtde_tot if qtde_tot else 0
    idx_best  = df["valor_total_real"].idxmax()
    melhor_m  = df.loc[idx_best, "nome_mes"]
    melhor_v  = df.loc[idx_best, "valor_total_real"]

    cards = [
        kpi_card(
            "Receita Real", fmt_brl(rec_real),
            f"{'▲' if desvio_t>=0 else '▼'} {fmt_brl(abs(desvio_t))} vs meta",
            GREEN if desvio_t >= 0 else RED,
        ),
        kpi_card(
            "Meta Esperada", fmt_brl(rec_esp),
            "acumulado anual", MUTED,
        ),
        kpi_card(
            "Qtde Vendida", f"{int(qtde_tot):,}".replace(",","."),
            f"ticket médio {fmt_brl(ticket)}", MUTED,
        ),
        kpi_card(
            "Atingimento", f"{pct_geral:.1f}%",
            "acima da meta" if pct_geral >= 100 else "abaixo da meta",
            GREEN if pct_geral >= 100 else RED,
            top_cor=GREEN if pct_geral >= 100 else RED,
        ),
        kpi_card(
            "Melhor Mês", melhor_m,
            fmt_brl(melhor_v), MUTED,
        ),
    ]

    meses = df["nome_mes"].tolist()

    # ── G1: Receita Real vs Esperada ─────────────────────────────────────────
    fig_rec = go.Figure(layout=go.Layout(**plot_layout()))
    fig_rec.add_trace(go.Bar(
        x=meses, y=df["valor_total_esperado"],
        name="Esperado",
        marker_color=BORDER, marker_line_width=0,
        hovertemplate="%{y:,.0f}<extra>Esperado</extra>",
    ))
    fig_rec.add_trace(go.Bar(
        x=meses, y=df["valor_total_real"],
        name="Real",
        marker_color=ORANGE, marker_line_width=0,
        hovertemplate="%{y:,.0f}<extra>Real</extra>",
    ))
    fig_rec.update_layout(barmode="group", bargap=0.22, bargroupgap=0.06)

    # ── G2: % Atingimento ────────────────────────────────────────────────────
    cores_at = [
        GREEN if v >= 100 else YELLOW if v >= 85 else RED
        for v in df["pct_ating"]
    ]
    fig_at = go.Figure(layout=go.Layout(**plot_layout()))
    fig_at.add_trace(go.Bar(
        x=meses, y=df["pct_ating"],
        marker_color=cores_at, marker_line_width=0,
        showlegend=False,
        text=[f"{v:.0f}%" for v in df["pct_ating"]],
        textposition="outside",
        textfont=dict(size=10, color=MUTED),
        hovertemplate="%{y:.1f}%<extra></extra>",
    ))
    fig_at.add_hline(
        y=100, line_dash="dash", line_color=MUTED, line_width=1,
        annotation_text="meta 100%",
        annotation_font_size=10, annotation_font_color=MUTED,
    )

    # ── G3: Qtde vendida (linha + área) ──────────────────────────────────────
    fig_qt = go.Figure(layout=go.Layout(**plot_layout()))
    fig_qt.add_trace(go.Scatter(
        x=meses, y=df["qtde_vendida"],
        mode="lines+markers",
        name="Qtde",
        line=dict(color=ORANGE, width=2.5),
        marker=dict(size=7, color=ORANGE,
                    line=dict(color=CARD, width=2)),
        fill="tozeroy",
        fillcolor="rgba(242,101,34,0.12)",
        hovertemplate="%{y:,}<extra>Qtde vendida</extra>",
    ))

    # ── G4: Desvio por mês ────────────────────────────────────────────────────
    cores_dev = [GREEN if v >= 0 else RED for v in df["desvio"]]
    fig_dev = go.Figure(layout=go.Layout(**plot_layout()))
    fig_dev.add_trace(go.Bar(
        x=meses, y=df["desvio"],
        marker_color=cores_dev, marker_line_width=0,
        showlegend=False,
        text=[fmt_brl(v) for v in df["desvio"]],
        textposition="outside",
        textfont=dict(size=10, color=MUTED),
        hovertemplate="%{y:,.0f}<extra>Desvio</extra>",
    ))
    fig_dev.add_hline(y=0, line_color=BORDER, line_width=1)

    # ── G5: Receita Real por Estado (Top 10) ─────────────────────────────────
    df_loc = DF_LOC[DF_LOC["ano"] == ano].copy()
    estado_agg = (
        df_loc.groupby(["sigla_estado"], as_index=False)
        .agg(valor_total_real=("valor_total_real", "sum"),
             valor_total_esperado=("valor_total_esperado", "sum"))
    )
    estado_agg["pct"] = (estado_agg["valor_total_real"] / estado_agg["valor_total_esperado"] * 100).round(1)
    estado_agg = estado_agg.nlargest(10, "valor_total_real")

    fig_est = go.Figure(layout=go.Layout(**plot_layout(height=320)))
    fig_est.add_trace(go.Bar(
        y=estado_agg["sigla_estado"], x=estado_agg["valor_total_real"],
        orientation="h",
        marker_color=ORANGE, marker_line_width=0,
        text=[f"{fmt_brl(v)}  ({p:.0f}%)" for v, p in zip(estado_agg["valor_total_real"], estado_agg["pct"])],
        textposition="outside",
        textfont=dict(size=10, color=MUTED),
        hovertemplate="%{y}: %{x:,.0f}<extra></extra>",
        showlegend=False,
    ))
    fig_est.update_layout(yaxis=dict(autorange="reversed", categoryorder="total ascending"))

    # ── G6: Receita Real vs. Meta por Cidade (Top 20) ────────────────────────
    cidade_agg = (
        df_loc.groupby(["cidade"], as_index=False)
        .agg(valor_total_real=("valor_total_real", "sum"),
             valor_total_esperado=("valor_total_esperado", "sum"))
    )
    cidade_agg = cidade_agg.nlargest(20, "valor_total_real")

    fig_cid = go.Figure(layout=go.Layout(**plot_layout(height=320)))
    fig_cid.add_trace(go.Bar(
        x=cidade_agg["cidade"], y=cidade_agg["valor_total_esperado"],
        name="Meta",
        marker_color=BORDER, marker_line_width=0,
        hovertemplate="%{y:,.0f}<extra>Meta</extra>",
    ))
    fig_cid.add_trace(go.Bar(
        x=cidade_agg["cidade"], y=cidade_agg["valor_total_real"],
        name="Real",
        marker_color=ORANGE, marker_line_width=0,
        hovertemplate="%{y:,.0f}<extra>Real</extra>",
    ))
    fig_cid.update_layout(barmode="group", bargap=0.18, bargroupgap=0.06,
                          xaxis=dict(tickangle=-45, tickfont=dict(size=9)))

    return cards, fig_rec, fig_at, fig_qt, fig_dev, fig_est, fig_cid


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(
        debug=os.getenv("DASH_DEBUG", "false").lower() == "true",
        host=os.getenv("DASH_HOST", "0.0.0.0"),
        port=int(os.getenv("DASH_PORT", "8050")),
    )
