from __future__ import annotations

import os

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from dash import dash_table, dcc, html

from fraudstream.dashboard.data import gold_table


def empty_message(table: str) -> html.Div:
    return html.Div(
        [
            html.H3("No Gold data yet"),
            html.P(f"Run `scripts/generate_offline_demo_data.sh` or `make run-pipeline` to build {table}."),
        ],
        className="empty-state",
    )


def metric_card(title: str, value: str, color: str = "primary") -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([html.Div(title, className="metric-title"), html.H3(value, className=f"text-{color}")]),
        className="metric-card",
    )


def layout() -> dbc.Container:
    summary = gold_table("gold_daily_fraud_summary")
    alerts = gold_table("gold_realtime_alerts")
    customers = gold_table("gold_customer_risk_profile")
    merchants = gold_table("gold_merchant_risk_profile")
    rules = gold_table("gold_fraud_rule_performance")
    dq = gold_table("gold_data_quality_scorecard")

    if summary.empty:
        return dbc.Container(empty_message("Gold tables"), fluid=True)

    total_tx = int(summary["transactions"].sum())
    high_risk = int(summary["high_risk_transactions"].sum())
    total_amount = float(summary["total_amount"].sum())
    avg_risk = float(summary["avg_risk_score"].mean())

    charts = []
    charts.append(dcc.Graph(figure=px.line(summary, x="event_date", y="total_amount", title="Daily Transaction Amount")))
    charts.append(dcc.Graph(figure=px.bar(summary, x="event_date", y="high_risk_transactions", title="High-Risk Transactions")))
    if not rules.empty:
        charts.append(dcc.Graph(figure=px.bar(rules, x="rule_name", y="triggered_count", title="Fraud Rule Trigger Volume")))
    if not merchants.empty:
        top_merchants = merchants.sort_values("high_risk_transactions", ascending=False).head(15)
        charts.append(dcc.Graph(figure=px.bar(top_merchants, x="merchant_id", y="high_risk_transactions", color="merchant_category", title="Merchant Risk Hotspots")))

    return dbc.Container(
        [
            html.Div(
                [
                    html.H1("FraudStream Lakehouse"),
                    html.P("Realtime fraud risk monitoring across transactions, customers, merchants, devices, and data quality."),
                ],
                className="hero",
            ),
            dbc.Row(
                [
                    dbc.Col(metric_card("Transactions", f"{total_tx:,}"), md=3),
                    dbc.Col(metric_card("High Risk", f"{high_risk:,}", "danger"), md=3),
                    dbc.Col(metric_card("Total Amount", f"${total_amount:,.0f}", "success"), md=3),
                    dbc.Col(metric_card("Avg Risk Score", f"{avg_risk:,.1f}", "warning"), md=3),
                ],
                className="g-3",
            ),
            html.H2("Risk Analytics"),
            dbc.Row([dbc.Col(chart, md=6) for chart in charts], className="g-3"),
            html.H2("Realtime Alerts"),
            dash_table.DataTable(
                alerts.head(25).to_dict("records"),
                [{"name": col, "id": col} for col in alerts.columns],
                page_size=10,
                style_table={"overflowX": "auto"},
            )
            if not alerts.empty
            else empty_message("alerts"),
            html.H2("Customer Risk 360"),
            dash_table.DataTable(
                customers.sort_values("max_risk_score", ascending=False).head(25).to_dict("records"),
                [{"name": col, "id": col} for col in customers.columns],
                page_size=10,
                style_table={"overflowX": "auto"},
            )
            if not customers.empty
            else empty_message("customer risk"),
            html.H2("Data Quality Scorecard"),
            dash_table.DataTable(
                dq.to_dict("records"),
                [{"name": col, "id": col} for col in dq.columns],
                page_size=10,
                style_table={"overflowX": "auto"},
            )
            if not dq.empty
            else empty_message("DQ scorecard"),
        ],
        fluid=True,
        className="app-container",
    )


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "FraudStream Lakehouse"
app.layout = layout


def main() -> None:
    app.run(
        host=os.getenv("DASH_HOST", "0.0.0.0"),
        port=int(os.getenv("DASH_PORT", "8060")),
        debug=False,
    )


if __name__ == "__main__":
    main()
