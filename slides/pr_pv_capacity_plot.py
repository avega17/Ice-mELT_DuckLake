from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

DATA_ROWS = slice(3, 189)
EXCEL_PATH = Path("db/pv_domain/LUMA-Interconnections-Prog-Report-Jun-2025.xlsx")
SHEET_NAME = "Monthly "


def load_monthly_capacity_records(path: Path = EXCEL_PATH) -> pd.DataFrame:
    """Return monthly PV capacity data with dates, kW totals, and client counts."""
    df = pd.read_excel(path, sheet_name=SHEET_NAME, usecols="A:J", header=None)
    df = df.iloc[DATA_ROWS].rename(columns={1: "period", 8: "capacity_kw", 9: "client_count"})
    data = df.loc[:, ["period", "capacity_kw", "client_count"]].copy()
    data = data.assign(
        period=pd.to_datetime(data["period"], errors="coerce"),
        capacity_kw=pd.to_numeric(data["capacity_kw"], errors="coerce"),
        client_count=pd.to_numeric(data["client_count"], errors="coerce"),
    )
    data = data.dropna(subset=["period", "capacity_kw"])
    data = data.sort_values("period").reset_index(drop=True)
    data.loc[:, "client_count"] = data["client_count"].astype("Int64")
    data.loc[:, "capacity_mw"] = data["capacity_kw"] / 1000
    return data


def build_capacity_figure(data: pd.DataFrame) -> go.Figure:
    """Create an interactive line chart of PV capacity in MW with client hover info."""
    fig = go.Figure(
        data=[
            go.Scatter(
                x=data["period"],
                y=data["capacity_mw"],
                mode="lines+markers",
                customdata=data[["client_count"]],
                hovertemplate=(
                    "Quarter: %{x|%Y-Q%q}<br>"
                    "Installed Capacity: %{y:,.1f} MW<br>"
                    "Clients: %{customdata[0]:,}<extra></extra>"
                ),
                line=dict(color="#1f77b4", width=2),
                marker=dict(size=6),
                name="Installed Capacity",
            )
        ]
    )
    fig.update_layout(
        title="Puerto Rico Grid-Connected PV Installed Capacity",
        xaxis_title="Quarter",
        yaxis_title="Installed Capacity (MW)",
        hovermode="x unified",
        template="plotly_white",
    )
    fig.update_xaxes(dtick="M3", tickformat="%Y-Q%q", rangeslider=dict(visible=True))
    fig.update_yaxes(ticksuffix=" MW", rangemode="tozero")
    return fig


if __name__ == "__main__":
    capacity_data = load_monthly_capacity_records()
    print(capacity_data.head())
    print(capacity_data.tail())
    print(capacity_data.describe(include="all"))
    figure = build_capacity_figure(capacity_data)
    output_path = Path(__file__).with_suffix(".html")
    figure.write_html(output_path, include_plotlyjs="cdn")
    print(f"Interactive chart saved to {output_path}")
