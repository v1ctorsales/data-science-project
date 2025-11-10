from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
import re

app = FastAPI(title="Data Science Project API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


@app.get("/")
def root():
    return {"message": "Welcome to the Data Science Project API"}


@app.get("/data/{dataset}")
def get_dataset(
    dataset: str,
    country: str | None = Query(None, description="Country name (e.g., Brazil)"),
    start_year: int | None = Query(None, description="Start year (e.g., 2005)"),
    end_year: int | None = Query(None, description="End year (e.g., 2015)"),
    indicator: str | None = Query(None, description="Indicator name (if applicable)"),
):
    file_path = PROCESSED_DIR / f"{dataset}.csv"
    if not file_path.exists():
        return {"error": f"Dataset '{dataset}' not found."}

    df = pd.read_csv(file_path)
    df.columns = [str(c).lower() for c in df.columns]

    if country:
        df = df[df["country_name"].str.lower() == country.lower()]

    if indicator and "indicator_name" in df.columns:
        df = df[df["indicator_name"].str.lower().str.contains(indicator.lower())]

    year_cols = [col for col in df.columns if col.isdigit()]
    if start_year or end_year:
        start = start_year or min(map(int, year_cols))
        end = end_year or max(map(int, year_cols))
        valid_cols = [
            "country_name",
            "indicator_name" if "indicator_name" in df.columns else None,
        ] + [y for y in year_cols if start <= int(y) <= end]
        valid_cols = [c for c in valid_cols if c]
        df = df[valid_cols]

    df = df.replace({float("nan"): None})
    df = df.where(pd.notnull(df), None)

    return df.to_dict(orient="records")



@app.get("/countries")
def get_countries():
    file_path = PROCESSED_DIR / "all_data_merged.csv"
    if not file_path.exists():
        return {"error": "Merged dataset not found."}

    df = pd.read_csv(file_path, usecols=["country_name"])
    return df["country_name"].dropna().unique().tolist()


@app.get("/latest")
def get_latest_data(indicator: str = Query(..., description="Indicator name, e.g. 'undernourishment'")):
    file_map = {
        "undernourishment": "undernourishment_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "energy_supply": "energy_supply_adeq_clean.csv",
        "poverty": "poverty_clean.csv",
        "population": "population_clean.csv",
        "consumer_price": "consumer_price_index_clean.csv",
    }

    if indicator not in file_map:
        return {"error": f"Unknown indicator '{indicator}'"}

    file_path = PROCESSED_DIR / file_map[indicator]
    if not file_path.exists():
        return {"error": f"Dataset for indicator '{indicator}' not found."}

    # === Leitura e limpeza ===
    df = pd.read_csv(file_path)
    df.columns = [str(c).strip().replace(",", "").replace('"', "") for c in df.columns]
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", c)]

    if not year_cols:
        return {"error": f"No year columns found in {file_path.name}"}

    # === Para cada país, pegar o último valor não nulo ===
    latest_records = []
    for _, row in df.iterrows():
        values = {y: row[y] for y in year_cols if pd.notna(row[y])}
        if not values:
            continue
        latest_year = max(values.keys(), key=int)
        latest_value = values[latest_year]
        latest_records.append({
            "Country Name": row["country_name"],
            "Value": latest_value,
            "Year": int(latest_year)
        })

    if not latest_records:
        return {"error": "No valid non-null data found."}

    df_latest = pd.DataFrame(latest_records)
    df_latest = df_latest.sort_values("Country Name").reset_index(drop=True)

    print(f"✅ Sent latest data per country for '{indicator}' ({len(df_latest)} records)")
    return df_latest.to_dict(orient="records")



@app.get("/country")
def get_country_timeseries(
    indicator: str = Query(..., description="Indicator name, e.g. 'undernourishment'"),
    country: str = Query(..., description="Country name, e.g. 'Brazil'")
):
    file_map = {
        "undernourishment": "undernourishment_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "energy_supply": "energy_supply_adeq_clean.csv",
        "poverty": "poverty_clean.csv",
        "population": "population_clean.csv",
        "consumer_price": "consumer_price_index_clean.csv",
    }

    if indicator not in file_map:
        return {"error": f"Unknown indicator '{indicator}'"}

    file_path = PROCESSED_DIR / file_map[indicator]
    if not file_path.exists():
        return {"error": f"Dataset for indicator '{indicator}' not found."}

    df = pd.read_csv(file_path)
    df.columns = [str(c).strip().replace(",", "").replace('"', "") for c in df.columns]
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", c)]

    df = df[df["country_name"].str.lower() == country.lower()]
    if df.empty:
        return {"error": f"No data found for country '{country}' in indicator '{indicator}'"}

    result = df[["country_name"] + year_cols].iloc[0].to_dict()

    for k, v in result.items():
        if pd.isna(v):
            result[k] = None

    print(f"📊 Sent time series for '{country}' ({indicator}) with {len(year_cols)} years")
    return [result]

@app.get("/indicators")
def analyze_indicators(
    country: str = Query(..., description="Country name (e.g., Brazil)"),
    indicators: list[str] = Query(..., description="List of indicators (max 2)"),
):
    import numpy as np
    import pandas as pd
    import re

    if len(indicators) < 2:
        return {"error": "Please provide at least two indicators for comparison."}
    if len(indicators) > 2:
        return {"error": "Only two indicators can be compared at a time."}

    file_map = {
        "undernourishment": "undernourishment_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "energy_supply": "energy_supply_adeq_clean.csv",
        "poverty": "poverty_clean.csv",
        "population": "population_clean.csv",
        "consumer_price_index": "consumer_price_index_clean.csv",
    }

    dfs = {}
    for ind in indicators:
        if ind not in file_map:
            return {"error": f"Unknown indicator '{ind}'"}

        path = PROCESSED_DIR / file_map[ind]
        if not path.exists():
            return {"error": f"Dataset for '{ind}' not found"}

        df = pd.read_csv(path)
        df.columns = [str(c).strip().replace(",", "").replace('"', "") for c in df.columns]
        df = df[df["country_name"].str.lower() == country.lower()]

        if df.empty:
            return {"error": f"No data for '{country}' in '{ind}'"}

        year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", c)]
        s = df[year_cols].iloc[0].dropna().astype(float)
        s.index = s.index.astype(int)

        dfs[ind] = s

    combined = pd.concat(dfs, axis=1)
    combined.sort_index(inplace=True)

    combined.dropna(how="all", inplace=True)

    combined.interpolate(inplace=True, limit_direction="both")

    if combined.dropna().shape[0] < 3:
        return {"error": "Not enough valid data to compare these indicators."}

    valid = combined.dropna()
    corr = float(np.corrcoef(valid[indicators[0]], valid[indicators[1]])[0, 1])
    trend = "direct" if corr >= 0 else "inverse"

    mean_diff = float(np.mean(np.abs(valid[indicators[0]] - valid[indicators[1]])))

    normalized = (combined - combined.min()) / (combined.max() - combined.min())

    details = {}
    for ind in indicators:
        details[ind] = [
            {
                "year": int(y),
                "value": float(combined.loc[y, ind]),
                "normalized": float(normalized.loc[y, ind]),
            }
            for y in combined.index
        ]

    return {
        "country": country,
        "indicators": indicators,
        "correlation": round(corr, 3),
        "summary": {
            "years_overlap": int(valid.shape[0]),
            "trend": trend,
            "mean_difference": round(mean_diff, 2),
        },
        "details": details,
    }

