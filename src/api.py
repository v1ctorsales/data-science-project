from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
import re
import numpy as np


from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware


app = FastAPI(title="Data Science Project API")

def real_ip(request: Request):
    return request.headers.get("X-Forwarded-For", request.client.host)

limiter = Limiter(key_func=real_ip)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded,
    lambda request, exc: JSONResponse(
        status_code=429,
        content={"detail": "Too many requests, slow down."}
    )
)
app.add_middleware(SlowAPIMiddleware)

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
@limiter.limit("40/minute")
def root(request: Request):
    return {"message": "Welcome to the Data Science Project API"}


@app.get("/data/{dataset}")
@limiter.limit("40/minute")
def get_dataset(
        request: Request,
        dataset: str,
        country: str | None = Query(None, description="Country name (e.g., Brazil)"),
        start_year: int | None = Query(None, description="Start year (e.g., 2005)"),
        end_year: int | None = Query(None, description="End year (e.g., 2015)"),
        indicator: str | None = Query(None, description="Indicator name (if applicable)"),
):
    # 1. Carrega o dataset principal
    file_path = PROCESSED_DIR / f"{dataset}.csv"
    if not file_path.exists():
        return {"error": f"Dataset '{dataset}' not found."}

    df = pd.read_csv(file_path)
    df.columns = [str(c).lower() for c in df.columns]

    # 2. Filtros principais (País, Indicador)
    if country:
        df = df[df["country_name"].str.lower() == country.lower()]

    if indicator and "indicator_name" in df.columns:
        df = df[df["indicator_name"].str.lower().str.contains(indicator.lower())]

    # 3. Filtro de Anos
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

    # 4. Limpeza para JSON (Dataset principal)
    df = df.replace({float("nan"): None})
    df = df.where(pd.notnull(df), None)

    # ---------------------------------------------------------
    # NOVO BLOCO: Buscar Correlações
    # ---------------------------------------------------------
    country_correlations = None  # Padrão caso não haja país ou arquivo

    if country:
        corr_path = PROCESSED_DIR / "country_indicator_correlation.csv"

        if corr_path.exists():
            # Carrega o CSV de correlações
            df_corr = pd.read_csv(corr_path)

            # Filtra pelo país solicitado (insensível a maiúsculas/minúsculas)
            # Assumindo que o CSV tem uma coluna 'country_name' ou similar (o índice do reset_index)
            # Se no passo anterior salvamos com index=False, deve haver uma coluna 'country_name'
            match = df_corr[df_corr['country_name'].str.lower() == country.lower()]

            if not match.empty:
                # Pega a primeira linha encontrada e converte para dicionário
                corr_data = match.iloc[0].to_dict()

                # Remove NaN do dicionário de correlação para não quebrar o JSON
                # (Itera sobre o dict e troca float('nan') por None)
                cleaned_corr = {}
                for k, v in corr_data.items():
                    try:
                        if np.isnan(v):
                            cleaned_corr[k] = None
                        else:
                            cleaned_corr[k] = v
                    except:
                        cleaned_corr[k] = v

                country_correlations = cleaned_corr

    # ---------------------------------------------------------
    # RETORNO FINAL
    # ---------------------------------------------------------

    # Atenção: Isso muda o formato da resposta de [{}, {}] para {"data": [], "correlations": {}}
    return {
        "data": df.to_dict(orient="records"),
        "correlations": country_correlations
    }


@app.get("/countries")
@limiter.limit("40/minute")
def get_countries(request: Request):
    file_path = PROCESSED_DIR / "all_data_merged.csv"
    if not file_path.exists():
        return {"error": "Merged dataset not found."}

    df = pd.read_csv(file_path, usecols=["country_name"])
    return df["country_name"].dropna().unique().tolist()


@app.get("/latest")
@limiter.limit("40/minute")
def get_latest_data(request: Request, indicator: str = Query(..., description="Indicator name, e.g. 'undernourishment'")):
    file_map = {
        "mean_inflation": "mean_inflation_rate.csv",
        "max_inflation": "max_inflation_shock.csv",
        "poverty": "poverty_clean.csv",
        "undernourishment": "undernourishment_clean.csv",
        "population": "population_clean.csv",
        "energy_suply_adeq": "energy_supply_adeq_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "gdp": "gdp_percapita_clean.csv",
    }

    if indicator not in file_map:
        return {"error": f"Unknown indicator '{indicator}'"}

    file_path = PROCESSED_DIR / file_map[indicator]
    if not file_path.exists():
        return {"error": f"Dataset for indicator '{indicator}' not found."}

    df = pd.read_csv(file_path)
    df.columns = [str(c).strip().replace(",", "").replace('"', "") for c in df.columns]
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", c)]

    if not year_cols:
        return {"error": f"No year columns found in {file_path.name}"}

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
@limiter.limit("40/minute")
def get_country_timeseries(
    request: Request,
    indicator: str = Query(..., description="Indicator name, e.g. 'undernourishment'"),
    country: str = Query(..., description="Country name, e.g. 'Brazil'")
):
    file_map = {
        "mean_inflation": "mean_inflation_rate.csv",
        "max_inflation": "max_inflation_shock.csv",
        "poverty": "poverty_clean.csv",
        "undernourishment": "undernourishment_clean.csv",
        "population": "population_clean.csv",
        "energy_suply_adeq": "energy_supply_adeq_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "gdp": "gdp_percapita_clean.csv",
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
@limiter.limit("40/minute")
def analyze_indicators(
    request: Request,
    country: str = Query(..., description="Country name (e.g., Brazil)"),
    indicators: list[str] = Query(..., description="List of indicators (max 2)"),
):

    if len(indicators) < 2:
        return {"error": "Please provide at least two indicators for comparison."}
    if len(indicators) > 2:
        return {"error": "Only two indicators can be compared at a time."}

    file_map = {
        "mean_inflation": "mean_inflation_rate.csv",
        "max_inflation": "max_inflation_shock.csv",
        "poverty": "poverty_clean.csv",
        "undernourishment": "undernourishment_clean.csv",
        "population": "population_clean.csv",
        "energy_suply_adeq": "energy_supply_adeq_clean.csv",
        "food_calories": "food_calories_clean.csv",
        "gdp": "gdp_percapita_clean.csv",
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

    combined = combined[combined.index >= 2001]

    combined.dropna(how="all", inplace=True)

    combined = combined.dropna(how="all")

    if combined.dropna().shape[0] < 3:
        return {"error": "Not enough valid data to compare these indicators."}

    valid = combined.dropna()
    corr = float(np.corrcoef(valid[indicators[0]], valid[indicators[1]])[0, 1])
    trend = "direct" if corr >= 0 else "inverse"

    mean_diff = float(np.mean(np.abs(valid[indicators[0]] - valid[indicators[1]])))

    normalized = (combined - combined.min()) / (combined.max() - combined.min())

    details = {}
    for ind in indicators:
        details[ind] = []
        for y in combined.index:
            val = combined.loc[y, ind]
            norm = normalized.loc[y, ind]

            # Convert NaN → None
            val = None if pd.isna(val) else float(val)
            norm = None if pd.isna(norm) else float(norm)

            details[ind].append({
                "year": int(y),
                "value": val,
                "normalized": norm,
            })

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

