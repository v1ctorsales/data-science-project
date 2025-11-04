import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

def clean_undernourishment():
    input_path = RAW_DIR / "undernourishment.csv"
    output_path = PROCESSED_DIR / "undernourishment_clean.csv"

    df = pd.read_csv(input_path, skiprows=4)
    df.rename(columns={"Country Name": "country_name"}, inplace=True)

    cols_to_keep = ["country_name"] + [col for col in df.columns if col.isdigit() and int(col) >= 2001]
    df = df[cols_to_keep]
    df = df.dropna(how="all", subset=[col for col in df.columns if col != "country_name"])
    df["country_name"] = df["country_name"].str.strip()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved:", output_path)

def clean_consumer_price_index():
    input_path = RAW_DIR / "consumer_price_index.csv"
    output_path = PROCESSED_DIR / "consumer_price_index_clean.csv"

    df = pd.read_csv(input_path, encoding="utf-8", low_memory=False)

    keep_cols = ["Area", "Item"] + [col for col in df.columns if col.startswith("Y20")]
    df = df[keep_cols].rename(columns={"Area": "country_name", "Item": "indicator_name"})

    year_cols = [col for col in df.columns if re.fullmatch(r"Y\d{4}", col)]

    for c in year_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df_long = df.melt(
        id_vars=["country_name", "indicator_name"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )
    df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)
    df_long = df_long.dropna(subset=["value"])

    result = (
        df_long.groupby(["country_name", "indicator_name", "year"], as_index=False)
        .agg(mean_value=("value", "mean"))
    )

    result = result[(result["year"] >= 2000) & (result["year"] <= 2025)]

    result_wide = result.pivot(
        index=["country_name", "indicator_name"],
        columns="year",
        values="mean_value",
    ).reset_index()

    result_wide.columns.name = None

    non_year_cols = ["country_name", "indicator_name"]
    year_cols = sorted([c for c in result_wide.columns if isinstance(c, int)])
    result_wide = result_wide[non_year_cols + year_cols]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    result_wide.to_csv(output_path, index=False)
    print("New clean file saved: ", {output_path})

def clean_energy_supply_adequacy():
    input_path = RAW_DIR / "energy_supply_adeq.csv"
    output_path = PROCESSED_DIR / "energy_supply_adeq_clean.csv"

    df = pd.read_csv(input_path, encoding="utf-8", low_memory=False)

    for col in ["Area", "Year", "Value"]:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in CSV.")

    def midpoint(year_str):
        match = re.findall(r"\d{4}", str(year_str))
        if len(match) == 2:
            start, end = map(int, match)
            return (start + end) // 2
        elif len(match) == 1:
            return int(match[0])
        else:
            return None

    df["mid_year"] = df["Year"].apply(midpoint)

    df = df[["Area", "mid_year", "Value"]].copy()
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

    df = df.groupby(["Area", "mid_year"], as_index=False)["Value"].mean()

    df_wide = df.pivot(index="Area", columns="mid_year", values="Value").reset_index()

    df_wide.rename(columns={"Area": "country_name"}, inplace=True)

    year_cols = sorted([c for c in df_wide.columns if isinstance(c, int)])
    df_wide = df_wide[["country_name"] + year_cols]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_wide.to_csv(output_path, index=False)

    print("New clean file saved: ", {output_path})

def clean_food_calories():
    input_path = RAW_DIR / "food_calories.csv"
    output_path = PROCESSED_DIR / "food_calories_clean.csv"

    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    data = []
    for line in lines:
        clean_line = line.strip().replace('"', '').split(",")
        if len(clean_line) >= 3:
            country, year, value = clean_line[:3]
            data.append([country.strip(), year.strip(), value.strip()])

    df = pd.DataFrame(data, columns=["country_name", "year", "value"])

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["country_name", "year", "value"])

    df_wide = df.pivot(index="country_name", columns="year", values="value").reset_index()

    year_cols = sorted([c for c in df_wide.columns if isinstance(c, (int, float))])
    df_wide = df_wide[["country_name"] + year_cols]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_wide.to_csv(output_path, index=False)

    print("New clean file saved:", output_path)

def clean_poverty():
    input_path = RAW_DIR / "poverty.csv"
    output_path = PROCESSED_DIR / "poverty_clean.csv"

    df = pd.read_csv(input_path, header=2, quotechar='"', skip_blank_lines=True)
    df = df.dropna(axis=1, how='all')

    # Rename columns to match other datasets
    df.rename(columns={"Country Name": "country_name"}, inplace=True)
    df.rename(columns={"Country Code": "country_code"}, inplace=True)
    df.rename(columns={"Indicator Name": "indicator_name"}, inplace=True)

# Count how many years have data per row
    df["data_count"] = df.drop(columns=["country_name", "indicator_name"]).notna().sum(axis=1)

# Count non-NaN values for each row, only considering year columns
    year_cols = [col for col in df.columns if col.isdigit()]
    df["n_obs"] = df[year_cols].count(axis=1)

    indicator_counts = (
        df.groupby(["indicator_name", "Indicator Code"])["n_obs"]
        .sum()
        .reset_index()
        .sort_values(by="n_obs", ascending=False)
    )

    # Keep relevant indicators
    filter_indicators = ["SI.POV.DDAY"] # (8.30) SI.POV.UMIC  (4.20) SI.POV.LMIC
    df = df[df["Indicator Code"].isin(filter_indicators)]

    # # Select relevant years
    cols_to_keep = ["country_name", "country_code", "indicator_name"] + [col for col in df.columns if col.isdigit() and int(col) >= 2000]
    df = df[cols_to_keep]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved:", output_path)

def clean_population():
    input_path = RAW_DIR / "population.csv"
    output_path = PROCESSED_DIR / "population_clean.csv"

    df = pd.read_csv(input_path, header=2, quotechar='"', skip_blank_lines=True)
    df = df.dropna(axis=1, how='all')

  # Rename columns to match other datasets
    df.rename(columns={"Country Name": "country_name"}, inplace=True)
    df.rename(columns={"Country Code": "country_code"}, inplace=True)
    df.rename(columns={"Indicator Name": "indicator_name"}, inplace=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved:", output_path)