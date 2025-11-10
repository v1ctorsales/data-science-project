import pandas as pd
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

## ------------------------ Undernourishment ----------------------- ##

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

## ------------------------ Consumer Price Index ----------------------- ##

def clean_consumer_price_index():
    input_path = RAW_DIR / "consumer_price_index.csv"
    output_path = PROCESSED_DIR / "consumer_price_index_clean.csv"
    output_path1 = PROCESSED_DIR / "inflation_data.csv"

    # Import data 
    df = pd.read_csv(input_path, encoding="utf-8", low_memory=False)

    # Keep relevant columns and rename
    keep_cols = ["Area", "Item", "Months Code"] + [col for col in df.columns if col.startswith("Y20")]
    df = df[keep_cols].rename(columns={"Area": "country_name", "Item": "indicator_name", "Months Code": "months_code"})

    # Clean year columns to numeric
    year_cols = [col for col in df.columns if re.fullmatch(r"Y\d{4}", col)]

    for c in year_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep relevant indicators (we only want food indices)
    filter_indicators = ["Consumer Prices, Food Indices (2015 = 100)"]
    df = df[df["indicator_name"].isin(filter_indicators)]

    df_long = df.melt(
        id_vars=["country_name", "indicator_name", "months_code"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )

    df_long["year"] = df_long["year"].str.extract(r"(\d{4})").astype(int)
    df_long = df_long.dropna(subset=["value"])

    df_long = df_long.sort_values(by=['country_name', 'year', 'months_code'])

    # Different computing of Inflation for years previous 2025 and after
    df_long_25 = df_long[df_long['year'] >= 2025].copy()
    df_long = df_long[df_long['year'] < 2025].copy()
    
    print(df_long_25.head())
    print(df_long.head())

    # Verifying if shift(12) method will work for this data set 
    monthly_count = df_long.groupby(['country_name', 'year'])['months_code'].nunique().reset_index()
    monthly_count = monthly_count.rename(columns={'months_code': 'months_available'})
    print(monthly_count.head().to_markdown(index=False))
    monthly_count['Has_12_Months'] = monthly_count['months_available'] == 12
    incomplete_data = monthly_count[monthly_count['Has_12_Months'] == False]

    if not incomplete_data.empty:
        print("\n--- Incomplete Country/Year Data Found ---")
        print(f"Total incomplete records: {len(incomplete_data)}")
        print("Example incomplete records (First 5):")
        print(incomplete_data.head().to_markdown(index=False))
    else:
        print("\n All Country/Year combinations appear to have 12 unique months of data.")

    # It won't since not all countries have data for the 12 months for each year. From this: 
        #   Different computing of Inflation for years previous 2025 and after
        #   shift(12) method won't work here. We need to do a manual merge of the current year data with the lagged year data.

    ## Merge with previous year to compute <=2024 inflation
    # 1. Prepare the Current DataFrame (P_t)
    current_df = df_long[['country_name', 'year', 'months_code', 'value']].copy()
    current_df = current_df.rename(columns={'value': 'CFPI_t'})

    # 2. Prepare the Lagged DataFrame (P_t-12)
    lagged_df = df_long[['country_name', 'year', 'months_code', 'value']].copy()
    # Shift the year forward by 1. Define the Lagged Year (T-1)
    lagged_df['year'] = lagged_df['year'] + 1 
    lagged_df = lagged_df.rename(columns={'value': 'CFPI_t_minus_12'})

    # Merge the two dataframes based on three conditions:
    # 1. Same Country Name
    # 2. Same Month Code (e.g., 7001 for January)
    # 3. Current Year = Lagged Year (e.g., Year 2022 matched with Lagged Year 2022)
    inflation_data = pd.merge(
        current_df,
        lagged_df,
        on=['country_name', 'year', 'months_code'],
        how='left' # Use 'left' to keep all current data points
    )

    # 3. Calculate the Year-over-Year Inflation Rate using the joined columns (CFPI_t)
    inflation_data['Inflation_Rate'] = (
        inflation_data['CFPI_t'] / inflation_data['CFPI_t_minus_12'] - 1
    ) * 100

    # Remove rows where inflation couldn't be calculated (i.e., the first 12 months of the series)
    inflation_data = inflation_data.dropna(subset=['Inflation_Rate']).copy()

    # Manual verification 
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    inflation_data.to_csv(output_path1, index=False) 
    print("New clean file saved:", output_path)



## ------------------------ Energy Supply Adequacy ----------------------- ##

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

## ------------------------ Food Calories ----------------------- ##

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

## ------------------------ Poverty ----------------------- ##

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


## ------------------------ Population ----------------------- ##

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

def clean_gdp_percapita():
    input_path = RAW_DIR / "gdp_percapita.csv"
    output_path = PROCESSED_DIR / "gdp_percapita_clean.csv"

    df = pd.read_csv(input_path, header=2, quotechar='"', skip_blank_lines=True)
    df = df.dropna(axis=1, how='all')
    df = df.dropna(how='all')

  # Rename columns to match other datasets
    df.rename(columns={"Country Name": "country_name"}, inplace=True)
    df.rename(columns={"Country Code": "country_code"}, inplace=True)
    df.rename(columns={"Indicator Name": "indicator_name"}, inplace=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved:", output_path)


## ------------------------ Merge Cleaned Data ----------------------- ##

def merge_cleaned_data():
    output_path = PROCESSED_DIR / "all_data_merged.csv"

    # Lista de arquivos limpos para combinar
    files = [
        "undernourishment_clean.csv",
        "consumer_price_index_clean.csv",
        "energy_supply_adeq_clean.csv",
        "food_calories_clean.csv",
        "poverty_clean.csv",
        "population_clean.csv",
        "gdp_percapita.csv"
    ]

    dataframes = []
    for file in files:
        path = PROCESSED_DIR / file
        if not path.exists():
            print(f"Warning: {file} not found, skipping.")
            continue

        df = pd.read_csv(path)

        # Garante a existência de 'country_name'
        if "country_name" not in df.columns:
            print(f"Warning: {file} does not contain 'country_name', skipping.")
            continue

        # Mantém apenas colunas numéricas de ano (2001–2025)
        year_cols = [col for col in df.columns if re.fullmatch(r"\d{4}", str(col)) and int(col) >= 2001]
        df = df[["country_name"] + year_cols]

        # Adiciona prefixo para evitar colisão de colunas
        prefix = file.replace("_clean.csv", "")
        df = df.rename(columns={col: f"{prefix}_{col}" for col in year_cols})

        dataframes.append(df)

    if not dataframes:
        print("No cleaned files found to merge.")
        return

    # Faz merge incremental em 'country_name'
    merged_df = dataframes[0]
    for df in dataframes[1:]:
        merged_df = pd.merge(merged_df, df, on="country_name", how="outer")

    # Ordena colunas: primeiro 'country_name', depois anos
    cols = ["country_name"] + sorted([c for c in merged_df.columns if c != "country_name"])
    merged_df = merged_df[cols]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    print("✅ Merged dataset created:", output_path)
