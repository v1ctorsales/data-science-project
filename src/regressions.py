import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import re
import statsmodels.api as sm

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

def regression_analysis():
    input_path = PROCESSED_DIR / "merged_cleaned_data.csv"
    df = pd.read_csv(input_path)

    # Example regression analysis: GDP per capita vs Undernourishment
    df = df.dropna(subset=["gdp_percapita", "undernourishment"])

    X = df["gdp_percapita"]
    y = df["undernourishment"]

    X = sm.add_constant(X)  # Adds a constant term to the predictor

    model = sm.OLS(y, X).fit()
    predictions = model.predict(X)

    print(model.summary())

def stats_descriptive():
    print("Descriptive statistics for merged cleaned data:")
