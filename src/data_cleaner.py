import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

def clean_undernourishment():
    input_path = RAW_DIR / "undernourishment.csv"
    output_path = PROCESSED_DIR / "undernourishment_clean.csv"

    df = pd.read_csv(input_path, skiprows=4)

    cols_to_keep = ["Country Name"] + [col for col in df.columns if col.isdigit() and int(col) >= 2001]
    df = df[cols_to_keep]
    df = df.dropna(how="all", subset=[col for col in df.columns if col != "Country Name"])
    df["Country Name"] = df["Country Name"].str.strip()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved: ", {output_path})

if __name__ == "__main__":
    clean_undernourishment()
