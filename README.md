# Global Starvation Map – Data Science Project

This project aims to **process and serve global sustainable development indicators** (such as undernourishment, poverty, energy supply, and consumer prices) for visualization.  
The pipeline includes data cleaning, standardization, and integration of raw datasets into processed formats accessible via a **Python** FastAPI backend.

---

## Project Structure

```
data-science-project/
│
├── data/
│   ├── raw/                # Original CSV files
│   └── processed/          # Cleaned and standardized CSV files
│
├── src/
│   ├── __init__.py
│   ├── api.py              # FastAPI endpoints to serve processed data
│   ├── data_cleaner.py     # Functions for cleaning and processing datasets
│   └── main.py             # Main script for running cleaning tasks
│
├── requirements.txt        # Project dependencies
└── README.md
```

---

## Installation

1. Clone the repository:

```bash
git clone https://github.com/username/data-science-project.git
cd data-science-project
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
# or
.venv\Scripts\activate      # Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Data Cleaning Pipeline

The `src/data_cleaner.py` module contains reusable functions that clean, filter, and standardize each dataset located in the `data/raw/` folder.  
Each cleaned dataset is saved under `data/processed/`.

### Example – `clean_undernourishment()`

```python
def clean_undernourishment():
    input_path = RAW_DIR / "undernourishment.csv"
    output_path = PROCESSED_DIR / "undernourishment_clean.csv"

    df = pd.read_csv(input_path, skiprows=4)
    df.rename(columns={"Country Name": "country_name"}, inplace=True)

    cols_to_keep = ["country_name"] + [col for col in df.columns if col.isdigit() and int(col) >= 2001]
    df = df[cols_to_keep].dropna(how="all", subset=[c for c in df.columns if c != "country_name"])
    df["country_name"] = df["country_name"].str.strip()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("New clean file saved:", output_path)
```

### Running Cleaning Functions

The `main.py` script can be used to execute one or more cleaning functions:

```python
import numpy as np
from src.data_cleaner import clean_consumer_price_index

def main():
    clean_consumer_price_index()

if __name__ == "__main__":
    main()
```

Run in the terminal:

```bash
python src/main.py
```

---

## API Overview

The API (built with **FastAPI**) serves the cleaned datasets to the frontend.  
For example, the `/latest` endpoint returns the **most recent value available per country** for a given indicator.

### Example Endpoint

```
GET /latest?indicator=undernourishment
```

### Example Response

```json
[
  { "Country Name": "Brazil", "Value": 5.8, "Year": 2022 },
  { "Country Name": "India", "Value": 14.3, "Year": 2021 },
  { "Country Name": "Finland", "Value": 2.4, "Year": 2022 }
]
```

### Available Indicators

| Indicator | Cleaned File | Description |
|------------|--------------|-------------|
| undernourishment | `undernourishment_clean.csv` | Undernourishment rate (%) |
| food_calories | `food_calories_clean.csv` | Average calorie intake per capita |
| energy_supply | `energy_supply_adeq_clean.csv` | Energy supply adequacy |
| poverty | `poverty_clean.csv` | Population below the poverty line (%) |
| population | `population_clean.csv` | Total population |
| consumer_price | `consumer_price_index_clean.csv` | Consumer Price Index |

---

## Running the API

To start the server locally:

```bash
uvicorn src.api:app --reload
```

Then open your browser and visit:

```
http://127.0.0.1:8000/docs
```

You’ll see the automatically generated FastAPI Swagger documentation with all endpoints.

---

## Requirements

Your `requirements.txt` file should include at least the following dependencies:

```
pandas
numpy
fastapi
uvicorn
```

---

## Authors

**Victor Alves da Silva Sales** and **Valentina Serrano-Muñoz**

---

## License

This project is distributed under the **MIT License**.  
You are free to use, modify, and share it with proper attribution to the original author.
