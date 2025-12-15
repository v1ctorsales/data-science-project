import pandas as pd
import numpy as np

from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def correlation():
    input_path = PROCESSED_DIR / "all_data_merged.csv"
    df_wide = pd.read_csv(input_path)

    # 2. "Melt" the data: Turn all Year columns into rows
    # This stacks everything into one long column
    df_melted = df_wide.melt(id_vars=['country_name'], var_name='original_col', value_name='value')

    # 3. Extract Year and Indicator Name using Regular Expressions (Regex). We look for the pattern: "AnyText_YYYY"
    df_melted[['indicator', 'year']] = df_melted['original_col'].str.extract(r'(.*)_(\d{4})')

    # 4. Convert year to integer and drop rows that didn't match the pattern
    df_melted['year'] = pd.to_numeric(df_melted['year'])
    df_melted = df_melted.dropna(subset=['year'])

    # 5. Pivot: Spread the indicators back into their own columns
    # Now, 'energy_supply_adeq' will be one column, 'pou' will be another, etc.
    df_final = df_melted.pivot_table(
        index=['country_name', 'year'], 
        columns='indicator', 
        values='value'
    ).reset_index()

    # 6. Run the Correlation Matrix
    # Filter for numeric columns only (excluding year if you prefer)
    numeric_df = df_final.drop(columns=['country_name', 'year'])


    plt.figure(figsize=(12, 10))
    sns.heatmap(numeric_df.corr(), annot=True, cmap='coolwarm', vmin=-1, vmax=1)
    plt.title('Correlation Matrix of Food Security Drivers')
    plt.show()

    output_path = PROCESSED_DIR / "long_food_security_data.csv"

    # Export dataset for manual validation 
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(output_path, index=False) 
    print("New clean file saved:", output_path)


def regression():
    input_path = PROCESSED_DIR / "long_food_security_data.csv"
    df_final = pd.read_csv(input_path)

    # 1. Define Features
    features = [
        'energy_supply_adeq', 
        'gdp_percapita', 
        'poverty',
        'max_inflation_shock'
    ]
    target = 'undernourishment'

    # 2. Sort by Country and Year (Crucial for Forward Fill)
    df_sorted = df_final.sort_values(by=['country_name', 'year'])

    # 3. Forward Fill (The "Data Science" Fix)
    # We group by country so we don't accidentally fill Angola's data into Argentina
    # We select only the columns we care about + the target
    cols_to_fill = features + [target]
    
    # Create a new dataframe with filled values
    df_filled = df_sorted.groupby('country_name')[cols_to_fill].ffill()

    # Add the identifier columns back (Groupby removes them from the columns list)
    df_filled['year'] = df_sorted['year']
    df_filled['country_name'] = df_sorted['country_name']

    #  Cleanup
    df_clean = df_filled.dropna()

    print(f"Original size: {len(df_sorted)}")
    print(f"Final Cleaned size: {len(df_clean)}") 
    # ^ You should see ~3015 here now.

    # ---------------------------------------------------------
    # PART 2: THE REGRESSION (Time Series Split)
    # ---------------------------------------------------------
    
    # 5. Split by Year (2020 Cutoff)
    cutoff_year = 2020
    
    # Ensure strict integer type for filtering
    df_clean['year'] = df_clean['year'].astype(int)

    train_data = df_clean[df_clean['year'] < cutoff_year]
    test_data  = df_clean[df_clean['year'] >= cutoff_year]

    X_train = train_data[features]
    y_train = train_data[target]

    X_test = test_data[features]
    y_test = test_data[target]

    print(f"Training on {len(X_train)} rows (Pre-2020)")
    print(f"Testing on  {len(X_test)} rows (2020+)")

    # Run model
    model = LinearRegression()
    model.fit(X_train, y_train)

    print("\n--- Model Coefficients (Drivers of Hunger) ---")
    for feature, coef in zip(features, model.coef_):
        print(f"{feature}: {coef:.4f}")
        
    print(f"Intercept: {model.intercept_:.4f}")
    
    # 8. Check Accuracy
    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    print(f"\nR-Squared (Accuracy): {r2:.4f}")

FILE_NAME = "long_food_security_data.csv"
OUTPUT_FILE_NAME = "country_indicator_correlation.csv"

input_path = PROCESSED_DIR / "long_food_security_data.csv"

def calculate_correlation_by_country(
    min_observations: int = 5,
    output_file: str = "country_indicator_correlation.csv"
):

    input_path = PROCESSED_DIR / "long_food_security_data.csv"
    df = pd.read_csv(input_path)

    target = "undernourishment"
    features = [
        "energy_supply_adeq",
        "gdp_percapita",
        "poverty",
        "max_inflation_shock"
    ]

    required_cols = ["country_name", "year", target] + features
    df = df[required_cols]

    df = df.dropna()

    def compute_country_correlations(country_df):
        if len(country_df) < min_observations:
            return pd.Series({f: np.nan for f in features})

        correlations = {}
        for feature in features:
            # Skip if either variable has zero variance
            if country_df[feature].std() == 0 or country_df[target].std() == 0:
                correlations[feature] = np.nan
            else:
                correlations[feature] = country_df[feature].corr(
                    country_df[target],
                    method="pearson"
                )

        return pd.Series(correlations)
    correlation_df = (
        df
        .groupby("country_name")
        .apply(compute_country_correlations)
        .reset_index()
    )

    output_path = PROCESSED_DIR / output_file
    correlation_df.to_csv(output_path, index=False)

    print(f"Country-level correlation file saved at: {output_path}")



#     # Visualize the Forecast

#     plt.figure(figsize=(10, 6))
#     plt.scatter(y_test, y_pred, alpha=0.5, color='green', label='2020-2024 Data')
#     plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2, label='Perfect Forecast')
#     plt.xlabel('Actual Undernourishment (%)')
#     plt.ylabel('Predicted Undernourishment (%)')
#     plt.title(f'Forecasting Accuracy: Testing on Years {cutoff_year}+')
#     plt.legend()
#     plt.show()
