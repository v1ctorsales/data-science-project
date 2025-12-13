import numpy as np
from src.data_cleaner import clean_undernourishment, clean_inflation_rate, clean_energy_supply_adequacy, clean_poverty, clean_population,  clean_food_calories, clean_gdp_percapita, merge_cleaned_data 
from src.regressions import correlation, regression, calculate_correlation_by_country

def main():
    calculate_correlation_by_country()

if __name__ == "__main__":
    main()