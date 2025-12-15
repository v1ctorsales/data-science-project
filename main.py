import numpy as np
from src.data_cleaner import clean_undernourishment, clean_inflation_rate, clean_energy_supply_adequacy, clean_poverty, clean_population,  clean_food_calories, clean_gdp_percapita, merge_cleaned_data 
from src.descriptive_analysis import undernourishment_analysis, get_country_color, get_top_performers
from src.regressions import correlation, regression, calculate_correlation_by_country

def main():
    undernourishment_analysis()

if __name__ == "__main__":
    main()