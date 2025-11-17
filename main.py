import numpy as np
from src.data_cleaner import clean_undernourishment, clean_consumer_price_index, clean_energy_supply_adequacy, clean_poverty, clean_population,  clean_food_calories, clean_gdp_percapita, merge_cleaned_data 

def main():
    clean_consumer_price_index() 

if __name__ == "__main__":
    main()