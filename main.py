import numpy as np
from src.data_cleaner import clean_undernourishment, clean_consumer_price_index, clean_energy_supply_adequacy, clean_poverty, clean_population, clean_food_calories, merge_cleaned_data

def main():
    merge_cleaned_data()

if __name__ == "__main__":
    main()