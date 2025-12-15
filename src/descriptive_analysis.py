import pandas as pd
import numpy as np

from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


# ==========================================
# 1. DATA PREPARATION & SEPARATION
# ==========================================
def undernourishment_analysis():
    input_path = PROCESSED_DIR / "undernourishment_clean.csv"

df = pd.read_csv(PROCESSED_DIR / "undernourishment_clean.csv")
# df = df.drop(columns=['2023', '2024'], errors='ignore')
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# ==========================================
# 1. SIX COLORS PALETTE
# ==========================================
C_BLACK_TEAL = '#001219' 
C_TEAL       = '#0a9396'
C_MINT       = '#94d2bd'
C_ORANGE     = '#ee9b00'
C_BRONZE     = '#ca6702'
C_GREEN      = '#6a994e'

# ==========================================
# 2. DATA PREPARATION
# ==========================================

df = df.drop(columns=['2023', '2024'], errors='ignore')

# Separate Countries and Regions
aggregate_codes = [
    'AFE', 'AFW', 'ARB', 'CEB', 'CSS', 'EAP', 'EAR', 'EAS', 'ECA', 'ECS', 'EMU', 'EUU', 'FCS', 'HIC',
    'HPC', 'IBD', 'IBT', 'IDA', 'IDB', 'IDX', 'INX', 'LAC', 'LCN', 'LDC', 'LIC', 'LMC', 'LMY', 'LTE',
    'MEA', 'MIC', 'MNA', 'NAC', 'OED', 'OSS', 'PRE', 'PSS', 'PST', 'SAS', 'SSA', 'SSF', 'SST', 'TEA',
    'TEC', 'TLA', 'TMN', 'TSA', 'TSS', 'UMC', 'WLD'
]
regions_df = df[df['country_code'].isin(aggregate_codes)].copy()
countries_df = df[~df['country_code'].isin(aggregate_codes)].copy()

# ==========================================
# 3. APPLYING THE 6 COLORS TO REGIONS
# ==========================================
# Mapping the 6 colors to the 6 Key Regions
region_color_map = {
    'Sub-Saharan Africa': C_BRONZE,      # #ca6702
    'South Asia': C_ORANGE,              # #ee9b00
    'Middle East & North Africa': C_BLACK_TEAL, # #001219
    'Latin America & Caribbean': C_TEAL, # #0a9396
    'East Asia & Pacific': C_MINT,       # #94d2bd
    'North America': C_GREEN,            # #6a994e
    'Europe & Central Asia': C_TEAL      # Fallback
}

key_regions_map = {
    'SAS': 'South Asia',
    'SSF': 'Sub-Saharan Africa',
    'LCN': 'Latin America & Caribbean',
    'EAS': 'East Asia & Pacific',
    'NAC': 'North America',
    'MEA': 'Middle East & North Africa'
}

# Detailed Country Mapping for Bar Colors
country_region_map = {
    # Sub-Saharan Africa (Bronze)
    'Angola': 'Sub-Saharan Africa', 'Rwanda': 'Sub-Saharan Africa', 'Ethiopia': 'Sub-Saharan Africa',
    'Sierra Leone': 'Sub-Saharan Africa', 'Chad': 'Sub-Saharan Africa', 'Uganda': 'Sub-Saharan Africa',
    'Guinea-Bissau': 'Sub-Saharan Africa', 'Mozambique': 'Sub-Saharan Africa', 'Togo': 'Sub-Saharan Africa',
    # South Asia (Orange)
    'Afghanistan': 'South Asia', 'Pakistan': 'South Asia', 'India': 'South Asia',
    # Middle East (Black/Dark)
    'Yemen, Rep.': 'Middle East & North Africa', 'Syrian Arab Republic': 'Middle East & North Africa',
    'Lebanon': 'Middle East & North Africa', 'Iraq': 'Middle East & North Africa', 'Djibouti': 'Middle East & North Africa',
    # East Asia (Mint)
    'Myanmar': 'East Asia & Pacific', 'Mongolia': 'East Asia & Pacific', "Korea, Dem. People's Rep.": 'East Asia & Pacific',
    # Latin America (Teal)
    'Venezuela, RB': 'Latin America & Caribbean', 'Haiti': 'Latin America & Caribbean', 'Dominica': 'Latin America & Caribbean',
}

# ==========================================
# GRAPH 1: REGIONAL TRENDS
# ==========================================
plot_regions = regions_df[regions_df['country_code'].isin(key_regions_map.keys())].copy()
plot_regions['region_name'] = plot_regions['country_code'].map(key_regions_map)
plot_regions = plot_regions.set_index('region_name')
years = [str(y) for y in range(2001, 2023)]

plt.figure(figsize=(12, 7))
plt.style.use('seaborn-whitegrid')

for region in key_regions_map.values():
    if region in plot_regions.index:
        c = region_color_map.get(region, '#555555')
        # Plotting the lines
        plt.plot(years, plot_regions.loc[region, years], 
                 label=region, color=c, linewidth=3, linestyle='-')

plt.title('Regional Trends in PoU (2001-2022)', fontsize=16, fontweight='bold', pad=20, color=C_BLACK_TEAL)
plt.ylabel('PoU (%)', fontsize=12, fontweight='bold')
plt.xticks(years[::2], rotation=0) 
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', frameon=False)
plt.grid(axis='x', alpha=0.3)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

plt.tight_layout()
plt.show()

# ==========================================
# GRAPH 2: TOP CHANGES
# ==========================================
countries_df[years] = countries_df[years].apply(pd.to_numeric, errors='coerce')
countries_df['change_total'] = countries_df['2022'] - countries_df['2001']

top_improved = countries_df.sort_values('change_total').head(5)
top_deteriorated = countries_df.sort_values('change_total', ascending=False).head(5)
top_combined = pd.concat([top_deteriorated, top_improved]).sort_values('change_total', ascending=True)

# Assign colors from the 6-color palette
top_combined['color'] = top_combined['country_name'].apply(
    lambda x: region_color_map.get(country_region_map.get(x, 'Other'), '#aaaaaa')
)

plt.figure(figsize=(12, 8))
bars = plt.barh(top_combined['country_name'], top_combined['change_total'], color=top_combined['color'])

plt.title('Top Changes in PoU (2001 - 2022)', fontsize=16, fontweight='bold', pad=20, color=C_BLACK_TEAL)
plt.xlabel('Percentage Point Change', fontsize=12, fontweight='bold')
plt.axvline(0, color='black', linewidth=1)

# Add Legend for the 6 Colors
legend_elements = [
    Patch(facecolor=C_BRONZE, label='Sub-Saharan Africa'),
    Patch(facecolor=C_ORANGE, label='South Asia'),
    Patch(facecolor=C_BLACK_TEAL, label='Middle East & N. Africa'),
    Patch(facecolor=C_TEAL, label='Latin America'),
    Patch(facecolor=C_MINT, label='East Asia & Pacific'),
    Patch(facecolor=C_GREEN, label='North America'),
]
plt.legend(handles=legend_elements, loc='lower right', frameon=True)

plt.grid(axis='x', alpha=0.5, linestyle='--')
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.gca().spines['left'].set_visible(False)

plt.tight_layout()
plt.show()