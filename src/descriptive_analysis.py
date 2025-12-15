import pandas as pd
import numpy as np

from pathlib import Path
import seaborn as sns

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


# ==========================================
# 1. DATA PREPARATION & SEPARATION
# ==========================================
def undernourishment_analysis():
    input_path = PROCESSED_DIR / "undernourishment_clean.csv"

df = pd.read_csv(PROCESSED_DIR / "undernourishment_clean.csv")
regions = pd.read_excel(
    BASE_DIR / "data" / "auxiliary" / "crosswalk_regions.xlsx",
    sheet_name="list"  # Replace "Sheet1" with the actual name of your tab
)

# 1. Perform an OUTER join first with indicator=True to see everything
merge_control = pd.merge(
    regions,
    df,
    left_on='Code',
    right_on='country_code',
    how='outer',      # Keep all rows initially to count them
    indicator=True    # Adds the '_merge' column
)

print("--- Merge Control Statistics ---")
print(merge_control['_merge'].value_counts())

df_final = merge_control[merge_control['_merge'] == 'both'].copy()

df_final = df_final.drop(columns=['_merge'])

print(f"\nFinal dataset size: {len(df_final)} observations")

# ==========================================
# 1. SIX COLORS PALETTE
# ==========================================
C_BLACK_TEAL = '#005f73' 
C_TEAL       = '#0a9396'
C_MINT       = '#94d2bd'
C_ORANGE     = '#ee9b00'
C_BRONZE     = '#ca6702'
C_GREEN      = '#6a994e'
C_GREEN_D    = '#386641'


# --- DATA SHAPE ADAPTATION ---
# 1. Identify Year Columns (2001 to 2024 based on your snippet)
years = [str(y) for y in range(2001, 2023)] # Using up to 2022 as 2023/24 often NaN

# 2. Clean numeric data
df_final[years] = df_final[years].apply(pd.to_numeric, errors='coerce')

# 3. Handle the 'Region' column
if 'Region' not in df_final.columns:
    print("Warning: 'Region' column not found. Attempting to map manually for testing...")
else:
    print("Successfully found 'Region' column.")


# ==========================================
# 3. COLOR 
# ==========================================
region_color_map = {
    'Sub-Saharan Africa': C_BRONZE,
    'South Asia': C_ORANGE,
    'Middle East & North Africa': C_BLACK_TEAL, 
    'Middle East, North Africa, Afghanistan & Pakistan': C_BLACK_TEAL, # Handling your snippet's specific name
    'Latin America & Caribbean': C_TEAL,
    'East Asia & Pacific': C_MINT,
    'Europe & Central Asia': C_GREEN,
    'North America': C_GREEN_D
}

df_final['color'] = df_final['Region'].map(region_color_map).fillna('#aaaaaa')

# ==========================================
# GRAPH 1: REGIONAL TRENDS 
# ==========================================

region_trends = df_final.groupby('Region')[years].mean().T

plt.figure(figsize=(12, 7))
plt.style.use('seaborn-whitegrid')

# Plot each region present in the data
for region_name in region_trends.columns:
    c = region_color_map.get(region_name, '#555555')
    
    label_name = region_name
    if "Middle East" in region_name: label_name = "Middle East & N. Africa"
    
    # Line Style
    linestyle = '-'
    if region_name == 'North America': linestyle = ':'
    
    plt.plot(region_trends.index, region_trends[region_name], 
             label=label_name, color=c, linewidth=3, linestyle=linestyle)

plt.title('Regional Trends in Undernourishment (2001-2022)', fontsize=16, fontweight='bold', pad=20, color=C_BLACK_TEAL)
plt.ylabel('Average PoU (%)', fontsize=12, fontweight='bold')
plt.xticks(years[::2], rotation=0) 
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', frameon=False)
plt.grid(axis='x', alpha=0.3)
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)

plt.tight_layout()
plt.show()

# ==========================================
# GRAPH 2: TOP CHANGES (Bar Chart)
# ==========================================

df_final['change_total'] = df_final['2022'] - df_final['2001']

# Get Top 5 Deteriorated and Top 5 Improved
top_deteriorated = df_final.sort_values('change_total', ascending=False).head(5)
top_improved = df_final.sort_values('change_total', ascending=True).head(5)
top_combined = pd.concat([top_deteriorated, top_improved]).sort_values('change_total', ascending=True)

plt.figure(figsize=(12, 8))

# Plot Bars
bars = plt.barh(top_combined['Economy'], top_combined['change_total'], color=top_combined['color'])

plt.title('Top Changes in PoU (2001 - 2022)', fontsize=16, fontweight='bold', pad=20, color=C_BLACK_TEAL)
plt.xlabel('Percentage Point Change', fontsize=12, fontweight='bold')
plt.axvline(0, color='black', linewidth=1)

# Custom Legend for the 7 Regions
legend_elements = [
    Patch(facecolor=C_BRONZE, label='Sub-Saharan Africa'),
    Patch(facecolor=C_ORANGE, label='South Asia'),
    Patch(facecolor=C_BLACK_TEAL, label='Middle East & N. Africa'),
    Patch(facecolor=C_TEAL, label='Latin America & Caribbean'),
    Patch(facecolor=C_MINT, label='East Asia & Pacific'),
    Patch(facecolor=C_GREEN, label='Europe & Central Asia'),
    Patch(facecolor=C_GREEN_D, label='North America'),
]
plt.legend(handles=legend_elements, loc='lower right', frameon=True)

plt.grid(axis='x', alpha=0.5, linestyle='--')
plt.gca().spines['top'].set_visible(False)
plt.gca().spines['right'].set_visible(False)
plt.gca().spines['left'].set_visible(False)

plt.tight_layout()
plt.show()


