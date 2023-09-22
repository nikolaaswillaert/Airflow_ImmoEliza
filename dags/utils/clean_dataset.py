import pandas as pd
import numpy as np
from pathlib import Path


def analyze_column(df, column_name):
    column = df[column_name]
    num_nan = column.isnull().sum()
    num_unique = column.nunique()
    unique_values = column.unique()
    percentage_nan = (num_nan / len(column)) * 100
    percentage_unique = (num_unique / len(column)) * 100
    print(f"Analysis for column '{column_name}':")
    print(f"Number of NaN values: {num_nan}")
    print(f"Number of unique values: {num_unique}")
    print(f"Percentage of NaN values: {percentage_nan:.2f}%")
    print(f"Percentage of unique values: {percentage_unique:.2f}%")
    print(f"Unique values: {unique_values}")

def clean_dataset(ds):
    df = pd.read_csv(f'data_output/history/dataframe_{ds}.csv')
    #REMOVE ALL THE EMPTY LINES
    rows_to_remove = []
    for index, row in df.iterrows():
        if row.isnull().all():
            rows_to_remove.append(index)
    # dropping empty rows
    df_clean = df.drop(rows_to_remove)
    # dropping duplicates (if any)
    df_clean = df_clean.drop_duplicates()

    # Locality
    # !!! CONVERT THE CITIES INTO COORDINATES

    # Type of property (House/apartment)
    # Apartment group and House group == NO PRICE

    # Subtype of property (Bungalow, Chalet, Mansion, ...)

    # Price

    # Type of sale (Exclusion of life sales

    # Open fire (Yes/No)
    df_clean['fireplace'] = df_clean['fireplace'].replace(-1, 0)
    df_clean['fireplace'] = df_clean['fireplace'].replace(np.nan, 0)
    df_clean['fireplace'] = df_clean['fireplace'].astype(int)

    # NUMBER OF ROOMS (nan values set to 0)
    # NUMBER OF ROOMS (nan values set to 0)
    df_clean['number_rooms'] = df_clean['number_rooms'].replace('nan', np.nan).fillna(0)
    analyze_column(df_clean, 'number_rooms')
    
    # Living Area
    df_clean['living_area'] = df_clean['living_area'].replace('nan', np.nan).fillna(df_clean['living_area'].mean())
    analyze_column(df, 'living_area')

    # Fully equipped kitchen (Yes/No)
    df_clean['kitchen'] = df_clean['kitchen'].replace('0', 'NOT_INSTALLED')
    df_clean['kitchen'] = df_clean['kitchen'].replace(np.nan, 'NOT_INSTALLED')

    # Furnished (Yes/No)
    df_clean['furnished'] = df_clean['furnished'].replace(np.nan, False)
    df_clean['furnished'] = df_clean['furnished'].replace(False, 0)
    df_clean['furnished'] = df_clean['furnished'].replace(True, 1)

    # Terrace (Yes/No)
    df_clean['terrace'] = df_clean['terrace'].fillna(0)
    df_clean['terrace'] = df_clean['terrace'].replace(True, 1)
    # If yes: Area
    df_clean['terrace_area'] = df_clean['terrace_area'].replace(np.nan, 0)
    # Garden (Yes/No)
    df_clean['garden'] = df_clean['garden'].fillna(False)
    df_clean['garden'] = df_clean['garden'].replace(True, 1)
    df_clean['garden'] = df_clean['garden'].replace(False, 0)
    # If yes: Area
    df_clean['garden_area'] = df_clean['garden_area'].fillna(0)

    # Surface of the land
    df_clean['surface_land'] = df_clean['surface_land'].replace('UNKNOWN', -1)

    # Surface area of the plot of land
    
    # Number of facades
    df_clean['number_facades'] = df_clean['number_facades'].replace(np.nan, '1')
    df_clean['number_facades'] = df_clean['number_facades'].replace('UNKNOWN', '1')
    df_clean['number_facades'] = df_clean['number_facades'].astype(int)

    # Swimming pool (Yes/No)
    df_clean['swimming_pool'] = df_clean['swimming_pool'].replace(np.nan, 0)
    df_clean['swimming_pool'] = df_clean['swimming_pool'].replace(True, 1)
    df_clean['swimming_pool'] = df_clean['swimming_pool'].replace(False, 0)

    # State of the building (New, to be renovated, ...)
    df_clean['building_state'] = df_clean['building_state'].replace("UNKNOWN", np.nan)

    print(df_clean)

    cwd = Path.cwd()
    csv_path = f'data_output/history/cleaned_dataframe_{ds}.csv'
    csv_path = (cwd / csv_path).resolve()

    df_clean.to_csv(csv_path)

    # return df_clean