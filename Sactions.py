#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  2 11:55:22 2025

@author: m2
"""

import pandas as pd
import numpy as np
import re
import csv

def load_data(file_path):
    import pandas as pd
    try:
        data = pd.read_csv(file_path)
        print(f"Successfully loaded data from {file_path}")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
data = load_data("/Users/m2/Desktop/Malverde/Book1.csv")


# Step 2: Cleanse and transform the data
def cleanse_and_transform_data(df):
    # Remove duplicates based on all columns
    df = df.drop_duplicates(keep='first')
    
    print(df)
    
    name_columns = ["Name 1","Name 2","Name 3","Name 4","Name 5","Name 6"]
    existing_columns = [col for col in name_columns if col in df.columns]
    
    if existing_columns:
        df['Full Name'] = df[existing_columns].apply(lambda x: ' '.join(x.dropna().astype(str).str.strip()), axis=1)
    else:
        print("None of the specified columns exist in the DataFrame.")
        
    # Check if 'DOB' column exists
    if 'DOB' in df.columns:
        # Standardize DOB to DD-MM-YYYY format
        def standardize_dob(dob):
            if pd.isna(dob) or dob.strip() == "":
                return np.nan
            try:
                # Handle cases where date/month is unknown (e.g., 00/00/1962 or 00-00-1962)
                dob = str(dob).replace('-', '/')
                parts = dob.split('/')
                day = parts[0] if parts[0] != "00" else "01"
                month = parts[1] if parts[1] != "00" else "01"
                year = parts[2]
                return f"{day.zfill(2)}-{month.zfill(2)}-{year}"
            except Exception:
                return np.nan
    
        df['DOB'] = df['DOB'].apply(standardize_dob)
    else:
        print("The 'DOB' column does not exist in the DataFrame.")
    
    # Extract and concatenate associated countries
    def extract_countries(row):
        countries = []
        columns_to_check = ['Country of Birth', 'Nationality', 'Country']
        existing_columns = [col for col in columns_to_check if col in df.columns]
        
        for col in existing_columns:
            if pd.notna(row[col]) and row[col].strip():
                # Use regex to extract country names
                matches = re.findall(r'\((\d+)\)\s*([A-Za-z]+)', str(row[col]))
                if matches:
                    for _, country in matches:
                        if country not in countries:
                            countries.append(country)
                else:
                    # Handle single country values without numbering
                    if str(row[col]).strip() not in countries:
                        countries.append(str(row[col]).strip())
        return ", ".join(countries)
    
    df['Associated Countries'] = df.apply(extract_countries, axis=1)
    
    # Concatenate Address fields (Address1 to Address6)
    def concatenate_addresses(row):
        addresses = []
        for i in range(1, 7):
            col = f"Address {i}"
            if col in df.columns and pd.notna(row[col]) and row[col].strip():
                addresses.append(str(row[col]))
        return ", ".join(addresses)
    
    df['Full Address'] = df.apply(concatenate_addresses, axis=1)
    
    # Keep relevant columns
    columns_to_keep = [
            'Full Name', 'DOB', 'Town of Birth', 'Associated Countries',
            'Passport Number', 'National Identification Number', 'Position',
            'Full Address', 'Other Information', 'Group Type', 'Group ID'
        ]
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        print(f"The following columns are missing: {missing_columns}")
    
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df_cleaned = df[existing_columns]
    print(df_cleaned)
    
    return df_cleaned  # Return the cleaned dataframe

# Step 3: Identify and document data quality issues
def assess_data_quality(df): 
    # Initialize empty list inside the function
    quality_issues = []
    
    # Check for missing values - INSIDE the function
    missing_values = df.isnull().sum()
    quality_issues.append("Missing Values:\n" + str(missing_values[missing_values > 0]))
    
    # Check for duplicates - INSIDE the function
    duplicates = df.duplicated().sum()
    quality_issues.append(f"Duplicate Records: {duplicates}")
    
    # Check if 'DOB' column exists before checking its format
    if 'DOB' in df.columns:
        # Check for formatting inconsistencies in DOB - INSIDE the function
        invalid_dobs = df[df['DOB'].apply(lambda x: not re.match(r'\d{2}-\d{2}-\d{4}', str(x)) if pd.notna(x) else False)]
        quality_issues.append(f"Invalid DOB Format: {len(invalid_dobs)} records")
    
    # Return the list
    return quality_issues

# Save the transformed data - separate function
def save_data(df, output_file):
    df.to_csv(output_file, index=False)

# Main function
def main():
    # Input and output file paths
    input_file = "/Users/m2/Desktop/Malverde/Book1.csv"  # Updated to use actual file path
    output_file = "/Users/m2/Desktop/Malverde/transformed_sanctions_data.csv"
    
    # Load the data
    raw_data = load_data(input_file)
    
    if raw_data is not None:
        # Cleanse and transform the data
        cleaned_data = cleanse_and_transform_data(raw_data)
        
        # Assess data quality
        quality_issues = assess_data_quality(cleaned_data)
        
        print("Data Quality Issues:") 
        for issue in quality_issues:
            print(issue)
        
        # Save the transformed data
        save_data(cleaned_data, output_file)
        print(f"Transformed data saved to {output_file}")
    else:
        print("Data loading failed. Cannot proceed.")

# Entry point
if __name__ == "__main__":
    main()