#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Process weather data for HTML visualization
This script processes weather_data.csv and generates data for the HTML page
"""

import pandas as pd
import json

def load_and_process_data():
    """Load and process the weather data"""
    # Load the CSV file
    df = pd.read_csv('weather_data.csv', encoding='utf-8')
    
    # Convert Date/Time to datetime
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    
    # Extract month
    df['Month'] = df['Date/Time'].dt.month
    
    # Convert temperature columns to numeric
    temp_cols = ['Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)']
    for col in temp_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with missing temperature data
    df = df.dropna(subset=temp_cols)
    
    return df

def create_monthly_data(df):
    """Create monthly aggregated data"""
    # Group by month and calculate averages
    monthly = df.groupby('Month').agg({
        'Max Temp (°C)': 'mean',
        'Min Temp (°C)': 'mean',
        'Mean Temp (°C)': 'mean'
    }).round(1).reset_index()
    
    # Add month names
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly['Month_Name'] = [month_names[i-1] for i in monthly['Month']]
    
    return monthly

def generate_insights(df):
    """Generate key insights"""
    insights = {}
    
    # Overall statistics
    insights['avg_temp'] = round(df['Mean Temp (°C)'].mean(), 1)
    insights['max_temp'] = df['Max Temp (°C)'].max()
    insights['min_temp'] = df['Min Temp (°C)'].min()
    
    # Find months with highest and lowest average temperatures
    monthly_avg = df.groupby('Month')['Mean Temp (°C)'].mean()
    hottest_month = monthly_avg.idxmax()
    coldest_month = monthly_avg.idxmin()
    
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    insights['hottest_month'] = month_names[hottest_month-1]
    insights['coldest_month'] = month_names[coldest_month-1]
    
    return insights

def save_data_for_html(monthly_data, insights):
    """Save processed data to JSON for HTML use"""
    # Prepare data for charts
    chart_data = {
        'labels': monthly_data['Month_Name'].tolist(),
        'min_temp': monthly_data['Min Temp (°C)'].tolist(),
        'max_temp': monthly_data['Max Temp (°C)'].tolist(),
        'mean_temp': monthly_data['Mean Temp (°C)'].tolist()
    }
    
    # Combine with insights
    all_data = {
        'chart_data': chart_data,
        'insights': insights
    }
    
    # Save to JSON file
    with open('weather_data.json', 'w') as f:
        json.dump(all_data, f, indent=2)

def main():
    """Main function"""
    print("Processing weather data for HTML visualization...")
    
    # Load and process data
    df = load_and_process_data()
    print(f"Loaded {len(df)} records")
    
    # Create monthly data
    monthly_data = create_monthly_data(df)
    
    # Generate insights
    insights = generate_insights(df)
    
    # Save data for HTML
    save_data_for_html(monthly_data, insights)
    
    print("Data processed and saved to weather_data.json")
    print("Ready for HTML visualization")

if __name__ == "__main__":
    main()