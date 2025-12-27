#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Weather Data Analysis for Toronto 2025
This script analyzes the weather_data.csv file and generates visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_and_clean_data():
    """Load and clean the weather data"""
    # Load the CSV file
    df = pd.read_csv('weather_data.csv', encoding='utf-8')
    
    # Convert Date/Time to datetime
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    
    # Extract month and day of year
    df['Month'] = df['Date/Time'].dt.month
    df['DayOfYear'] = df['Date/Time'].dt.dayofyear
    
    # Convert temperature columns to numeric (they might be strings)
    temp_cols = ['Max Temp (°C)', 'Min Temp (°C)', 'Mean Temp (°C)']
    for col in temp_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with missing temperature data
    df = df.dropna(subset=temp_cols)
    
    return df

def create_monthly_aggregates(df):
    """Create monthly aggregates for analysis"""
    # Group by month and calculate averages
    monthly = df.groupby('Month').agg({
        'Max Temp (°C)': 'mean',
        'Min Temp (°C)': 'mean',
        'Mean Temp (°C)': 'mean'
    }).reset_index()
    
    # Add month names
    monthly['Month_Name'] = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    return monthly

def plot_temperature_trends(monthly_data):
    """Plot temperature trends"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = monthly_data['Month_Name']
    ax.plot(x, monthly_data['Min Temp (°C)'], marker='o', linewidth=2, label='Average Min Temperature')
    ax.plot(x, monthly_data['Max Temp (°C)'], marker='s', linewidth=2, label='Average Max Temperature')
    ax.plot(x, monthly_data['Mean Temp (°C)'], marker='^', linewidth=2, label='Average Mean Temperature')
    
    ax.set_xlabel('Month')
    ax.set_ylabel('Temperature (°C)')
    ax.set_title('2025 Monthly Temperature Trends for Toronto')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def plot_temperature_range(monthly_data):
    """Plot temperature ranges"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = monthly_data['Month_Name']
    ax.bar(x, monthly_data['Max Temp (°C)'] - monthly_data['Min Temp (°C)'], 
           color='orange', alpha=0.7, label='Temperature Range')
    
    ax.set_xlabel('Month')
    ax.set_ylabel('Temperature Range (°C)')
    ax.set_title('2025 Monthly Temperature Ranges for Toronto')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def generate_insights(df, monthly_data):
    """Generate key insights from the data"""
    insights = []
    
    # Overall temperature statistics
    avg_temp = df['Mean Temp (°C)'].mean()
    insights.append(f"Average temperature for 2025: {avg_temp:.1f}°C")
    
    # Hottest and coldest months
    hottest_month_idx = monthly_data['Mean Temp (°C)'].idxmax()
    coldest_month_idx = monthly_data['Mean Temp (°C)'].idxmin()
    
    hottest_month = monthly_data.loc[hottest_month_idx, 'Month_Name']
    coldest_month = monthly_data.loc[coldest_month_idx, 'Month_Name']
    
    insights.append(f"Hottest month: {hottest_month}")
    insights.append(f"Coldest month: {coldest_month}")
    
    # Temperature extremes
    max_temp = df['Max Temp (°C)'].max()
    min_temp = df['Min Temp (°C)'].min()
    
    insights.append(f"Highest temperature recorded: {max_temp:.1f}°C")
    insights.append(f"Lowest temperature recorded: {min_temp:.1f}°C")
    
    return insights

def main():
    """Main function to analyze the weather data"""
    print("Analyzing Toronto 2025 Weather Data...")
    
    # Load and clean data
    weather_data = load_and_clean_data()
    print(f"Loaded data for {len(weather_data)} days")
    
    # Create monthly aggregates
    monthly_data = create_monthly_aggregates(weather_data)
    
    # Generate plots
    temp_fig = plot_temperature_trends(monthly_data)
    range_fig = plot_temperature_range(monthly_data)
    
    # Save plots
    temp_fig.savefig('toronto_temperature_trends_2025.png', dpi=300, bbox_inches='tight')
    range_fig.savefig('toronto_temperature_ranges_2025.png', dpi=300, bbox_inches='tight')
    
    # Generate insights
    insights = generate_insights(weather_data, monthly_data)
    
    # Print summary
    print("\n=== TORONTO 2025 WEATHER ANALYSIS SUMMARY ===")
    for insight in insights:
        print(f"• {insight}")
    
    print("\nCharts saved as 'toronto_temperature_trends_2025.png' and 'toronto_temperature_ranges_2025.png'")
    print("Analysis complete!")

if __name__ == "__main__":
    main()
