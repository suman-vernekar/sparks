#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
2025 Weather Data Analysis Generator
This script simulates the analysis of 2025 weather data and generates visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime, timedelta

# Set style for better-looking plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def generate_sample_data():
    """Generate sample weather data for 2025"""
    # Create date range for 2025 (just a sample)
    dates = pd.date_range(start='2025-01-01', end='2025-03-31', freq='D')
    
    # Generate realistic weather data
    np.random.seed(42)  # For reproducible results
    
    data = []
    for date in dates:
        month = date.month
        
        # Adjust temperatures based on season
        if month == 1:  # January - Winter
            base_min = 0
            base_max = 10
        elif month == 2:  # February - Late Winter
            base_min = 2
            base_max = 12
        else:  # March - Early Spring
            base_min = 5
            base_max = 16
            
        # Add some randomness
        min_temp = base_min + np.random.normal(0, 2)
        max_temp = base_max + np.random.normal(0, 2)
        avg_temp = (min_temp + max_temp) / 2
        
        # Humidity (lower in warmer months)
        humidity = 80 - (month - 1) * 10 + np.random.normal(0, 5)
        humidity = max(30, min(100, humidity))  # Keep within reasonable bounds
        
        # Wind speed (more variable)
        wind_speed = 10 + np.random.exponential(5)
        wind_speed = min(wind_speed, 30)  # Cap at reasonable maximum
        
        data.append({
            'date': date,
            'min_temp': round(min_temp, 1),
            'max_temp': round(max_temp, 1),
            'avg_temp': round(avg_temp, 1),
            'humidity': round(humidity, 1),
            'wind_speed': round(wind_speed, 1)
        })
    
    return pd.DataFrame(data)

def create_monthly_aggregates(df):
    """Create monthly aggregates for analysis"""
    df['month'] = df['date'].dt.month
    df['month_name'] = df['date'].dt.strftime('%b')
    
    monthly = df.groupby(['month', 'month_name']).agg({
        'min_temp': 'mean',
        'max_temp': 'mean',
        'avg_temp': 'mean',
        'humidity': 'mean',
        'wind_speed': 'mean'
    }).reset_index()
    
    return monthly

def plot_temperature_trends(monthly_data):
    """Plot temperature trends"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = monthly_data['month_name']
    ax.plot(x, monthly_data['min_temp'], marker='o', linewidth=2, label='Min Temperature')
    ax.plot(x, monthly_data['max_temp'], marker='s', linewidth=2, label='Max Temperature')
    ax.plot(x, monthly_data['avg_temp'], marker='^', linewidth=2, label='Avg Temperature')
    
    ax.set_xlabel('Month')
    ax.set_ylabel('Temperature (°C)')
    ax.set_title('2025 Monthly Temperature Trends')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def plot_humidity_wind(monthly_data):
    """Plot humidity and wind speed trends"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Humidity plot
    ax1.plot(monthly_data['month_name'], monthly_data['humidity'], 
             marker='o', linewidth=2, color='blue')
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Humidity (%)')
    ax1.set_title('2025 Monthly Humidity Trends')
    ax1.grid(True, alpha=0.3)
    
    # Wind speed plot
    ax2.bar(monthly_data['month_name'], monthly_data['wind_speed'], 
            color='green', alpha=0.7)
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Wind Speed (km/h)')
    ax2.set_title('2025 Monthly Average Wind Speed')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def generate_insights(monthly_data):
    """Generate key insights from the data"""
    insights = []
    
    # Temperature trend
    temp_change = monthly_data['avg_temp'].iloc[-1] - monthly_data['avg_temp'].iloc[0]
    if temp_change > 0:
        insights.append(f"Temperature increased by {temp_change:.1f}°C from January to March, indicating seasonal warming.")
    
    # Humidity trend
    humidity_change = monthly_data['humidity'].iloc[-1] - monthly_data['humidity'].iloc[0]
    if humidity_change < 0:
        insights.append(f"Humidity decreased by {abs(humidity_change):.1f}% from January to March, showing drier conditions in spring.")
    
    # Wind pattern
    avg_wind = monthly_data['wind_speed'].mean()
    insights.append(f"Average wind speed was {avg_wind:.1f} km/h across the period.")
    
    return insights

def main():
    """Main function to generate the 2025 weather analysis"""
    print("Generating 2025 Weather Data Analysis...")
    
    # Generate sample data
    weather_data = generate_sample_data()
    print(f"Generated data for {len(weather_data)} days")
    
    # Create monthly aggregates
    monthly_data = create_monthly_aggregates(weather_data)
    
    # Generate plots
    temp_fig = plot_temperature_trends(monthly_data)
    hum_wind_fig = plot_humidity_wind(monthly_data)
    
    # Save plots
    temp_fig.savefig('temperature_trends_2025.png', dpi=300, bbox_inches='tight')
    hum_wind_fig.savefig('humidity_wind_2025.png', dpi=300, bbox_inches='tight')
    
    # Generate insights
    insights = generate_insights(monthly_data)
    
    # Print summary
    print("\n=== 2025 WEATHER ANALYSIS SUMMARY ===")
    print(f"Data Period: {weather_data['date'].min().strftime('%Y-%m-%d')} to {weather_data['date'].max().strftime('%Y-%m-%d')}")
    print(f"Total Records: {len(weather_data)}")
    
    print("\nMONTHLY AVERAGES:")
    for _, row in monthly_data.iterrows():
        print(f"{row['month_name']}: Avg Temp = {row['avg_temp']:.1f}°C, Humidity = {row['humidity']:.1f}%, Wind = {row['wind_speed']:.1f} km/h")
    
    print("\nKEY INSIGHTS:")
    for insight in insights:
        print(f"• {insight}")
    
    print("\nCharts saved as 'temperature_trends_2025.png' and 'humidity_wind_2025.png'")
    print("Analysis complete!")

if __name__ == "__main__":
    main()
