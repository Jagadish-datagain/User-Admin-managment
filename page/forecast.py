import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from page.db import get_connection

def fetch_data():
    """Fetch total monthly volume data from the admin table"""
    connection = get_connection()
    query = """
        SELECT 
            DATE_FORMAT(timestamp, '%Y-%m') AS month, 
            COUNT(*) AS volume
        FROM managed_services  
        WHERE timestamp IS NOT NULL
        GROUP BY month
        ORDER BY month;  
    """
    try:
        df = pd.read_sql(query, connection)
        connection.close()

        df['month'] = pd.to_datetime(df['month'], format='%Y-%m')  
        df['volume'] = df['volume'].fillna(0).astype(int)
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

def forecast_monthly_volumes(actual_data, growth_rate=0.05, fluctuation_range=(-0.1, 0.1)):
    """Generate predicted volumes up to December of the current year"""
    if actual_data.empty:
        return pd.Series()
    
    last_value = actual_data.iloc[-1]
    last_month = actual_data.index[-1]
    
    # Generate months till December of the current year
    forecast_months = pd.date_range(start=last_month + pd.DateOffset(months=1),
                                    end=pd.Timestamp.today().replace(month=12, day=1), freq='MS')
    forecast = []
    
    for _ in forecast_months:
        growth_factor = (1 + growth_rate) + np.random.uniform(*fluctuation_range)
        last_value *= growth_factor
        forecast.append(int(round(last_value)))
    
    return pd.Series(forecast, index=forecast_months)

def generate_daily_forecast(monthly_data):
    """Generate daily forecast based on historical monthly volumes"""
    daily_data = []
    for month, volume in monthly_data.items():
        days_in_month = (month + pd.DateOffset(months=1)).replace(day=1) - month
        days_in_month = int(days_in_month.days)
        
        base_daily_volume = max(0, int(round(volume / days_in_month)))
        adjusted_volumes = [max(0, int(round(base_daily_volume * (1 + np.random.uniform(-0.1, 0.1))))) for _ in range(days_in_month)]
        
        scale_factor = volume / sum(adjusted_volumes) if sum(adjusted_volumes) > 0 else 1
        adjusted_volumes = [max(0, int(round(v * scale_factor))) for v in adjusted_volumes]
        
        for day, adjusted_volume in zip(range(1, days_in_month + 1), adjusted_volumes):
            daily_data.append({'Date': pd.to_datetime(month.replace(day=day)), 'Forecast Volume': adjusted_volume})
    
    return pd.DataFrame(daily_data)

def forecast_page():
    st.title("📈 Total Volume Forecasting")
    
    data = fetch_data()
    if data.empty:
        st.warning("⚠ No data available.")
        return
    
    st.subheader("📅 Historical Monthly Data")
    if st.button("Show Monthly Table"):
        data['month_name'] = data['month'].dt.strftime('%B %Y')
        data_display = data[['month_name', 'volume']].copy()
        data_display.index = range(1, len(data_display) + 1)
        st.dataframe(data_display)
    
    actual_series = data.set_index('month')['volume']
    forecast_series = forecast_monthly_volumes(actual_series)
    
    all_months = pd.date_range(start=actual_series.index.min(), end=forecast_series.index.max(), freq='MS')
    full_data = pd.Series(index=all_months, dtype='float64')
    full_data.update(actual_series)
    full_data.update(forecast_series)
    full_data.fillna(0, inplace=True)
    
    full_df = pd.DataFrame({'Month': full_data.index, 'Volume': full_data.values})
    
    st.subheader("📊 Monthly Volume Forecast")
    fig = px.line(full_df, x='Month', y='Volume', markers=True, title="Monthly Volume Forecast")
    fig.update_layout(xaxis=dict(title="Month", tickformat="%b-%Y"), yaxis=dict(range=[0, full_data.max() * 1.1]))
    st.plotly_chart(fig)
    
    st.subheader("📆 Show Daily Forecast Table")
    st.session_state.daily_forecast = generate_daily_forecast(full_data)
    
    months_available = sorted(st.session_state.daily_forecast['Date'].dt.strftime('%B %Y').unique(), key=lambda x: pd.to_datetime(x, format='%B %Y'))
    selected_month = st.selectbox("Select a Month", months_available)
    
    if st.button("Show Daily Table"):
        filtered_data = st.session_state.daily_forecast[st.session_state.daily_forecast['Date'].dt.strftime('%B %Y') == selected_month].copy()
        filtered_data['Date'] = filtered_data['Date'].dt.strftime('%Y-%m-%d')
        filtered_data.index += 1
        
        st.dataframe(filtered_data)
        
        st.subheader(f"📊 Daily Forecast Chart for {selected_month}")
        daily_fig = px.line(filtered_data, x='Date', y='Forecast Volume', markers=True, title=f"Daily Forecast for {selected_month}")
        daily_fig.update_layout(xaxis_title="Date", yaxis_title="Forecast Volume", xaxis=dict(tickangle=-45), yaxis=dict(range=[0, filtered_data['Forecast Volume'].max() * 1.1]))
        st.plotly_chart(daily_fig)

if __name__ == "__main__":
    forecast_page()