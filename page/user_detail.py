import streamlit as st
import pandas as pd
from page.db import get_connection

def user_details():
    # Fetch data from the database
    connection = get_connection()
    
    try:
        query = "SELECT employee_name, email, role FROM employees WHERE role='user';"
        df = pd.read_sql(query, connection)  # Load data into a pandas DataFrame
        
        if not df.empty:
            df.index = range(1, len(df) + 1)  # Set index to start from 1
            
            st.title("User Details")
            st.subheader("List of all Employees")
            st.dataframe(df)  # Display the employee data in table format
        else:
            st.warning("No data found in the employees table.")
    except Exception as e:
        st.error(f"Error loading data: {e}")
    finally:
        connection.close()  # Close the database connection

# Run the function when the page is called
if __name__ == "__main__":
    user_details()