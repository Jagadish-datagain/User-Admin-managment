import streamlit as st
import pandas as pd
from page.db import get_connection

# Function to fetch unique values for a specific column
def fetch_unique_values(column_name):
    connection = get_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        query = f"SELECT DISTINCT `{column_name}` FROM implementation_DBR WHERE `{column_name}` IS NOT NULL"
        cursor.execute(query)
        results = cursor.fetchall()
        return [row[column_name] for row in results]
    except Exception as e:
        st.error(f"Error fetching unique values for {column_name}: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

# Function to fetch filtered data
def fetch_data_by_filters(rocketlane_name=None, task=None, start_date=None, end_date=None, unique_data=False):
    connection = get_connection()
    cursor = connection.cursor(dictionary=True)

    query = "SELECT * FROM implementation_DBR WHERE 1=1"
    parameters = []
    
    if start_date and end_date:
        query += " AND DATE(`Timestamp`) BETWEEN %s AND %s"
        parameters.append(start_date.strftime('%Y-%m-%d'))
        parameters.append(end_date.strftime('%Y-%m-%d'))
    if task:
        query += " AND `Task` = %s"
        parameters.append(task)
    if rocketlane_name:
        query += " AND `Rocketlane Name` = %s"
        parameters.append(rocketlane_name)

    try:
        cursor.execute(query, tuple(parameters))
        results = cursor.fetchall()
        return results
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

# Export Data Page
def implementation_page():
    st.title("Search and Export Data")

    # Fetch user details from session
    username = st.session_state.get("username", "Not Available")
    email = st.session_state.get("email", "Not Available")
    department = st.session_state.get("department", "Not Available")

    # Sidebar profile section
    st.sidebar.title("User Profile")
    st.sidebar.info(f"""
    **Name:** {username}  
    **Email:** {email}  
    **Department:** {department}  
    """)

    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.clear()
        st.rerun()

    # Filters
    start_date = st.date_input("Start Date", value=None)
    end_date = st.date_input("End Date", value=None)
    task = st.selectbox("Task", options=[""] + fetch_unique_values("Task"))

    # Selection for unique or all data
    unique_selection = st.radio("Select Data Type:", ("All Data", "Unique Data"))
    unique_data = unique_selection == "Unique Data"

    # Export data button
    st.write("\n")  # Adds some spacing
    if st.button("Export Data"):
        data = fetch_data_by_filters(
            rocketlane_name=None,
            task=task if task else None,
            start_date=start_date,
            end_date=end_date,
            unique_data=unique_data
        )

        if not (task or start_date or end_date):
            st.warning("Select at least one filter to retrieve data.")
        else:
            df = pd.DataFrame(data)

            if unique_data:
                # **Keep only columns that contain at least one non-null, non-empty value**
                df.dropna(axis=1, how='all', inplace=True)  # Drop fully empty columns
                df = df.loc[:, (df != "").any()]  # Drop columns where all values are empty strings

            df.index = range(1, len(df) + 1)

            if df.empty:
                st.warning("No valid data found after applying filters.")
            else:
                st.write("### Filtered Data")
                st.dataframe(df)
                csv = df.to_csv(index=False)
                st.download_button("Download Data", data=csv, file_name="implementation_filtered_export.csv", mime="text/csv")
                st.success(f"Total Records Found: {len(df)}")

    # Rocketlane Name filter
    rocketlane_name = st.selectbox("Rocketlane Name", options=[""] + fetch_unique_values("Rocketlane Name"))

if __name__ == "__main__":
    implementation_page()