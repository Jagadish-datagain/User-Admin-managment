# import streamlit as st
# import pandas as pd
# from page.db import get_connection

# # Function to fetch unique values for a specific column
# def fetch_unique_values(column_name):
#     connection = get_connection()
#     cursor = connection.cursor(dictionary=True)

#     try:
#         # Query to fetch distinct values
#         query = f"SELECT DISTINCT `{column_name}` FROM managed_services WHERE `{column_name}` IS NOT NULL"
#         cursor.execute(query)
#         results = cursor.fetchall()

#         # Extract values as a list
#         return [row[column_name] for row in results]
#     except Exception as e:
#         st.error(f"Error fetching unique values for {column_name}: {e}")
#         return []
#     finally:
#         cursor.close()
#         connection.close()

# # Function to fetch data based on dynamic filters
# def fetch_data_by_filters(email_address=None, OCR=None, payers=None, start_date=None, end_date=None, bill_ref_code=None, track_id=None):
#     connection = get_connection()
#     cursor = connection.cursor(dictionary=True)
    
#     query = "SELECT * FROM managed_services WHERE 1=1"
#     parameters = []
    
#     if email_address:
#         query += " AND `Email Address` = %s"
#         parameters.append(email_address)
#     if OCRs:
#         query += " AND `OCR` IN (" + ",".join(["%s"] * len(OCRs)) + ")"
#         parameters.extend(OCRs)
#     if payers:
#         query += " AND `payer` IN (" + ",".join(["%s"] * len(payers)) + ")"
#         parameters.extend(payers)
#     if start_date and end_date:
#         query += " AND DATE(timestamp) BETWEEN %s AND %s"
#         parameters.append(start_date.strftime('%Y-%m-%d'))
#         parameters.append(end_date.strftime('%Y-%m-%d'))
#     if bill_ref_code:
#         query += " AND `bill ref code` = %s"
#         parameters.append(bill_ref_code)
#     if track_id:
#         query += " AND `track id` = %s"
#         parameters.append(track_id)

#     try:
#         cursor.execute(query, tuple(parameters))
#         return cursor.fetchall()
#     except Exception as e:
#         st.error(f"Error fetching data with filters: {e}")
#         return []
#     finally:
#         cursor.close()
#         connection.close()

# # Export Data Page
# def export_data_page():
#     st.title("Search and Export Data")

#     # email_options = fetch_unique_values("Email Address")
#     # OCR_options = fetch_unique_values("OCR")
#     # payer_options = fetch_unique_values("payer")
#     # bill_ref_code_options = fetch_unique_values("bill ref code")
#     # track_id_options = fetch_unique_values("track id")

#     email_address = st.selectbox("Email Address", options=[""] + fetch_unique_values("Email Address"))
#     OCRs = st.multiselect("OCR", options=fetch_unique_values("OCR"))
#     payers = st.multiselect("Payers", options=fetch_unique_values("payer"))
#     bill_ref_code = st.text_input("Bill Reference Code")
#     track_id = st.text_input("Track ID")

#     start_date = st.date_input("Start Date", value=None)
#     end_date = st.date_input("End Date", value=None)

#     if st.button("Search and Export Data"):
#         if not (email_address or OCR or payers or start_date or end_date or bill_ref_code or track_id):
#             st.warning("Select any one of the sections to filter data.")
#         else:
#             data = fetch_data_by_filters(
#                 email_address=email_address if email_address else None,
#                 OCR=OCR if OCR else None,
#                 payers=payers if payers else None,
#                 start_date=start_date,
#                 end_date=end_date,
#                 bill_ref_code=bill_ref_code if bill_ref_code else None,
#                 track_id=track_id if track_id else None
#             )
#             if data:
#                 df = pd.DataFrame(data)
#                 df.index = range(1, len(df) + 1)  # Set index to start from 1
#                 st.write("Filter Data:", df)
#                 csv = df.to_csv(index=False)
#                 st.download_button(
#                     label="Download Data",
#                     data=csv,
#                     file_name="user_data.csv",
#                     mime="text/csv"
#                 )
#             else:
#                 st.warning("No data found for the selected filters.")

# if __name__ == "__main__":
#     export_data_page()

#______________________________________admin export code__________________________________________________
import streamlit as st
import pandas as pd
from page.db import get_connection

# Function to fetch unique values for a specific column
def fetch_unique_values(column_name):
    connection = get_connection()
    cursor = connection.cursor(dictionary=True)

    try:
        # Query to fetch distinct values
        query = f"SELECT DISTINCT `{column_name}` FROM managed_services WHERE `{column_name}` IS NOT NULL"
        cursor.execute(query)
        results = cursor.fetchall()

        # Extract values as a list
        return [row[column_name] for row in results]
    except Exception as e:
        st.error(f"Error fetching unique values for {column_name}: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

# Function to fetch data based on dynamic filters
def fetch_data_by_filters(email_address=None, OCRs=None, payers=None, start_date=None, end_date=None, bill_ref_code=None, track_id=None, annotation_id=None):
    connection = get_connection()
    cursor = connection.cursor(dictionary=True)

    # Base query
    query = "SELECT * FROM managed_services WHERE 1=1"
    parameters = []

    # Apply filters dynamically
    if email_address:
        query += " AND `Email Address` = %s"
        parameters.append(email_address)
    if OCRs:
        query += " AND `OCR` IN (" + ",".join(["%s"] * len(OCRs)) + ")"
        parameters.extend(OCRs)
    if payers:
        query += " AND `payer` IN (" + ",".join(["%s"] * len(payers)) + ")"
        parameters.extend(payers)
    if start_date and end_date:
        query += " AND DATE(`Timestamp`) BETWEEN %s AND %s"
        parameters.append(start_date.strftime('%Y-%m-%d'))
        parameters.append(end_date.strftime('%Y-%m-%d'))
    if bill_ref_code:
        query += " AND `bill ref code` = %s"
        parameters.append(bill_ref_code)
    if track_id:
        query += " AND `track id` = %s"
        parameters.append(track_id)
    if annotation_id:
        query += " AND `annotation id` = %s"
        parameters.append(annotation_id)

    try:
        # Execute query
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
def export_data_page():
    st.title("Search and Export Data")

    # Filters
    email_address = st.selectbox("Email Address", options=[""] + fetch_unique_values("Email Address"))
    OCRs = st.multiselect("OCR", options=fetch_unique_values("OCR"))
    payers = st.multiselect("Payers", options=fetch_unique_values("payer"))
    bill_ref_code = st.text_input("Bill Reference Code")
    track_id = st.text_input("Track ID")
    annotation_id = st.text_input("Annotation ID")
    start_date = st.date_input("Start Date", value=None)
    end_date = st.date_input("End Date", value=None)

    # Add custom button style
    st.markdown(
        """
        <style>
            div.stButton > button:first-child {
                background-color: yellow !important;
                color: black !important;
                font-weight: bold;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Initialize data
    data = []
    if st.button("Export Data"):
        data = fetch_data_by_filters(
            email_address=email_address if email_address else None, 
            OCRs=OCRs if OCRs else None, 
            payers=payers if payers else None, 
            start_date=start_date, 
            end_date=end_date,
            bill_ref_code=bill_ref_code if bill_ref_code else None,
            track_id=track_id if track_id else None,
            annotation_id=annotation_id if annotation_id else None
        )
        if not (email_address or OCRs or payers or start_date or end_date or bill_ref_code or track_id or annotation_id):
            st.warning("Select any one of the sections to filter data.")
        else:
            # Convert data to a DataFrame
            df = pd.DataFrame(data)

            # Display total count of filtered records
            count_records = len(df)

            # Reset index to start from 1 instead of 0
            df.index = range(1, count_records + 1)

            # Show the updated table
            st.write("### Filter Data")
            st.dataframe(df)

            # Export the filtered data as CSV
            csv = df.to_csv(index=True)  # Keep the new index in the CSV
            st.download_button(
                label="Download Data",
                data=csv,
                file_name="admin_export_Data.csv",
                mime="text/csv"
            )
            # Display total count of filtered records
            total_records = len(df)
            st.success(f"Total Records Found: {total_records}")

if __name__ == "__main__":
    export_data_page()
