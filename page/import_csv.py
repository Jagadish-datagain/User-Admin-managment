import streamlit as st
import pandas as pd
from io import BytesIO
from page.db import get_connection

# Required Columns for Validation
REQUIRED_COLUMNS = [
    "Timestamp", "Email Address", "OCR", "Bill Ref Code", "Track ID", "Annotation ID", "Document Type",
    "Matched Payee ID", "Payer", "Review", "Bill Lines", "Payee Name", "Invoice", "Invoice Date",
    "Invoice Due Date", "Terms", "PO", "Tax Amt", "Total Amt", "Currency", "Foreign Language",
    "Quality Analyst", "Invoice Pages", "Multiple Payees", "Comments", "PST to IST", "US Date",
    "IND Date", "IND Time", "Team", "Agent", "Unique ID", "Priority", "Month", "Hour", "EMEA"
]

TABLE_NAME = "managed_services"  # All records will go into this table

# Column Data Type Mapping
COLUMN_DATA_TYPES = {
    "Timestamp": "datetime64",
    "Email Address": "string",
    "OCR": "string",
    "Bill Ref Code": "string",
    "Track ID": "string",
    "Annotation ID": "string",
    "Document Type": "string",
    "Matched Payee ID": "string",
    "Payer": "string",
    "Review": "string",
    "Bill Lines": "Int64",
    "Payee Name": "string",
    "Invoice": "Int64",
    "Invoice Date": "string",
    "Invoice Due Date": "string",
    "Terms": "string",
    "PO": "string",
    "Tax Amt": "float64",
    "Total Amt": "float64",
    "Currency": "string",
    "Foreign Language": "string",
    "Quality Analyst": "string",
    "Invoice Pages": "Int64",
    "Multiple Payees": "string",
    "Comments": "string",
    "PST to IST": "string",
    "US Date": "datetime64",
    "IND Date": "datetime64",
    "IND Time": "string",
    "Team": "string",
    "Agent": "string",
    "Unique ID": "string",
    "Priority": "string",
    "Month": "string",
    "Hour": "Int64",
    "EMEA": "string"
}

# Generate a CSV Template
def generate_csv_template():
    df_template = pd.DataFrame(columns=REQUIRED_COLUMNS)
    buffer = BytesIO()
    df_template.to_csv(buffer, index=False, encoding="utf-8-sig")
    buffer.seek(0)
    return buffer

# Clean Column Names
def clean_column_names(columns):
    return [col.strip().replace("\xa0", " ") for col in columns]  # Removes extra spaces & non-breaking spaces

# Check if the Table Exists
def table_exists(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None

# Convert Data Types Based on Mapping
# Convert Data Types Based on Mapping
def enforce_data_types(df):
    for column, dtype in COLUMN_DATA_TYPES.items():
        if column in df.columns:
            try:
                if dtype.startswith("datetime64"):
                    df[column] = pd.to_datetime(df[column], errors="coerce")
                    
                    # Convert datetime to string (MySQL expects 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS')
                    df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                elif dtype == "float64":
                    df[column] = pd.to_numeric(df[column], errors="coerce")
                
                elif dtype == "Int64":
                    df[column] = pd.to_numeric(df[column], errors="coerce", downcast="integer")
                
                else:
                    df[column] = df[column].astype(str).str.strip()
            
            except Exception as e:
                st.warning(f"Could not convert column {column} to {dtype}: {e}")
    
    return df


# Insert Unique Data into `managed_services`
def insert_unique_data(connection, df):
    df.columns = clean_column_names(df.columns)
    primary_key_col = "Unique ID"

    if primary_key_col not in df.columns:
        st.error(f"Missing primary key column: {primary_key_col}")
        return

    df[primary_key_col] = df[primary_key_col].astype(str).str.strip().str.lower()
    df.drop_duplicates(subset=[primary_key_col], inplace=True)

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT `{primary_key_col}` FROM `{TABLE_NAME}`")
        existing_records = {str(row[0]).strip().lower() for row in cursor.fetchall()}

    new_records = df[~df[primary_key_col].isin(existing_records)]

    if new_records.empty:
        st.warning("No new unique records found.")
        return

    insert_data_in_batches(connection, new_records)
    st.success(f"{len(new_records)} unique records uploaded successfully.")

# Insert Data in Batches
def insert_data_in_batches(connection, df, batch_size=2000):
    insert_query = f"INSERT INTO `{TABLE_NAME}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    
    with connection.cursor() as cursor:
        for i in range(0, len(df), batch_size):
            batch_data = [tuple(row.where(pd.notna(row), None)) for _, row in df.iloc[i:i + batch_size].iterrows()]
            cursor.executemany(insert_query, batch_data)
            connection.commit()

# Import CSV Page
def import_csv_page():
    st.title("Import Page")

    # Provide CSV Template Download
    st.download_button(
        label="Download CSV Template",
        data=generate_csv_template(),
        file_name="managed_services.csv",
        mime="text/csv"
    )

    uploaded_file = st.file_uploader("Choose a file to upload", type=["csv", "xlsx", "json"])

    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file, dtype=str, low_memory=False)  # Load everything as string
            df.fillna("", inplace=True)  # Replace NaN values with empty strings
            df.columns = clean_column_names(df.columns)
            df.index = range(1, len(df) + 1)

            missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
            if missing_columns:
                st.error(f"Invalid file: Missing required columns - {', '.join(missing_columns)}")
                return

            df = enforce_data_types(df)  # Apply Data Type Conversions

            st.write(f"Total records: {len(df)}")
            st.success("File loaded successfully!")
            st.dataframe(df)

            if st.button("Upload Data"):
                connection = get_connection()
                if connection:
                    if table_exists(connection, TABLE_NAME):
                        insert_unique_data(connection, df)
                    else:
                        st.error(f"Table `{TABLE_NAME}` does not exist. Please create it manually in the database.")
                    connection.close()
                else:
                    st.error("Could not connect to the database.")

        except Exception as e:
            st.error(f"Error loading file: {e}")
    else:
        st.warning("Please upload a file.")

if __name__ == "__main__":
    import_csv_page()

# import streamlit as st
# import pandas as pd
# from io import BytesIO
# from page.db import get_connection

# # Predefined required columns
# REQUIRED_COLUMNS = [
#     "Timestamp", "Email Address", "OCR", "Bill Ref Code", "Track ID", "Annotation ID", "Document Type",
#     "Matched Payee ID", "Payer", "Review", "Bill Lines", "Payee Name", "Invoice", "Invoice Date",
#     "Invoice Due Date", "Terms", "PO", "Tax Amt", "Total Amt", "Currency", "Foreign Language",
#     "Quality Analyst", "Invoice Pages", "Multiple Payees", "Comments", "PST to IST", "US Date",
#     "IND Date", "IND Time", "Team", "Agent", "Unique ID", "Priority", "Month", "Hour", "EMEA"
# ]

# # Function to generate and download CSV template
# def generate_csv_template():
#     df_template = pd.DataFrame(columns=REQUIRED_COLUMNS)
#     buffer = BytesIO()
#     df_template.to_csv(buffer, index=False, encoding="utf-8-sig")
#     buffer.seek(0)
#     return buffer

# # Function to check if a table exists
# def table_exists(connection, table_name):
#     with connection.cursor() as cursor:
#         cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
#         return cursor.fetchone() is not None

# # Function to clean column names
# def clean_column_names(columns):
#     return [col.strip().replace("\xa0", " ") for col in columns]  # Removes extra spaces & non-breaking spaces

# # Function to create table
# def create_table_from_df(connection, df, table_name):
#     df.columns = clean_column_names(df.columns)
#     columns = ", ".join([f"`{col}` LONGTEXT" for col in df.columns])
#     create_query = f"CREATE TABLE `{table_name}` ({columns})"
    
#     with connection.cursor() as cursor:
#         cursor.execute(create_query)
#         connection.commit()
#     insert_data_in_batches(connection, df, table_name)
#     st.success("Data uploaded successfully.")

# # Function to insert unique data
# def insert_unique_data(connection, df, table_name):
#     df.columns = clean_column_names(df.columns)
#     primary_key_col = "Unique ID"
    
#     if primary_key_col not in df.columns:
#         st.error(f"Missing primary key column: {primary_key_col}")
#         return

#     df[primary_key_col] = df[primary_key_col].astype(str).str.strip().str.lower()
#     df.drop_duplicates(subset=[primary_key_col], inplace=True)

#     with connection.cursor() as cursor:
#         cursor.execute(f"SELECT `{primary_key_col}` FROM `{table_name}`")
#         existing_records = {str(row[0]).strip().lower() for row in cursor.fetchall()}

#     new_records = df[~df[primary_key_col].isin(existing_records)]
    
#     if new_records.empty:
#         st.warning("Records not inserted.")
#         return

#     insert_data_in_batches(connection, new_records, table_name)
#     st.success("Unique data uploaded.")

# # Function to insert data in batches
# def insert_data_in_batches(connection, df, table_name, batch_size=2000):
#     insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    
#     with connection.cursor() as cursor:
#         for i in range(0, len(df), batch_size):
#             batch_data = [tuple(row.where(pd.notna(row), None)) for _, row in df.iloc[i:i + batch_size].iterrows()]
#             cursor.executemany(insert_query, batch_data)
#             connection.commit()

# # Import CSV Page
# def import_csv_page():
#     st.title("Import Page")
    
#     # Provide CSV download button
#     st.download_button(
#         label="Download CSV Template",
#         data=generate_csv_template(),
#         file_name="managed_services.csv",
#         mime="text/csv"
#     )
    
#     uploaded_file = st.file_uploader("Choose a file to upload", type=["csv", "xlsx", "json"])
    
#     if uploaded_file is not None:
#         try:
#             df = pd.read_csv(uploaded_file, dtype=str, low_memory=False)  # Load everything as string
#             df.fillna("", inplace=True)  # Replace NaN values with empty strings
#             df.columns = clean_column_names(df.columns)
#             df.index = range(1, len(df) + 1)
            
#             missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
#             if missing_columns:
#                 st.error(f"Invalid file: Missing required columns - {', '.join(missing_columns)}")
#                 return
#             st.write(f"Total records : {len(df)}")  
#             st.success("File loaded successfully!")
#             st.dataframe(df)
            
#             if st.button("Upload Data"):
#                 connection = get_connection()
#                 if connection:
#                     table_name = uploaded_file.name.split('.')[0]
#                     if table_exists(connection, table_name):
#                         insert_unique_data(connection, df, table_name)
#                     else:
#                         create_table_from_df(connection, df, table_name)
#                     connection.close()
#                 else:
#                     st.error("Could not connect to the database.")
#         except Exception as e:
#             st.error(f"Error loading : {e}")
#     else:
#         st.warning("Please upload a file.")

# if __name__ == "__main__":
#     import_csv_page()
