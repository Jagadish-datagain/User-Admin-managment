import streamlit as st
from page.db import get_connection

def add_user():
    st.title("Add New User")
    
    # Create form to input user details
    name = st.text_input("Name")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    role = st.selectbox("Role", ["admin", "user"])  # Select role (Admin/User)
    
    submit_button = st.button("Submit")
    
    if submit_button:
        # Validate inputs
        if name and email and password and role:
            # Check email format based on role
            if role == "user" and not email.endswith("@gmail.com"):
                st.error("For 'User' role, the email must end with '@gmail.com'.")
            elif role == "admin" and not email.endswith("@admin"):
                st.error("For 'Admin' role, the email must end with '@admin'.")
            else:
                # Connect to the database
                connection = get_connection()
                
                try:
                    # Check if email already exists for the given role
                    cursor = connection.cursor()
                    check_query = "SELECT * FROM employees WHERE email = %s AND role = %s;"
                    cursor.execute(check_query, (email, role))
                    existing_user = cursor.fetchone()  # Fetch result
                    
                    if existing_user:
                        st.warning(f"The email '{email}' already exists for the role '{role}'. Please use a different email.")
                    else:
                        # Insert user data into the employees table
                        insert_query = "INSERT INTO employees (employee_name, email, role, password) VALUES (%s, %s, %s, %s)"
                        cursor.execute(insert_query, (name, email, role, password))  # Insert data into the table
                        connection.commit()  # Commit changes to the database
                        
                        st.success(f"{name} has been added as {role}.")
                    
                except Exception as e:
                    st.error(f"Error adding user: {e}")
                finally:
                    cursor.close()  # Close cursor and connection
                    connection.close()
        else:
            st.error("Please fill in all fields.")

# Run the function when the page is called
if __name__ == "__main__":
    add_user()