import streamlit as st
from page.db import get_connection
from page.audit_loger import log_user_login  # Only import log_user_login

# Function to apply background image at the top-left corner with a smaller size
def add_bg_from_url():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("https://datagainservices.com/wp-content/uploads/2024/01/datagain-logo.png");
            background-size: 150px; /* Adjust size as needed */
            background-repeat: no-repeat;
            background-position: top left;
            padding-top: 50px; /* Space to prevent content overlap */
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Apply the background
add_bg_from_url()

# Apply custom CSS for background color and button color
st.markdown(
    """
    <style> 
        body {
            background-color: white;
        }
        .stButton>button {
            background-color: yellow !important;
            color: black;
            font-weight: bold;
        }
        .title {
            font-size: 2em;
            font-weight: bold;
        }
        .title .data {
            color: darkblue;
        }
        .title .gain {
            color: yellow;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# Login function to authenticate user and fetch details
def login(email, password):
    connection = get_connection()
    if connection:
        try:
            query = "SELECT employee_name, email, role,department FROM employees WHERE email = %s AND password = %s;"
            cursor = connection.cursor()
            cursor.execute(query, (email.strip(), password.strip()))
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            if result:
                name, email, role = result  # Unpack all three values correctly

                st.session_state.username = name
                st.session_state.email = email
                st.session_state.role = role

                # ✅ Fix: Pass all required arguments correctly
                log_user_login(email, name, role,department)

                return role 
            else:
                return None
        except Exception as e:
            st.error(f"Error during login: {e}")
            return None
    return None

# Initialize session state if not already done
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "role" not in st.session_state:
    st.session_state.role = None
if "department" not in st.session_state:
    st.session_state.department = None
if "username" not in st.session_state:
    st.session_state.username = None
if "email" not in st.session_state:
    st.session_state.email = None
if "page" not in st.session_state:
    st.session_state.page = "Login"

# Handle page navigation
if st.session_state.page == "Login":
    st.markdown('<h1 class="title"><span class="data">Data</span><span class="gain">gain</span></h1>', unsafe_allow_html=True)

    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    login_button = st.button("Login")

    if login_button:
        role = login(email, password)
        if role == "admin":
            st.success(f"Logged in as Admin: {st.session_state.username}")
            st.session_state.logged_in = True
            st.session_state.page = "Admin"
        elif role == "user" and department=="manage_services":
            st.success(f"Logged in as User: {st.session_state.username}")
            st.session_state.logged_in = True
            st.session_state.page = "user"
        else:
            st.error("Invalid credentials. Please try again.")

# Admin/User page redirection
elif st.session_state.page == "Admin" and st.session_state.role == "admin":
    import page.admin
    page.admin.admin_page()

elif st.session_state.page == "User" and st.session_state.role == "user":
    import page.user
    page.user.user_page()
