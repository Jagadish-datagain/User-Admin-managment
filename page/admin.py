import streamlit as st
from page.audit_loger import log_user_login, log_user_logout

def admin_page():
    """
    Welcome to Admin page 
    """
    # Sidebar title
    st.sidebar.title("Admin Profile")
    
    # Check if username and email exist in session state
    if st.session_state.get("username") and st.session_state.get("email"):
        st.sidebar.info(f"""     
            **Name:** {st.session_state.username}  
            **Email:** {st.session_state.email} 
        """) # it will highlight the Admin/User information
        
        # Log the admin login (only when accessing the page for the first time)
        if not st.session_state.get("logged_in"): 
            log_user_login(st.session_state.email, st.session_state.username, "admin")
            st.session_state.logged_in = True
    else:
        st.sidebar.error("Error: Admin details not found!")
        return

    # Sidebar navigation
    st.sidebar.title("Navigation")
    section = st.sidebar.radio("Go to:", ["Dashboard", "User Details", "Import CSV", "Add User", "Admin Export", "Forecast"])

    # Navigation logic
    if section == "Dashboard":
        try:
            from page.dashboard import admin_dashboard
            admin_dashboard()  # Call the admin_dashboard function
        except ImportError as e:
            st.error(f"Error loading Dashboard: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

    elif section == "User Details":
        try:
            from page.user_detail import user_details
            user_details()  # Call the user_details function
        except ImportError as e:
            st.error(f"Error loading User Details: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

    elif section == "Import CSV":
        try:
            from page.import_csv import import_csv_page
            import_csv_page()  # Call the import_csv_page function
        except ImportError as e:
            st.error(f"Error loading Import CSV: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    
    elif section == "Add User":
        try:
            from page.add_user import add_user
            add_user()
        except ImportError as e:
            st.error(f"Error loading ADD USER: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    elif section == "Admin Export":
        try:
            from page.admin_export import admin_export_page
            admin_export_page()
        except ImportError as e:
            st.error(f"Error loading ADD USER: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    elif section == "Forecast":
        try:
            from page.forecast import forecast_page
            forecast_page()
        except ImportError as e:
            st.error(f"Error loading Forecast: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
    
    # Logout button at the end of navigation
    if st.sidebar.button("Logout", key="logout_btn"):
        if st.session_state.get("email"):
            log_user_logout(st.session_state.email)  # Log the logout event
        
        st.session_state.clear()  # Clear all session data
        st.success("Logged out successfully!")
        st.stop()  # Terminate execution to enforce logout
