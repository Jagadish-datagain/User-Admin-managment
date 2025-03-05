import streamlit as st
import page.export_data as export_data  # Import the export_data.py page

def user_page():
    # Ensure username and email are fetched from session state
    username = st.session_state.get("username", "Not Available")
    email = st.session_state.get("email", "Not Available")

    # Sidebar profile section
    st.sidebar.title("User Profile")
    st.sidebar.info(f"""
    **Name:** {username}  
    **Email:** {email}
    """)

    # Sidebar options for "Export Data" and "Logout"
    option = st.sidebar.radio("Select Option", ["Export Data", "Logout"], key="user_options")

    # Handle "Export Data" selection
    if option == "Export Data":
        # Navigate to export data page
        export_data.export_data_page()
    
    # Handle "Logout" selection
    # Logout button at the end of navigation
    if st.sidebar.button("Logout", key="logout_btn"):
        if st.session_state.get("email"):
            log_user_logout(st.session_state.email)  # Log the logout event
        
        st.session_state.clear()  # Clear all session data
        st.success("Logged out successfully!")
        st.stop()  # Terminate execution to enforce logout


if __name__ == "__main__":
    user_page()  # Call the user page function once
