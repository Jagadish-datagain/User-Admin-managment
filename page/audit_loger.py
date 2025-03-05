import mysql.connector
from page.db import get_connection

def log_user_login(email, name, role):
    """Logs user login with login time."""
    connection = get_connection()
    if not connection:
        print("❌ Database connection failed.")
        return

    try:
        cursor = connection.cursor()

        # Create table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                login_id INT AUTO_INCREMENT PRIMARY KEY,
                user_email VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                role ENUM('admin', 'user') NOT NULL,
                action ENUM('login', 'logout') NOT NULL,
                login_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                logout_time DATETIME NULL
            )
        """)
        connection.commit()

        # Insert login record
        cursor.execute(
            "INSERT INTO audit_logs (user_email, name, role, action, login_time) VALUES (%s, %s, %s, 'login', NOW())",
            (email, name, role)
        )
        connection.commit()

        cursor.close()
    except mysql.connector.Error as e:
        print(f"❌ Error logging login: {e}")
    finally:
        if connection.is_connected():
            connection.close()


def log_user_logout(email):
    """Updates logout time when user logs out."""
    connection = get_connection()
    if not connection:
        print("❌ Database connection failed.")
        return

    try:
        cursor = connection.cursor()

        # Update the latest login record for the user
        cursor.execute("""
            UPDATE audit_logs 
            SET action = 'logout', logout_time = NOW() 
            WHERE user_email = %s AND action = 'login' 
            ORDER BY login_id DESC LIMIT 1
        """, (email,))
        connection.commit()

        cursor.close()
    except mysql.connector.Error as e:
        print(f"❌ Error logging logout: {e}")
    finally:
        if connection.is_connected():
            connection.close()