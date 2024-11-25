# Import python packages
import streamlit as st
import requests
from snowflake.snowpark.functions import col

# Write directly to the app
st.title("💬 Chat Messaging App")

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch users from the Snowflake database
def fetch_users():
    query = "SELECT USERNAME, DISPLAY_NAME FROM MSG_APP.PUBLIC.USERS"
    users_df = session.sql(query).to_pandas()
    return users_df

# Fetch the users data
users_df = fetch_users()

# Login section
st.sidebar.header("Login")
username = st.sidebar.text_input("Username:")
user_display_name = None

# Check login
if st.sidebar.button("Login"):
    if username in users_df["USERNAME"].values:
        user_display_name = users_df[users_df["USERNAME"] == username]["DISPLAY_NAME"].values[0]
        st.sidebar.success(f"Welcome, {user_display_name}!")
    else:
        st.sidebar.error("User not found!")

# Messaging section (only visible after login)
if user_display_name:
    st.subheader("Send a Message")
    
    # Show a dropdown for selecting a recipient (exclude current user)
    recipient = st.selectbox("Select Recipient", users_df[users_df["USERNAME"] != username]["DISPLAY_NAME"])
    
    message_text = st.text_area("Enter your message:")

    # Send message
    if st.button("Send"):
        if message_text.strip():
            # Log message contents before sending
            st.write(f"Sending message to {recipient}: {message_text}")
            
            # Get recipient's username
            recipient_username = users_df[users_df["DISPLAY_NAME"] == recipient]["USERNAME"].values[0]
            
            # Construct the SQL query to insert the message using usernames (not IDs)
            query = f"""
            INSERT INTO MSG_APP.PUBLIC.MESSAGES (SENDER_ID, RECEIVER_ID, MESSAGE_TEXT, MESSAGE_TIMESTAMP)
            SELECT SENDER.USER_ID, RECEIVER.USER_ID, '{message_text.replace("'", "''")}', CURRENT_TIMESTAMP
            FROM MSG_APP.PUBLIC.USERS SENDER
            JOIN MSG_APP.PUBLIC.USERS RECEIVER
            ON SENDER.USERNAME = '{username}' AND RECEIVER.USERNAME = '{recipient_username}'
            """
            
            try:
                # Execute the query to insert the message
                session.sql(query).collect()  # Execute the query to insert the message
                st.success("Message sent!")
            except Exception as e:
                st.error(f"Error sending message: {e}")
        else:
            st.error("Message cannot be empty!")

    # View conversation
    st.subheader("Conversation")
    
    # Query to fetch the conversation for the logged-in user
    query = f"""
    SELECT 
        U1.DISPLAY_NAME AS SENDER,
        U2.DISPLAY_NAME AS RECEIVER,
        M.MESSAGE_TEXT,
        M.MESSAGE_TIMESTAMP
    FROM MSG_APP.PUBLIC.MESSAGES M
    JOIN MSG_APP.PUBLIC.USERS U1 ON M.SENDER_ID = U1.USER_ID
    JOIN MSG_APP.PUBLIC.USERS U2 ON M.RECEIVER_ID = U2.USER_ID
    WHERE (U1.USERNAME = '{username}' OR U2.USERNAME = '{username}')
    ORDER BY M.MESSAGE_TIMESTAMP DESC
    """
    
    # Fetch conversation from the database
    try:
        conversation_df = session.sql(query).to_pandas()
        if conversation_df.empty:
            st.write("No messages yet. Start the conversation!")
        else:
            st.write(conversation_df)
    except Exception as e:
        st.error(f"Error fetching conversation: {e}")
