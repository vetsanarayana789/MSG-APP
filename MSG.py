import streamlit as st
import pandas as pd
import requests

from snowflake.snowpark.functions import col

# Snowflake connection setup
st.title("💬 Chat Messaging App")

# Assuming the connection is defined as 'snowflake' in Streamlit
cnx = st.connection("snowflake")  # Assuming 'snowflake' is a predefined connection in Streamlit
session = cnx.session()

# Fetch user data from the Snowflake session
def fetch_users():
    query = "SELECT USERNAME, DISPLAY_NAME FROM MSG_APP.PUBLIC.USERS"
    users_df = session.sql(query).to_pandas()
    return users_df

users_df = fetch_users()

# Login section
st.sidebar.header("Login")

# Use session state to track user state
if 'username' not in st.session_state:
    st.session_state.username = None
    st.session_state.user_display_name = None

# Username input
username = st.sidebar.text_input("Username:", value=st.session_state.username)

if st.sidebar.button("Login"):
    # Check if username exists in users
    if username in users_df["USERNAME"].values:
        # Get the user's display name
        user_display_name = users_df[users_df["USERNAME"] == username]["DISPLAY_NAME"].values[0]
        
        # Store login info in session state
        st.session_state.username = username
        st.session_state.user_display_name = user_display_name

        st.sidebar.success(f"Welcome, {user_display_name}!")
    else:
        st.sidebar.error("User not found!")

# Messaging section (only visible after login)
if st.session_state.username:
    st.subheader("Send a Message")
    
    # Show a dropdown for selecting a recipient (exclude current user)
    recipient = st.selectbox("Select Recipient", users_df[users_df["USERNAME"] != st.session_state.username]["DISPLAY_NAME"])
    
    message_text = st.text_area("Enter your message:")
    
    # Ensure the button click action is only handled once
    if 'message_sent' not in st.session_state:
        st.session_state.message_sent = False  # Track if the message was sent

    if st.button("Send") and not st.session_state.message_sent:
        if message_text.strip():
            # Debugging: Log message contents before sending
            st.write(f"Sending message to {recipient}: {message_text}")
            
            # Get the recipient's username
            recipient_username = users_df[users_df["DISPLAY_NAME"] == recipient]["USERNAME"].values[0]
            
            # Construct the query with usernames (not IDs)
            query = f"""
            INSERT INTO MSG_APP.PUBLIC.MESSAGES (SENDER_ID, RECEIVER_ID, MESSAGE_TEXT, MESSAGE_TIMESTAMP)
            SELECT SENDER.USER_ID, RECEIVER.USER_ID, '{message_text.replace("'", "''")}', CURRENT_TIMESTAMP
            FROM MSG_APP.PUBLIC.USERS SENDER
            JOIN MSG_APP.PUBLIC.USERS RECEIVER
            ON SENDER.USERNAME = '{st.session_state.username}' AND RECEIVER.USERNAME = '{recipient_username}'
            """
            
            try:
                # Execute the query to insert the message
                session.sql(query).collect()  # Execute the query to insert the message
                st.success("Message sent!")
                
                # Update session state to indicate that the message has been sent
                st.session_state.message_sent = True
            except Exception as e:
                st.error(f"Error sending message: {e}")
        else:
            st.error("Message cannot be empty!")

    # View the conversation
    st.subheader("Conversation")
    
    # Query to fetch the conversation
    query = f"""
    SELECT 
        U1.DISPLAY_NAME AS SENDER,
        U2.DISPLAY_NAME AS RECEIVER,
        M.MESSAGE_TEXT,
        M.MESSAGE_TIMESTAMP
    FROM MSG_APP.PUBLIC.MESSAGES M
    JOIN MSG_APP.PUBLIC.USERS U1 ON M.SENDER_ID = U1.USER_ID
    JOIN MSG_APP.PUBLIC.USERS U2 ON M.RECEIVER_ID = U2.USER_ID
    WHERE (U1.USERNAME = '{st.session_state.username}' OR U2.USERNAME = '{st.session_state.username}')
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

