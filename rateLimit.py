import sqlite3
import time
from flask import request
import random

# Create an in-memory SQLite database and initialize the rate-limiting table
db_connection = sqlite3.connect(":memory:", check_same_thread=False)
cursor = db_connection.cursor()
cursor.execute('''
    CREATE TABLE rate_limit (
        client_ip TEXT PRIMARY KEY,
        attempts INTEGER DEFAULT 0,
        last_attempt_time REAL DEFAULT 0
    )
''')
db_connection.commit()

# Configuration
MAX_ATTEMPTS = 5
BASE_DELAY = 1.0  # Base delay in seconds
JITTER = 0.5      # Jitter in seconds
BLOCK_TIME = 60   # Time in seconds before resetting attempts


def rate_limit():
    """Implements rate-limiting with exponential backoff and jitter."""
    client_ip = request.remote_addr  # Get client IP
    current_time = time.time()

    # Fetch client data from the database
    cursor.execute("SELECT attempts, last_attempt_time FROM rate_limit WHERE client_ip = ?", (client_ip,))
    row = cursor.fetchone()

    if row:
        attempts, last_attempt_time = row
        # Check if the blocking period has passed
        if current_time - last_attempt_time > BLOCK_TIME:
            attempts = 0  # Reset attempts after the blocking period

        # Increment attempts and calculate exponential delay with jitter if above limit
        if attempts >= MAX_ATTEMPTS:
            delay = BASE_DELAY * (2 ** (attempts - MAX_ATTEMPTS)) + random.uniform(0, JITTER)
            time_since_last_attempt = current_time - last_attempt_time

            # Enforce the delay only if the last attempt was too recent
            if time_since_last_attempt < delay:
                remaining_delay = delay - time_since_last_attempt
                return False, f"Rate limit exceeded. Retry in {round(remaining_delay, 2)} seconds."

        # Update the attempts count
        cursor.execute('''
            UPDATE rate_limit 
            SET attempts = ?, last_attempt_time = ?
            WHERE client_ip = ?
        ''', (attempts + 1, current_time, client_ip))
    else:
        # Insert a new record for the client
        cursor.execute('''
            INSERT INTO rate_limit (client_ip, attempts, last_attempt_time)
            VALUES (?, ?, ?)
        ''', (client_ip, 1, current_time))

    db_connection.commit()
    return True, "Request allowed."