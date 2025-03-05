import os
import time
import threading
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 5000))
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
TOKENS_FILE = "tokens.txt"  # File to store token heartbeats

app = Flask(__name__)

# Store last received timestamp for each token
heartbeat_data = {}
last_signal_time = time.time()  # Tracks last global signal time
tokens_loaded = False  # Ensure tokens are loaded before accepting signals
loaded_tokens = set()  # Track tokens loaded from the file to prevent false alerts

def load_heartbeat_data():
    """Load initial heartbeat timestamps from the file before accepting new heartbeats."""
    global heartbeat_data, last_signal_time, tokens_loaded, loaded_tokens

    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as file:
            for line in file:
                try:
                    token, timestamp = line.strip().split(",")
                    heartbeat_data[token] = float(timestamp)
                    loaded_tokens.add(token)  # Mark as preloaded
                    print(f"Loaded token: {token}")  # Print each token when loaded
                except ValueError:
                    print(f"Skipping malformed line: {line.strip()}")  # Skip incorrect formats

    if heartbeat_data:
        last_signal_time = max(heartbeat_data.values(), default=time.time())

    tokens_loaded = True  # Mark tokens as loaded before accepting new heartbeats

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    """Receive a heartbeat signal after tokens are loaded."""
    global last_signal_time

    if not tokens_loaded:
        return jsonify({"error": "Server is still loading initial tokens"}), 503  # 503 = Service Unavailable

    data = request.json
    token = data.get("token")

    if not token:
        return jsonify({"error": "Token is required"}), 400

    heartbeat_data[token] = time.time()
    last_signal_time = time.time()  # Update last system-wide activity

    print(f"Received heartbeat from token: {token}")  # Print each token received

    return jsonify({"message": f"Heartbeat received for token {token}"}), 200

def monitor_collector():
    """Monitor heartbeats and trigger Slack alerts if no signals are received in 10 minutes."""
    global last_signal_time
    timeSlots = 600  # 10 minutes threshold

    while True:
        time.sleep(60)  # Check every minute
        current_time = time.time()

        # 1️⃣ Send a GLOBAL alert if NO signals at all in 10 minutes
        if current_time - last_signal_time > timeSlots:
            send_slack_alert("⚠️ No signals received from **any** application in the last 10 minutes!")
            last_signal_time = time.time()  # Reset alert time

        # 2️⃣ Check for inactive tokens individually, but skip preloaded tokens
        for token, last_timestamp in list(heartbeat_data.items()):
            if token in loaded_tokens:
                continue  # Skip tokens that were loaded at startup

            if current_time - last_timestamp > timeSlots:  # 10-minute threshold
                send_slack_alert(f"⚠️ No heartbeat received from `{token}` in the last 10 minutes!")
                del heartbeat_data[token]  # Remove token after alert

def send_slack_alert(message):
    print(f"send_slack_alert: {message}")  # Print each token when loaded
    """Send a message to Slack using the webhook URL."""
    if SLACK_WEBHOOK_URL:
        payload = {"text": message}
        requests.post(SLACK_WEBHOOK_URL, json=payload)
    else:
        print(f"⚠️ SLACK_WEBHOOK_URL is not set. Message: {message}")

# Load initial recorded heartbeat data
load_heartbeat_data()

# Start background threads for monitoring and data persistence
threading.Thread(target=monitor_collector, daemon=True).start()

if __name__ == '__main__':
    app.run(host=HOST, port=PORT)
