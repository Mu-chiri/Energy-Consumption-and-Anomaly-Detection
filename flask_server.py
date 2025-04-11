from flask import Flask, request, jsonify
from pymongo import MongoClient, errors as pymongo_errors
from datetime import datetime
import os
from dotenv import load_dotenv
import pytz

load_dotenv()

app = Flask(__name__)

# Load the MongoDB URI from your .env file.
MONGO_URI = os.getenv("MONGO_URI")
try:
    client = MongoClient(MONGO_URI)
    db = client["energy_db"]
    collection = db["sensor_readings"]
except pymongo_errors.PyMongoError as e:
    app.logger.error("Failed to connect to MongoDB: %s", e)
    raise SystemExit("MongoDB connection failed")

# Define the EAT timezone (Africa/Nairobi)
EAT = pytz.timezone("Africa/Nairobi")

@app.route('/')
def welcome():
    return "Welcome to the Energy Data API"

@app.route('/energy', methods=['POST'])
def store_energy_data():
    # Stage 1: Retrieve JSON data from request
    try:
        data = request.get_json()
        if not data:
            app.logger.error("No JSON payload received")
            return jsonify({"error": "No JSON payload received"}), 400
        app.logger.info("Received Data: %s", data)
    except Exception as e:
        app.logger.error("Error parsing JSON payload: %s", e)
        return jsonify({"error": "Invalid JSON payload"}), 400

    # Stage 2: Process the data and prepare the document
    try:
        # Record the current time in Kenyan time (EAT)
        timestamp_dt = datetime.now(EAT)

        # Validate required field "sensor_1"
        if "sensor_1" not in data:
            app.logger.error("Missing required sensor data: sensor_1")
            return jsonify({"error": "Missing required sensor data: sensor_1"}), 400

        document = {
            "timestamp": timestamp_dt,
            "metadata": {"sensor_ids": ["sensor_1"]},
            "granularity": data.get("granularity", 15),
            "sensor_1_energy": data.get("sensor_1")
        }
    except Exception as e:
        app.logger.error("Error processing data: %s", e)
        return jsonify({"error": "Error processing data"}), 500

    # Stage 3: Insert document into MongoDB
    try:
        result = collection.insert_one(document)
        app.logger.info("Inserted document with id: %s", result.inserted_id)
    except pymongo_errors.PyMongoError as e:
        app.logger.error("Error inserting document into MongoDB: %s", e)
        return jsonify({"error": "Failed to insert data into database"}), 500

    # Stage 4: Send acknowledgment response
    response = {
        "message": "Data stored successfully and acknowledged.",
        "id": str(result.inserted_id)
    }
    return jsonify(response), 200

if __name__ == "__main__":
    # Bind to static IP address 192.168.137.207 on port 5000.
    app.run(host="192.168.137.1", port=5000, debug=True)
