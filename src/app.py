from flask import Flask, jsonify
from flask.signals import got_request_exception
from pyspark.sql.streaming import DataStreamWriter
import socket
from flask_restful import Resource, Api
import logging
import pyspark


app = Flask(__name__)

# Define a sample endpoint that returns JSON data
@app.route('/api/data', methods=['GET'])
def get_data():
  data = [
    {
        'id': 1,
        'name': 'Example Data',
        'value': 42
    },
    {
        'id': 2,
        'name': 'Example 2',
        'value': 99
    }
  ]
  return jsonify(data)

if __name__ == '__main__':
    host = socket.gethostname() # Allows external connections
    port = 8999

    # Run the Flask app
    app.run(debug=True, host=host, port=port)
   
