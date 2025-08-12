""" Very simple server returning random json data activity
"""
from flask import Flask, jsonify
from random import randint
from datetime import date


app = Flask(__name__)

@app.route("/employee/<id_employee>", methods=["GET"])
def get_data(id_employee):
    """ Handle GET request
    """
    id_sport = randint(1, 15)
    distance = 12000 if id_sport in [4, 5, 6] else 0
    data = [{
        "id_employee": int(id_employee),
        "id_sport": id_sport,
        "distance": distance,
        'start_date': str(date.today()) + " 21:00:00",
        'end_date': str(date.today()) + " 23:00:00",
        "comment": "was great"
    }]
    # jsonify automatically transforms data in JSON and define the right Content-Type
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)