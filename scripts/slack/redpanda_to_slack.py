""" Consume redpanda topic and send mesage to a slack channel
"""
from confluent_kafka import Consumer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from babel.dates import format_datetime
from os import getenv
import json
import psycopg2 as pg

def main ():
    """
    """
    # Load environment variables
    load_dotenv()
    SLACK_TOKEN = getenv("SLACK_TOKEN")
    SLACK_CHANNEL = getenv("SLACK_CHANNEL")
    SLACK_USERNAME = getenv("SLACK_USERNAME")
    # Connect to DB
    config = {
           'host': getenv("POSTGRES_DB_HOST"),
           'dbname': getenv("POSTGRES_DB_NAME"), 
           'user': getenv("POSTGRES_ADMIN_USER"), 
           'password': getenv("POSTGRES_ADMIN_PWD"),
           'port': 5432
    }
    conn = pg.connect(**config)
    cursor = conn.cursor()
    # Connect to Slack
    slack_client = WebClient(token=SLACK_TOKEN)
    consumer = Consumer({
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'slack-poster',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['sport_data.public.activities'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if not msg.error():
            payload = json.loads(msg.value().decode("utf-8"))['payload']
            after_data = payload.get("after")
            if after_data:
                try:
                    # Difference in microseconds
                    delta_us = after_data['end_date'] - after_data['start_date']
                    # Convert in hours
                    delta_hours = delta_us / 1_000_000 / 3600
                    # Get the activity date in french
                    start_date = datetime.fromtimestamp(after_data['start_date'] / 1_000_000, tz=ZoneInfo("Europe/Paris"))
                    formatted_date = format_datetime(start_date, "EEEE d MMMM yyyy", locale="fr_FR")
                    # Get employee first and last names
                    cursor.execute(f"SELECT first_name, last_name FROM employees WHERE id_employee={after_data['id_employee']}")
                    result = cursor.fetchone()
                    first_name = result[0]
                    last_name = result[1]
                    # Get sport name
                    cursor.execute(f"SELECT name FROM sports WHERE id={after_data['id_sport']}")
                    sport = cursor.fetchone()[0]
                    # Building message
                    text_msg= f"Bravo {first_name} {last_name}! Vous avez fait {int(delta_hours)} heures de {sport} le {formatted_date}"
                    #print(f"Message envoyé: {text_msg}")
                    slack_client.chat_postMessage(channel=SLACK_CHANNEL, text=text_msg, username=SLACK_USERNAME)
                except SlackApiError as e:
                    print(f"Erreur Slack: {e.response['error']}")

if __name__ == "__main__":
    main()