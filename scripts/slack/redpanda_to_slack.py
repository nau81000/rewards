""" Consume redpanda topic and send mesage to a slack channel
"""
from confluent_kafka import Consumer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from babel.dates import format_datetime
import os
import json

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")
SLACK_USERNAME = os.getenv("SLACK_USERNAME")

load_dotenv()
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
                # Building message
                text_msg= f"Bravo {after_data['id_employee']}! Vous avez fait {int(delta_hours)} heures de {after_data['id_sport']} le {formatted_date}"
                #print(f"Message envoyé: {text_msg}")
                slack_client.chat_postMessage(channel=SLACK_CHANNEL, text=text_msg, username=SLACK_USERNAME)
            except SlackApiError as e:
                print(f"Erreur Slack: {e.response['error']}")
