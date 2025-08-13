""" Consume redpanda topic and send mesage to a slack channel
"""
import os
from slack_sdk import WebClient

message = "This is my first Slack message from Python!"

# Set up a WebClient with the Slack OAuth token
client = WebClient(token=os.environ['SLACK_AUTH_TOKEN'])

# Send a message
client.chat_postMessage(
    channel="general", 
    text=message, 
    username="python messenger"
)