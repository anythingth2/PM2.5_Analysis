import os
from dotenv import load_dotenv

load_dotenv()

HOST_IP = os.getenv('HOST_IP')
SERVICE_PORT = os.getenv('SERVICE_PORT')
DEBUG = bool(os.getenv('DEBUG'))
