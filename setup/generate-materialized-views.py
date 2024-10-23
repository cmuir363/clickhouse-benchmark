import clickhouse_connect
from dotenv import load_dotenv
import os


load_dotenv()

db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')

client = clickhouse_connect.get_client(host=db_host, port=db_port, username=db_username, password=db_password, interface="https")