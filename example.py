from client import ClickHouseClient
from errors import Error as ClickHouseError
import logging

logging.basicConfig(level=logging.DEBUG)

def on_progress(total, read, progress):
    print(total,read,progress)

try:
    client = ClickHouseClient('http://example.com:8123/', on_progress=on_progress, user='user', password='password')
    query = 'SELECT sum(load) as loads FROM dbname.table WHERE date = today()'
    result = client.select(query, send_progress_in_http_headers=1)
    print(result.data)
except ClickHouseError as e:
    print(e)
except Exception as e:
    print(e)
