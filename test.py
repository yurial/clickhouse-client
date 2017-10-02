from client import ClickHouseClient
from errors import Error as ClickHouseError

def on_progress(total, read, progress):
    print(total,read,progress)

try:
    client = ClickHouseClient('http://ch00.fin.adfox.ru:8123/?user=api&password=apipass&send_progress_in_http_headers=1',on_progress=on_progress)
    query = 'SELECT date FROM adfox.dist_elog WHERE date > toDate(0)'
    result = client.select(query)
    print(result)
except ClickHouseError as e:
    print(e)
except Exception as e:
    print(e)
