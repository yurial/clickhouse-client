from client import ClickHouseClient

def on_progress(total, read, progress):
    print(total,read,progress)

client = ClickHouseClient('http://ch00.fin.adfox.ru:8123/?user=analytic&password=anal&send_progress_in_http_headers=1',on_progress=on_progress)
query = 'SELECT date, count() FROM adfox.dist_elog WHERE date > today()-30 GROUP BY date'
result = client.select(query)
print(result)
