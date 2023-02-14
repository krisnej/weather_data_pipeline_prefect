import clickhouse_connect


def get_connection():
    return clickhouse_connect.get_client(host='localhost', port=8123, user='default', password='')
