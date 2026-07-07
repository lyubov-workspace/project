import os


def get_clickhouse_url() -> str:
    port = os.environ.get('CLICKHOUSE_HTTP_PORT', '8123')
    return f'http://main_clickhouse:{port}'


def get_clickhouse_auth() -> tuple[str, str]:
    return os.environ['CLICKHOUSE_USER'], os.environ['CLICKHOUSE_PASSWORD']


def escape_ch_sql(value: str) -> str:
    """Экранирование для строк в SQL ClickHouse (postgresql table function)."""
    return value.replace('\\', '\\\\').replace("'", "\\'")
