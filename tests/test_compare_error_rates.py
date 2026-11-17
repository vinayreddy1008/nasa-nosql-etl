"""Offline SQL-comparison checks; no PostgreSQL service required."""
from decimal import Decimal
from app import store


class FakeCursor:
    def __init__(self, data, state):
        self.data = data
        self.state = state
        self.output = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=()):
        if "FROM runs WHERE" in sql:
            self.output = [
                ("run_mongo", "mongo", [{"name": "NASA_access_log_Jul95", "size_bytes": 12}], 100000, Decimal('80.0')),
                ("run_mapreduce", "mapreduce", [{"name": "NASA_access_log_Jul95", "size_bytes": 12}], 100000, Decimal('30.0')),
            ]
        else:
            run_id = params[0]
            if "FROM q1_daily_traffic" in sql:
                self.output = [("1995-07-01", 200, 1, 8)]
            elif "FROM q2_top_resources" in sql:
                self.output = [("/a", 1, 8, 1)]
            elif "FROM q3_hourly_errors" in sql:
                rate = Decimal('0.500000000000')
                if run_id == 'run_mapreduce' and self.state['tamper']:
                    rate = Decimal('0.400000000000')
                self.output = [("1995-07-01", 0, 1, 2, rate, 1)]
            else:
                raise AssertionError('Unexpected SQL: '+sql)

    def fetchall(self):
        return self.output


class FakeConnection:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return FakeCursor(None, self.state)


def test_comparison_includes_stored_hourly_error_rates(monkeypatch):
    state = {'tamper': False}
    monkeypatch.setattr(store, 'db_connect', lambda: FakeConnection(state))
    assert store.compare_latest() is True

    state['tamper'] = True
    assert store.compare_latest() is False
