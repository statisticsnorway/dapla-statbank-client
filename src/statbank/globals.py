import datetime as dt

OSLO_TIMEZONE = dt.timezone.utc + dt.timedelta(hours=1)
TOMORROW = dt.datetime.now(tz=OSLO_TIMEZONE) + dt.timedelta(days=1)
APPROVE_DEFAULT_JIT = 2
STATBANK_TABLE_ID_LEN = 5
REQUEST_OK = 200
SSB_TBF_LEN = 3
