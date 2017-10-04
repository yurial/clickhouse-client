
class Statistic:
    bytes_read = None
    rows_read = None
    elapsed = None

    def __init__(self, bytes_read, rows_read, elapsed):
        self.bytes_read = bytes_read
        self.rows_read = rows_read
        self.elapsed = elapsed


class Result:
    meta = None
    data = None
    totals = None
    statistics = None

    def __init__(self, meta, data, totals = None, statistics = None, **kwargs):
        self.meta = meta
        self.data = data
        self.totals = totals
        if statistics:
            self.statistics = Statistic(*statistics)

