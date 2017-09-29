from sys import version_info
if version_info < (3, 0):
    from urlparse import urlparse, parse_qs
else:
    from urllib.parse import urlparse, parse_qs

from copy import deepcopy

class ClickHouseClient:
    scheme = None
    netloc = None
    options = None

    on_progress = None


    def __init__(self, url, on_progress=None):
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.options = dict([(key,str(val[0])) for key, val in parse_qs(url.query).iteritems()])
        self.on_progress = on_progress


    def __repr__(self):
        return str( (self.scheme, self.netloc, self.options, self.on_progress) )


    def _on_header_x_clickhouse_progress(self, on_progress, key, val):
        from json import loads
        obj = loads( val )
        total = int(obj['total_rows'])
        read = int(obj['read_rows'])
        progress=float(read)/float(total)
        on_progress(total=total, read=read, progress=progress)


    def _on_header(self, on_progress):
        def wrapper(header):
            try:
                key, val = header.split(':', 1)
                if key == 'X-ClickHouse-Progress':
                    self._on_header_x_clickhouse_progress(on_progress, key, val)
            except Exception as e:
                return
        return wrapper


    def _fetch(self, url, query, on_progress):
        from pycurl import Curl, POST, POSTFIELDS
        from io import BytesIO
        from json import loads
        c = Curl()
        c.setopt(c.URL, url)
        c.setopt(POST, 1)
        c.setopt(POSTFIELDS, query)
        if on_progress:
            c.setopt(c.HEADERFUNCTION, self._on_header(on_progress))
        buffer = BytesIO()
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
        c.close()
        return loads( buffer.getvalue().decode('UTF-8') )

    def _build_url(self, opts):
        options = deepcopy(self.options)    #get copy of self.options
        options.update(opts)                #and override with opts
        options = dict([(key,val) for key, val in options.iteritems() if val is not None]) #remove keys with None values
        urlquery = '&'.join(['{}={}'.format(key,val) for key,val in options.iteritems()])
        url = '{scheme}://{netloc}/?{urlquery}'.format(scheme=self.scheme,netloc=self.netloc,urlquery=urlquery)
        return url

    def select(self, query, on_progress = None, **opts):
        import re
        if re.search('[)\s]FORMAT\s', query, re.IGNORECASE):
            raise ProgrammingError('Formatting is not available')
        query += ' FORMAT JSONCompact'
        if on_progress is None:
            on_progress = self.on_progress
        url = self._build_url(opts)
        return self._fetch(url, query, on_progress)


