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


    def __init__(self, url, on_progress=None, **options):
        url = urlparse(url)
        self.scheme = url.scheme
        self.netloc = url.netloc
        self.options = dict([(key,str(val[0])) for key, val in parse_qs(url.query).iteritems()])
        self.options.update(options)
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
        return buffer


    def _build_url(self, opts):
        options = deepcopy(self.options)    #get copy of self.options
        options.update(opts)                #and override with opts
        options = dict([(key,val) for key, val in options.iteritems() if val is not None]) #remove keys with None values
        urlquery = '&'.join(['{}={}'.format(key,val) for key,val in options.iteritems()])
        url = '{self.scheme}://{self.netloc}/?{urlquery}'.format(self=self,urlquery=urlquery)
        return url


    def select(self, query, on_progress = None, **opts):
        import re
        from json import loads
        from errors import Error
        from result import Result
        if re.search('[)\s]FORMAT\s', query, re.IGNORECASE):
            raise Exception('Formatting is not available')
        query += ' FORMAT JSONCompact'
        if on_progress is None:
            on_progress = self.on_progress
        url = self._build_url(opts)
        data = self._fetch(url, query, on_progress)
        strdata = data.getvalue().decode('UTF-8')
        try:
            return Result(**loads(strdata))
        except BaseException as e:
            errre = re.compile('Code: (\d+), e.displayText\(\) = DB::Exception: (.+?), e.what\(\) = (.+)')
            m = errre.search(strdata)
            if m:
                raise Error(*m.groups())
            else:
                print(e, strdata)
                raise

