import sys

# In Python 3 StandardError --> Exception.
if sys.version_info < (3, 0):
    class Error(StandardError):
        pass
else:
    class Error(Exception):
        pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class OperationalError(DatabaseError):

    @classmethod
    def from_response(cls, response):
        return cls('{r.status_code}: {r.reason}; {r.content}'.format(r=response))
