import sys

# In Python 3 StandardError --> Exception.
if sys.version_info < (3, 0):
    BaseClass = StandardError
else:
    BaseClass = Exception

class Error(BaseClass):
    def __init__(self, code, msg, what):
        BaseClass.__init__(self, msg)
        self.code = code
        self.what = what


    def __str__(self):
        return 'code: {self.code}, message: {self.message}'.format(self=self)


    def __repr__(self):
        return self.__str__()

