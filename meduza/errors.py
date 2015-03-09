__author__ = 'dvirsky'

class MeduzaError(Exception):
    pass

class ColumnValueError(MeduzaError):
    pass

class ModelError(MeduzaError):
    pass

class RequestError(MeduzaError):
    pass