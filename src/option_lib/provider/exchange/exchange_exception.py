"""Exchange class exceptions"""
import json


class APIException(Exception):
    """Exchange API exception"""

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = f'Invalid JSON error message: {response.text}'
        else:
            self.code = json_res.get('code')
            self.message = json_res.get('msg')
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return f'APIError(code={self.code}): {self.message}'


class RequestException(Exception):
    """RequestException"""
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f'DeribitRequestException: {self.message}'



class NotImplementedException(NotImplementedError):
    """Method not implemented Error"""
    def __init__(self, value):
        message = f'Not implemented: {value}'
        super().__init__(message)
