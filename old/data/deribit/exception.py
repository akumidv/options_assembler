# coding=utf-8
import json



class DeribitAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res.get('code')
            self.message = json_res.get('msg')
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class DeribitRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'DeribitRequestException: %s' % self.message



class NotImplementedException(NotImplemented):
    def __init__(self, value):
        message = f'Not implemented: {value}'
        super().__init__(message)
