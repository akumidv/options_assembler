from base64 import b64encode
import hashlib
import hmac
from Crypto.PublicKey import RSA
from Crypto.Hash import SHA256
from Crypto.Signature import pkcs1_15
import time
from operator import itemgetter
from urllib.parse import urlencode
from pathlib import Path
from typing import Optional, Dict, Union, List, Tuple
import aiohttp
import asyncio
from .exception import BinanceAPIException, BinanceRequestException

# https://github.com/sammchardy/python-binance/tree/master/binance
class BaseClient:
    API_URL = 'https://api.binance.com/api'
    API_TESTNET_URL = 'https://testnet.binance.vision/api'
    BASE_ENDPOINT_DEFAULT = ''
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    REQUEST_TIMEOUT: float = 10

    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            requests_params: Optional[Dict[str, str]] = None,
            base_api: str = 'https://api.binance.com/api',
            testnet: bool = False, private_key: Optional[Union[str, Path]] = None,
            private_key_pass: Optional[str] = None
    ):
        self.API_URL = base_api
        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.PRIVATE_KEY = self._init_private_key(private_key, private_key_pass)
        self.session = self._init_session()
        self.testnet = testnet
        self._requests_params = requests_params
        self.response = None
        self.timestamp_offset = 0


    def _get_headers(self) -> Dict:
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            # noqa
        }
        if self.API_KEY:
            assert self.API_KEY
            headers['X-MBX-APIKEY'] = self.API_KEY
        return headers


    def _init_private_key(self, private_key: Optional[Union[str, Path]], private_key_pass: Optional[str] = None):
        if not private_key:
            return
        if isinstance(private_key, Path):
            with open(private_key, "r") as f:
                private_key = f.read()
        return RSA.import_key(private_key, passphrase=private_key_pass)

    def _init_session(self):
        raise NotImplementedError

    def _create_api_uri(self, path: str, signed: bool = True, version: str = PUBLIC_API_VERSION) -> str:
        url = self.API_URL
        if self.testnet:
            url = self.API_TESTNET_URL
        v = self.PRIVATE_API_VERSION if signed else version
        return url + '/' + v + '/' + path

    def _rsa_signature(self, query_string: str):
        assert self.PRIVATE_KEY
        h = SHA256.new(query_string.encode("utf-8"))
        signature = pkcs1_15.new(self.PRIVATE_KEY).sign(h)
        return b64encode(signature).decode()

    def _hmac_signature(self, query_string: str) -> str:
        assert self.API_SECRET, "API Secret required for private endpoints"
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    def _generate_signature(self, data: Dict) -> str:
        sig_func = self._hmac_signature
        if self.PRIVATE_KEY:
            sig_func = self._rsa_signature
        query_string = '&'.join([f"{d[0]}={d[1]}" for d in self._order_params(data)])
        return sig_func(query_string)

    @staticmethod
    def _order_params(data: Dict) -> List[Tuple[str, str]]:
        """Convert params to list with signature as last element

        :param data:
        :return:

        """
        data = dict(filter(lambda el: el[1] is not None, data.items()))
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, str(value)))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _get_request_kwargs(self, method, signed: bool, force_params: bool = False, **kwargs) -> Dict:

        # set default requests timeout
        kwargs['timeout'] = self.REQUEST_TIMEOUT

        # add our global requests params
        if self._requests_params:
            kwargs.update(self._requests_params)

        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data
            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del kwargs['data']['requests_params']

        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        # sort get and post params to match signature order
        if data:
            # sort post params and remove any arguments with values of None
            kwargs['data'] = self._order_params(kwargs['data'])
            # Remove any arguments with values of None.
            null_args = [i for i, (key, value) in enumerate(kwargs['data']) if value is None]
            for i in reversed(null_args):
                del kwargs['data'][i]

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = '&'.join('%s=%s' % (data[0], data[1]) for data in kwargs['data'])
            del kwargs['data']
        return kwargs


class AsyncClient(BaseClient):
    def __init__(
        self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
        requests_params: Optional[Dict[str, str]] = None,
        base_endpoint: str = BaseClient.BASE_ENDPOINT_DEFAULT,
        testnet: bool = False, loop=None, session_params: Optional[Dict[str, str]] = None,
        private_key: Optional[Union[str, Path]] = None, private_key_pass: Optional[str] = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self._session_params: Dict[str, str] = session_params or {}
        super().__init__(api_key, api_secret, requests_params, base_endpoint, testnet, private_key, private_key_pass)


    @classmethod
    async def create(
        cls, api_key: Optional[str] = None, api_secret: Optional[str] = None,
        requests_params: Optional[Dict[str, str]] = None, tld: str = 'com',
        base_endpoint: str = BaseClient.BASE_ENDPOINT_DEFAULT,
        testnet: bool = False, loop=None, session_params: Optional[Dict[str, str]] = None
    ):

        self = cls(api_key, api_secret, requests_params, tld, base_endpoint, testnet, loop, session_params)

        try:
            await self.ping()
            # calculate timestamp offset between local and binance server
            res = await self.get_server_time()
            self.timestamp_offset = res['serverTime'] - int(time.time() * 1000)

            return self
        except Exception:
            # If ping throw an exception, the current self must be cleaned
            # else, we can receive a "asyncio:Unclosed client session"
            await self.close_connection()
            raise


    def _init_session(self) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession(
            loop=self.loop,
            headers=self._get_headers(),
            **self._session_params
        )
        return session

    async def close_connection(self):
        if self.session:
            assert self.session
            await self.session.close()


    async def _request(self, method, uri: str, signed: bool, force_params: bool = False, **kwargs):

        kwargs = self._get_request_kwargs(method, signed, force_params, **kwargs)

        async with getattr(self.session, method)(uri, **kwargs) as response:
            self.response = response
            return await self._handle_response(response)

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(response.status).startswith('2'):
            raise BinanceAPIException(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise BinanceRequestException(f'Invalid Response: {txt}')

    async def _request_api(self, method, path, signed=False, version=BaseClient.PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return await self._request(method, uri, signed, **kwargs)

    async def _request_futures_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_futures_api_uri(path)

        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_futures_data_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_futures_data_api_uri(path)

        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_futures_coin_api(self, method, path, signed=False, version=1, **kwargs) -> Dict:
        uri = self._create_futures_coin_api_url(path, version=version)

        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_futures_coin_data_api(self, method, path, signed=False, version=1, **kwargs) -> Dict:
        uri = self._create_futures_coin_data_api_url(path, version=version)

        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_options_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_options_api_uri(path)

        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_margin_api(self, method, path, signed=False, version=1, **kwargs) -> Dict:
        uri = self._create_margin_api_uri(path, version)

        return await self._request(method, uri, signed, **kwargs)

    async def _request_website(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_website_uri(path)
        return await self._request(method, uri, signed, **kwargs)

    async def _get(self, path, signed=False, version=BaseClient.PUBLIC_API_VERSION, **kwargs):
        return await self._request_api('get', path, signed, version, **kwargs)

    async def _post(self, path, signed=False, version=BaseClient.PUBLIC_API_VERSION, **kwargs) -> Dict:
        return await self._request_api('post', path, signed, version, **kwargs)

    async def _put(self, path, signed=False, version=BaseClient.PUBLIC_API_VERSION, **kwargs) -> Dict:
        return await self._request_api('put', path, signed, version, **kwargs)

    async def _delete(self, path, signed=False, version=BaseClient.PUBLIC_API_VERSION, **kwargs) -> Dict:
        return await self._request_api('delete', path, signed, version, **kwargs)

    # General Endpoints

    async def ping(self) -> Dict:
        return await self._get('ping', version=self.PRIVATE_API_VERSION)

    async def get_server_time(self) -> Dict:
        return await self._get('time', version=self.PRIVATE_API_VERSION)
    # get_server_time.__doc__ = Client.get_server_time.__doc__