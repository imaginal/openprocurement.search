# -*- coding: utf-8 -*-
import munch
import logging
import requests
import socket
import time

logger = logging.getLogger(__name__)


class NotFoundError(Exception):
    pass


class RetryError(Exception):
    pass


class BaseClient(object):
    """base class for API"""
    def __init__(self, key,
                 host_url,
                 api_version,
                 resource,
                 params=None,
                 **kwargs):
        self.prefix_path = '{}/api/{}/{}'.format(host_url, api_version, resource)
        if not isinstance(params, dict):
            params = {"mode": "_all_"}
        self.params = params
        self.timeout = float(kwargs.get('timeout', 30))
        self.max_retry = int(kwargs.get('max_retry', 5))
        self.use_cookies = int(kwargs.get('use_cookies', True))
        self.user_agent = kwargs.get('user_agent', 'op.search')
        if 'session' in kwargs:  # shared session between clients
            self.shared_session = True
            self.session = kwargs['session']
            return
        self.shared_session = False
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        if self.timeout and self.timeout > 0:
            self.session.timeout = self.timeout
        if key:
            self.session.auth = (key, '')
        if self.use_cookies:
            self.request_cookie()

    @property
    def headers(self):
        return self.session.headers

    @property
    def cookies(self):
        return dict(self.session.cookies)

    def close(self):
        if not self.shared_session:
            self.session.close()

    def request_cookie(self):
        self.session.get(self.prefix_path, timeout=self.timeout)

    def log_error(self, url, exc, method='GET'):
        logger.error("{} {} {}".format(method, url, repr(exc)))
        if hasattr(exc, 'request') and getattr(exc, 'request', None):
            request = exc.request
            headers = "\n".join(["  {}: {}".format(*i) for i in request.headers.items()])
            logger.error("Request {} {}\n{}".format(request.method, request.url, headers))
        if hasattr(exc, 'response') and getattr(exc, 'response', None):
            response = exc.response
            headers = "\n".join(["  {}: {}".format(*i) for i in response.headers.items()])
            logger.error("Response {}\n{}".format(response.status_code, headers))

    def get_list(self, params={}, feed='changes'):
        params['feed'] = feed
        for i in range(self.max_retry):
            try:
                self.params.update(params)
                response = self.session.get(self.prefix_path,
                    params=self.params, timeout=self.timeout)

                if response.status_code == 404:
                    raise NotFoundError("404 Not found {}".format(self.prefix_path))
                else:
                    response.raise_for_status()

                resp_list = munch.munchify(response.json())
                if 'next_page' in resp_list and 'offset' in resp_list['next_page']:
                    self.params['offset'] = resp_list.next_page.offset
                return resp_list['data']

            except (socket.error, requests.RequestException) as e:
                self.log_error(self.prefix_path, e)
                if i < self.max_retry - 1:
                    logger.error("Retry {} of {}".format(i + 1, self.max_retry))
                    time.sleep(10 * i + 10)

        raise RetryError("Maximum retry reached for {}".format(self.prefix_path))

    def get_item(self, item_id):
        url = "{}/{}".format(self.prefix_path, item_id)
        for i in range(self.max_retry):
            try:
                if self.use_cookies and not self.session.cookies:
                    self.request_cookie()
                response = self.session.get(url, timeout=self.timeout)

                if response.status_code == 404:
                    raise NotFoundError("404 Not found {}".format(url))
                else:
                    response.raise_for_status()

                return munch.munchify(response.json())

            except (socket.error, requests.RequestException) as e:
                self.log_error(url, e)
                if i > 1:
                    self.session.cookies.clear()
                if i < self.max_retry - 1:
                    logger.error("Retry {} of {}".format(i, self.max_retry))
                    time.sleep(10 * i + 10)

        raise RetryError("Maximum retry reached for {}".format(url))
