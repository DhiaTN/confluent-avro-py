from abc import ABC
from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth


class RegistryAuthBase(AuthBase, ABC):
    pass


class RegistryHTTPBasicAuth(RegistryAuthBase, HTTPBasicAuth):
    pass


class RegistryHTTPDigestAuth(RegistryAuthBase, HTTPDigestAuth):
    pass
