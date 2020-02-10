from abc import ABC

from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth


class RegistryAuthBase(AuthBase, ABC):
    """Base class that all RegistryAuth drives from"""


class RegistryHTTPBasicAuth(RegistryAuthBase, HTTPBasicAuth):
    """Implements HTTP Basic Authentication"""


class RegistryHTTPDigestAuth(RegistryAuthBase, HTTPDigestAuth):
    """Implements HTTP Digest Authentication"""
