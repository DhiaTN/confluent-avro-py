import json

import requests

from avrokafka import exceptions

CONTETNT_TYPE = "application/json"


class SchemaRegistry(object):
    def __init__(self, url: str, auth: tuple, schema_id_size=4):
        self.url = url
        self.auth = auth
        self.schema_id_size = schema_id_size

    def get_schema(self, id: int):
        response = requests.get(
            url="{url}/schemas/ids/{id}".format(url=self.url, id=id),
            headers={"Content-Type": CONTETNT_TYPE},
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json().get("schema")

    def get_schema_info(self, subject: str, schema: dict):
        response = requests.post(
            url="{url}/subjects/{subject}".format(url=self.url, subject=subject),
            headers={"Content-Type": CONTETNT_TYPE},
            data=json.dumps({"schema": schema}),
            auth=self.auth,
        )
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise exceptions.SchemaNotRegistredError(e)
            raise exceptions.SchemaRegistryUnkownError(e)

    def register_schema(self, subject: str, schema: dict):
        response = requests.post(
            url="{url}/subjects/{subject}/versions".format(
                url=self.url, subject=subject
            ),
            headers={"Content-Type": CONTETNT_TYPE},
            data=json.dumps({"schema": schema}),
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json()

    def set_default_compatibility(self, level):
        response = requests.put(
            url="{url}/config".format(url=self.url),
            headers={"Content-Type": CONTETNT_TYPE},
            data=json.dumps({"compatibility": level}),
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json()

    def set_subject_compatibility(self, subject, level):
        response = requests.put(
            url="{url}/config/{subject}".format(url=self.url, subject=subject),
            headers={"Content-Type": CONTETNT_TYPE},
            data=json.dumps({"compatibility": level}),
            auth=self.auth,
        )
        response.raise_for_status()
        return response.json()
