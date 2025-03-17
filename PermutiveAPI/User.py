import logging
from typing import List
from dataclasses import dataclass
from datetime import datetime

from PermutiveAPI.Utils import RequestHelper, JSONSerializable


_API_VERSION = 'v2.0'
_API_ENDPOINT = f'https://api.permutive.com/{_API_VERSION}/identify'
_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Alias(JSONSerializable):
    """
    Dataclass for the Alias entity in the Permutive ecosystem.
    """
    id: str
    tag: str
    priority: int


@dataclass
class Identity(JSONSerializable):
    """
    Dataclass for the Source entity in the Permutive ecosystem.
    """
    user_id: str
    aliases: List[Alias]

    def to_json(self) -> dict:
        return {
            "user_id": self.user_id,
            "aliases": [alias.to_json() for alias in self.aliases] if self.aliases else None
        }

    @classmethod
    def from_json(cls, 
                  data: dict) -> 'Identity':
        aliases_data = data.get('aliases')
        if aliases_data:
            data['segments'] = [Alias.from_json(
                alias) for alias in aliases_data]
        return super().from_json(data)

    def Identify(self,
                 privateKey: str):

        logging.debug(
            f"{datetime.now()}::UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        return RequestHelper.post_static(api_key=privateKey,
                                                url=url,
                                                data=RequestHelper.to_payload_static(self,
                                                                                     _API_PAYLOAD))
