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


    def Identify(self,
                 api_key: str):

        logging.debug(
            f"{datetime.now()}::UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        return RequestHelper.post_static(api_key=api_key,
                                                url=url,
                                                data=RequestHelper.to_payload_static(self,
                                                                                     _API_PAYLOAD))
