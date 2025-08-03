"""User identification helpers for the Permutive API."""

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

    :param id: The ID of the alias.
    :type id: str
    :param tag: The tag of the alias.
    :type tag: str
    :param priority: The priority of the alias.
    :type priority: int
    """

    id: str
    tag: str
    priority: int


@dataclass
class Identity(JSONSerializable):
    """
    Dataclass for the Identity entity in the Permutive ecosystem.

    :param user_id: The user's ID.
    :type user_id: str
    :param aliases: A list of aliases for the user.
    :type aliases: List[Alias]
    """

    user_id: str
    aliases: List[Alias]

    def identify(self,
                 api_key: str):
        """Identify a user in Permutive.

        This method sends a POST request to the Permutive API to identify a user
        with the given aliases.

        Args:
            api_key (str): The API key for authentication.

        Returns:
            Response: The response from the Permutive API.
        """
        logging.debug(
            f"{datetime.now()}::UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        return RequestHelper.post_static(api_key=api_key,
                                                url=url,
                                                data=RequestHelper.to_payload_static(self,
                                                                                     _API_PAYLOAD))
