"""User identification helpers for the Permutive API."""

import logging
from typing import List, Optional
from dataclasses import dataclass

from requests import Response

from PermutiveAPI.Utils import RequestHelper, JSONSerializable


_API_VERSION = 'v2.0'
_API_ENDPOINT = f'https://api.permutive.com/{_API_VERSION}/identify'
_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Alias(JSONSerializable):
    """Dataclass for the Alias entity in the Permutive ecosystem.

    Attributes
    ----------
    id : str
        The ID of the alias.
    tag : str
        The tag of the alias.
    priority : int
        The priority of the alias.
    """

    id: str
    tag: str
    priority: int


@dataclass
class Identity(JSONSerializable):
    """Dataclass for the Identity entity in the Permutive ecosystem.

    Attributes
    ----------
    user_id : str
        The user's ID.
    aliases : List[Alias]
        A list of aliases for the user.
    """

    user_id: str
    aliases: List[Alias]

    def identify(self,
                 api_key: str) -> Optional[Response]:
        """Identify a user in Permutive.

        This method sends a POST request to the Permutive API to identify a user
        with the given aliases.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        Optional[Response]
            The response from the Permutive API.
        """
        logging.debug(f"UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        return RequestHelper.post_static(api_key=api_key,
                                         url=url,
                                         data=RequestHelper.to_payload_static(self,
                                                                              _API_PAYLOAD))
