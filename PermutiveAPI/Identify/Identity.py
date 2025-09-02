"""User identification helpers for the Permutive API."""

import logging
from typing import List, Dict, Any
from dataclasses import dataclass


from . import _API_ENDPOINT
from .Alias import Alias
from PermutiveAPI._Utils import http
from PermutiveAPI._Utils.json import JSONSerializable

_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Identity(JSONSerializable[Dict[str, Any]]):
    """Dataclass for the Identity entity in the Permutive ecosystem.

    Parameters
    ----------
    user_id : str
        The user's ID.
    aliases : List[Alias]
        A list of aliases for the user.

    Methods
    -------
    identify(api_key)
        Identify a user in Permutive.
    """

    _request_helper = http

    user_id: str
    aliases: List[Alias]

    def identify(self, api_key: str) -> None:
        """Identify a user in Permutive.

        This method sends a POST request to the Permutive API to identify a user
        with the given aliases. Any exceptions raised during the request are
        propagated to the caller.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Raises
        ------
        ValueError
            If the API call returns no response.
        PermutiveAPIError
            If the Permutive API responds with an error.
        """
        logging.debug(f"UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        self._request_helper.post(
            api_key=api_key,
            url=url,
            data=self._request_helper.to_payload(self, _API_PAYLOAD),
        )
