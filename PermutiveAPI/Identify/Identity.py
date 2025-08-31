"""User identification helpers for the Permutive API."""

import logging
from typing import List
from dataclasses import dataclass

from requests import Response

from PermutiveAPI.Identify import _API_ENDPOINT
from PermutiveAPI.Identify.Alias import Alias
from PermutiveAPI.Utils import RequestHelper, JSONSerializable


_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Identity(JSONSerializable):
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

    user_id: str
    aliases: List[Alias]

    def identify(self, api_key: str) -> Response:
        """Identify a user in Permutive.

        This method sends a POST request to the Permutive API to identify a user
        with the given aliases. Any exceptions raised during the request are
        propagated to the caller.

        Parameters
        ----------
        api_key : str
            The API key for authentication.

        Returns
        -------
        Response
            The response from the Permutive API.

        Raises
        ------
        ValueError
            If the API call returns no response.
        """
        logging.debug(f"UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        response = RequestHelper.post_static(
            api_key=api_key,
            url=url,
            data=RequestHelper.to_payload_static(self, _API_PAYLOAD),
        )

        if response is None:
            raise ValueError("Failed to identify user")

        return response
