import logging
from typing import Dict, List
from dataclasses import dataclass

from .APIRequestHandler import APIRequestHandler




class User(APIRequestHandler):
    USER_API_VERSION = 'v2.0'

    @dataclass
    class Identity:
        """
        Dataclass for the Source entity in the Permutive ecosystem.
        """
        user_id: str
        aliases: List['User.Identity.Alias']

        @dataclass
        class Alias():
            """
            Dataclass for the Alias entity in the Permutive ecosystem.
            """
            id: str
            tag: str = "email_sha256"
            priority: int = 0

    def __init__(self,
                 api_key
                 ) -> None:
        super().__init__(api_key=api_key,
                         api_endpoint=f'https://api.permutive.com/{self.USER_API_VERSION}/identify',
                         payload_keys=["user_id", "aliases"])

    def identify(self,
                 identity: Identity):

        logging.info(f"UserAPI::identify::{identity.user_id}")

        url = f"{self.api_endpoint}"
        aliases_name = [alias.tag for alias in identity.aliases]
        if "email_sha256" in aliases_name and "uID" not in aliases_name:
            alias_id = next(
                (alias.id for alias in identity.aliases if alias.tag == "email_sha256"), "")
            alias = User.Identity.Alias(id=alias_id,
                                           tag="uID")
            identity.aliases.append(alias)
        if "email_sha256" not in aliases_name and "uID" in aliases_name:
            tag = "uID"
            alias_id = next(
                (alias.id for alias in identity.aliases if alias.tag == tag), "")
            alias = User.Identity.Alias(id=alias_id,
                                           tag="email_sha256")
            identity.aliases.append(alias)

        return self.postRequest(
            url=url,
            data=self.to_payload(identity))
