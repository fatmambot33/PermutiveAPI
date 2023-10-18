from .APIRequestHandler import APIRequestHandler
import logging


class UserAPI():
    API_VERSION = 'v2.0'
    API_ENDPOINT = f'https://api.permutive.com/{API_VERSION}/identify'

    def __init__(self, api_key: str):
        logging.info(f"UserAPI::__init__")
        self.__api_key = api_key

    def identify(self,
                 user_id: str,
                 id: str,
                 tag: str = "email_sha256",
                 priority: int = 0):

        if user_id is None:
            raise ValueError('user_id must be specified')
        if id is None:
            raise ValueError('id must be specified')
        logging.info(f"UserAPI::identify::{user_id}")

        url = f"{self.API_ENDPOINT}?k={self.__api_key}"
        aliases = [
            {
                "tag": tag,
                "id": id,
                "priority": priority
            }
        ]
        if tag == "email_sha256":
            aliases.append({
                "tag": "uID",
                "id": id,
                "priority": priority
            })
        if tag == "uID":
            aliases.append({
                "tag": "email_sha256",
                "id": id,
                "priority": priority
            })
        payload = {
            "aliases": aliases,
            "user_id": user_id
        }
        return APIRequestHandler.post(
            url=url,
            data=payload)
