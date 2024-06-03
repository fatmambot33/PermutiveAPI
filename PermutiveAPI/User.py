import logging
import json
from typing import List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from . import RequestHelper, FileHelper

_API_VERSION = 'v2.0'
_API_ENDPOINT = f'https://api.permutive.com/{_API_VERSION}/identify'
_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Alias():
    """
    Dataclass for the Alias entity in the Permutive ecosystem.
    """
    id: str
    tag: str = "email_sha256"
    priority: int = 0

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Alias':
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Alias(**json.load(json_file))


@dataclass
class AliasList(List[Alias]):
    def __init__(self, alias_list: Optional[List["Alias"]] = None):
        """Initializes the ImportList with an optional list of Import objects."""
        super().__init__(alias_list if alias_list is not None else [])


@dataclass
class Identity():
    """
    Dataclass for the Source entity in the Permutive ecosystem.
    """
    user_id: str
    aliases: AliasList

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> "Identity":
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Identity(**json.load(json_file))


def Identify(
        identity: Identity,
        privateKey: str):

    logging.debug(
        f"{datetime.now()}::UserAPI::identify::{identity.user_id}")

    url = f"{_API_ENDPOINT}"

    aliases_name = [alias.tag for alias in identity.aliases]
    if "email_sha256" in aliases_name and "uID" not in aliases_name:
        alias_id = next(
            (alias.id for alias in identity.aliases if alias.tag == "email_sha256"), "")
        alias = Alias(id=alias_id,
                      tag="uID")
        identity.aliases.append(alias)
    if "email_sha256" not in aliases_name and "uID" in aliases_name:
        tag = "uID"
        alias_id = next(
            (alias.id for alias in identity.aliases if alias.tag == tag), "")
        alias = Alias(id=alias_id,
                           tag="email_sha256")
        identity.aliases.append(alias)

    return RequestHelper.postRequest_static(privateKey=privateKey,
                                            url=url,
                                            data=RequestHelper.to_payload_static(identity,
                                                                                 _API_PAYLOAD))
