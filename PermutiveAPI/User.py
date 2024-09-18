import logging
import json
from typing import List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from .Utils import RequestHelper, FileHelper

_API_VERSION = 'v2.0'
_API_ENDPOINT = f'https://api.permutive.com/{_API_VERSION}/identify'
_API_PAYLOAD = ["user_id", "aliases"]


@dataclass
class Alias():
    """
    Dataclass for the Alias entity in the Permutive ecosystem.
    """
    id: str
    tag: str 
    priority: int 

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(asdict(self), f, ensure_ascii=False, indent=4)

    @staticmethod
    def from_json(filepath: str) -> 'Alias':
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Alias(**json.load(json_file))




@dataclass
class Identity():
    """
    Dataclass for the Source entity in the Permutive ecosystem.
    """
    user_id: str
    aliases: List[Alias]

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(asdict(self), f, ensure_ascii=False, indent=4)

    @staticmethod
    def from_json(filepath: str) -> "Identity":
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Identity(**json.load(json_file))
    
    def Identify(
        self,
        privateKey: str):

        logging.debug(
            f"{datetime.now()}::UserAPI::identify::{self.user_id}")

        url = f"{_API_ENDPOINT}"

        return RequestHelper.postRequest_static(privateKey=privateKey,
                                                url=url,
                                                data=RequestHelper.to_payload_static(self,
                                                                                    _API_PAYLOAD))

