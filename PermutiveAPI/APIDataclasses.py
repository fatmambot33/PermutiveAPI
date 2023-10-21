from dataclasses import dataclass
from typing import Dict, List, Optional, Union, Any   
from datetime import datetime

from .Utils import RequestHelper,FileHelper


@dataclass
class Cohort:
    """
    Dataclass for the Cohort entity in the Permutive ecosystem.
    """
    name: str
    id: Optional[str] = None
    code: Optional[str] = None
    query: Optional[Dict] = None
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    state: Optional[str] = None
    segment_type: Optional[str] = None
    live_audience_size: Optional[int] = 0
    created_at: Optional[datetime] = datetime.now()
    last_updated_at: Optional[datetime] = datetime.now()
    workspace_id: Optional[str] = None
    request_id: Optional[str] = None
    error: Optional[str] = None

    def to_payload(self, 
                    keys: Optional[List[str]] = ["name", "query", "description", "tags"]) -> Dict[str, Any]:
        return RequestHelper.to_payload(self, keys=keys)

    def to_json(self, 
                filepath: str):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: str) -> 'Cohort':
        jsonObj = FileHelper.from_json(filepath=filepath)
        return Cohort(**jsonObj)
@dataclass
class Import:
    """
    Dataclass for the Provider Import in the Permutive ecosystem.
    """
    id: str
    name: str
    code: str
    relation: str
    identifiers: List[str]
    description: Optional[str] = None
    source: Optional['Source'] = None
    inheritance: Optional[str] = None
    segments: Optional[List['Segment']] = None
    updated_at: Optional[datetime] = datetime.now()

    def to_json(self, filepath: str):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: str) -> 'Import':
        jsonObj = FileHelper.from_json(filepath=filepath)
        return Import(**jsonObj)

   
@dataclass
class Source:
    """
    Dataclass for the Source entity in the Permutive ecosystem.
    """
    id: str
    state: Dict
    bucket: str
    permissions: Dict
    phase: str
    type: str

    def to_json(self, filepath: str):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: str) -> 'Source':
        jsonObj = FileHelper.from_json(filepath=filepath)
        return Source(**jsonObj)

@dataclass
class Segment:
    """
    Dataclass for the Segment entity in the Permutive ecosystem.
    """
    id: str
    code: str
    name: str
    import_id: str
    description: Optional[str] = None
    cpm: Optional[float] = 0.0
    categories: Optional[List[str]] = None
    updated_at: Optional[datetime] = datetime.now()


    def to_json(self, filepath: str):
        FileHelper.to_json(self, filepath=filepath)
        
    @staticmethod
    def from_json(filepath: str) -> 'Segment':
        jsonObj = FileHelper.from_json(filepath=filepath)
        return Segment(**jsonObj)

