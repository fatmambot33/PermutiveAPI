"""Source entity serialization for the Permutive API."""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from PermutiveAPI.Utils import JSONSerializable


@dataclass
class Source(JSONSerializable):
    """Dataclass for the Source entity in the Permutive ecosystem.

    Attributes
    ----------
    id : str
        Unique identifier for the source.
    state : Dict[str, Any]
        State information of the source.
    type : str
        Type of the source.
    schema_id : Optional[str]
        Schema identifier associated with the source.
    cohorts : Optional[List[str]]
        List of cohorts associated with the source.
    bucket : Optional[str]
        Bucket information for the source.
    permissions : Optional[Dict[str, Any]]
        Permissions associated with the source.
    phase : Optional[str]
        Phase information of the source.
    errors : Optional[List[str]]
        List of errors associated with the source.
    advertiser_name : Optional[str]
        Name of the advertiser associated with the source.
    organization_id : Optional[str]
        Organization identifier associated with the source.
    version : Optional[str]
        Version information of the source.
    """

    id: str
    state: Dict[str, Any]
    type: str
    schema_id: Optional[str] = None
    cohorts: Optional[List[str]] = None
    bucket: Optional[str] = None
    permissions: Optional[Dict[str, Any]] = None
    phase: Optional[str] = None
    errors: Optional[List[str]] = None
    advertiser_name: Optional[str] = None
    organization_id: Optional[str] = None
    version: Optional[str] = None
