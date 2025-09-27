"""Source entity serialization for the Permutive API."""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from PermutiveAPI._Utils.json import JSONSerializable


@dataclass
class Source(JSONSerializable[Dict[str, Any]]):
    """Represents a Source entity in the Permutive ecosystem.

    Parameters
    ----------
    id : str
        Unique identifier for the source.
    state : Dict[str, Any]
        State information of the source.
    type : str
        Type of the source.
    schema_id : Optional[str], optional
        Schema identifier associated with the source (default: None).
    cohorts : Optional[List[str]], optional
        List of cohorts associated with the source (default: None).
    bucket : Optional[str], optional
        Bucket information for the source (default: None).
    permissions : Optional[Dict[str, Any]], optional
        Permissions associated with the source (default: None).
    phase : Optional[str], optional
        Phase information of the source (default: None).
    errors : Optional[List[str]], optional
        List of errors associated with the source (default: None).
    advertiser_name : Optional[str], optional
        Name of the advertiser associated with the source (default: None).
    organization_id : Optional[str], optional
        Organization identifier associated with the source (default: None).
    version : Optional[str], optional
        Version information of the source (default: None).
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
