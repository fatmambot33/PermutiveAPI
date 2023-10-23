import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass,asdict
from datetime import datetime
import os
import urllib.parse
import pandas as pd
import glob

from .APIRequestHandler import APIRequestHandler
from .Utils import FileHelper, ListHelper


from dataclasses import dataclass

from typing import List, Optional
import os
from .Utils import FileHelper

TAGS = ['#automatic', '#spireglobal']
DEFAULT = {}
DEFAULT['NUM_OPERATOR'] = 'greater_than_or_equal_to'
ITEMS = {'ä': 'a',  'â': 'a', 'á': 'a', 'à': 'a', 'ã': 'a', 'ç': 'c', 'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
         'í': 'i',  'ï': 'i', 'ò': 'o', 'ó': 'o', 'õ': 'o', 'ô': 'o', 'ñ': 'n', 'ù': 'u', 'ú': 'u', 'ü': 'u'}


@dataclass
class Query():
    name: str
    market: Optional[str] = "CN"
    id: Optional[str] = None
    description: Optional[str] = None
    accurate_id: Optional[str] = None
    volume_id: Optional[str] = None
    obsidian_id: Optional[str] = None
    keywords: Optional[List[str]] = None
    urls: Optional[List[str]] = None
    taxonomy: Optional[List[str]] = None
    segments: Optional[List[str]] = None
    second_party_segments: Optional[List[Tuple[str, str]]] = None
    third_party_segments: Optional[List[int]] = None
    cohort_global: Optional[str] = None
    accurate_segments: Optional[List[str]] = None
    volume_segments: Optional[List[str]] = None
    obsidian_segments: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    number: Optional[str] = None
    during_value: Optional[int] = 0
    frequency_value: Optional[int] = 1
    page_view: Optional[bool] = True
    link_click: Optional[bool] = False
    slot_click: Optional[bool] = False
    engaged_time: Optional[bool] = False
    engaged_completion: Optional[bool] = False
    workspace_id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def sync_clickers(self, api_key, new_tags: List[str] = TAGS,  override_tags: bool = False):
        if not self.name:
            raise ValueError("self.name is None")

        logging.info('segment: ' + self.name)

        cohort_api = CohortAPI(api_key=api_key)
        cohort = cohort_api.get_by_name(cohort_name=self.name + " | Clickers")
        if cohort is None:
            cohort = CohortAPI.Cohort(
                name=self.name + " | Clickers", query=self.__create_cohort_query_clickers(), tags=new_tags)
            cohort_api.create(cohort=cohort)
        else:
            cohort.query = self.__create_cohort_query_clickers()
            if not override_tags:
                new_tags = ListHelper.merge_list(new_tags, cohort.tags)
            cohort.tags = new_tags
            cohort_api.update(cohort=cohort)

    def sync(self, api_key: str, new_tags: List[str] = TAGS,  override_tags: bool = False):
        if not self.name:
            raise ValueError("self.name is None")

        logging.info('segment: ' + self.name)
        cohort_api = CohortAPI(api_key=api_key)
        cohort = CohortAPI.Cohort(
            name=self.name, id=self.id, query=self.to_query(), tags=new_tags)

        if self.keywords is not None:
            cohort.description = ",".join(self.keywords)
        if self.id is None:
            if cohort.tags is not None:
                cohort.tags = ListHelper.merge_list(cohort.tags, TAGS)
            else:
                cohort.tags = TAGS
            cohort = cohort_api.create(cohort=cohort)
            if cohort is not None:
                self.id = cohort.id
                self.number = cohort.code
        else:
            if not override_tags:
                old_cohort = cohort_api.get(cohort_id=self.id)
                if old_cohort is not None:
                    if old_cohort.tags is not None:
                        cohort.tags = ListHelper.merge_list(
                            new_tags+old_cohort.tags)
            cohort = cohort_api.update(cohort=cohort)

    def to_query(self) -> Dict:
        query_list = []
        slugify_keywords = []
        if self.keywords is not None or self.taxonomy is not None or self.urls is not None or self.obsidian_id is not None:
            if self.keywords:
                slugify_keywords = Query.slugify_keywords(self.keywords)
                query_list.append(
                    self.__create_cohort_pageview(slugify_keywords=slugify_keywords))
                query_list.append(
                    self.__create_cohort_videoview(slugify_keywords=slugify_keywords))
            if self.engaged_time:
                query_list.append(
                    self.__create_cohort_engaged_time(slugify_keywords=slugify_keywords))
            if self.engaged_completion:
                query_list.append(
                    self.__create_cohort_engaged_completion(slugify_keywords=slugify_keywords))
            if self.link_click:
                query_list.append(
                    self.__create_cohort_link_click(slugify_keywords=slugify_keywords))

        if self.slot_click:
            query_list.append(self.__create_cohort_slot_click())

        if self.segments:
            query_list = query_list + self.__create_cohort_transition()

        if self.second_party_segments:
            query_list = query_list+self.__create_second_party_segments()

        query = {
            'or': query_list
        }
        if self.domains is not None:
            query = {'and': [query, self.__create_cohort_domains()]}

        return query

    def merge(self, wrap_query: 'Query'):
        if wrap_query.segments:
            if self.segments is None:
                self.segments = wrap_query.segments
            else:
                self.segments = ListHelper.merge_list(
                    self.segments, wrap_query.segments)
        if wrap_query.accurate_segments:
            if self.accurate_segments is None:
                self.accurate_segments = wrap_query.accurate_segments
            else:
                self.accurate_segments = ListHelper.merge_list(
                    self.accurate_segments, wrap_query.accurate_segments)
        if wrap_query.volume_segments:
            if self.volume_segments is None:
                self.volume_segments = wrap_query.volume_segments
            else:
                self.volume_segments = ListHelper.merge_list(
                    self.volume_segments, wrap_query.volume_segments)
        if wrap_query.obsidian_segments:
            if self.obsidian_segments is None:
                self.obsidian_segments = wrap_query.obsidian_segments
            else:
                self.obsidian_segments = ListHelper.merge_list(
                    self.obsidian_segments, wrap_query.obsidian_segments)
        if wrap_query.keywords:
            if self.keywords is None:
                self.keywords = wrap_query.keywords
            else:
                self.keywords = ListHelper.merge_list(
                    self.keywords, wrap_query.keywords)
        if wrap_query.taxonomy:
            if self.taxonomy is None:
                self.taxonomy = wrap_query.taxonomy
            else:
                self.taxonomy = ListHelper.merge_list(
                    self.taxonomy, wrap_query.taxonomy)
        if wrap_query.urls:
            if not self.urls:
                self.urls = wrap_query.urls
            else:
                self.urls = ListHelper.merge_list(
                    self.urls, wrap_query.urls)

        if wrap_query.second_party_segments:
            if self.second_party_segments:
                self.second_party_segments = ListHelper.merge_list(
                    self.second_party_segments, wrap_query.second_party_segments)
            else:
                self.second_party_segments = wrap_query.second_party_segments

        if wrap_query.third_party_segments:
            if self.third_party_segments:
                self.third_party_segments = ListHelper.merge_list(
                    self.third_party_segments, wrap_query.third_party_segments)
            else:
                self.third_party_segments = wrap_query.third_party_segments

    def to_json(self, filepath):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: str) -> 'Query':
        definition = FileHelper.from_json(filepath)
        return Query(**definition)

# region permutive query dict

    def __create_cohort_query_clickers(self) -> Dict:
        query_list = []
        query_list.append(self.__create_cohort_slot_click())

        query = {
            'or': query_list
        }

        return query

    def __create_cohort_pageview(self, slugify_keywords: Optional[List[str]] = None) -> Dict:

        conditions = []
        if slugify_keywords is None:
            slugify_keywords = []
        contains = []
        if self.keywords is not None:
            for keyword in self.keywords:
                if " " in keyword or "-" in keyword or len(keyword) > 7:
                    contains.append(keyword)
                else:
                    contains.append(f' {keyword} ')

            conditions.append({
                'condition': {
                    'contains': contains
                },
                'property': 'properties.article.title'})
            conditions.append({
                'condition': {
                    'contains': contains
                },
                'property': 'properties.article.description'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.category'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.subcategory'})
            conditions.append({
                'condition': {
                    'list_contains': self.keywords
                },
                'property': 'properties.article.tags'})

        if self.taxonomy is not None:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})
        if self.obsidian_id is not None:
            conditions.append({
                'condition': {
                    'list_contains': [self.obsidian_id.upper()]
                },
                'property': 'properties.context.sg'})
        if (self.urls is not None) or (self.keywords is not None):
            urls_list = []
            if self.urls is not None:
                urls_list = self.urls.copy()
            if self.keywords is not None:
                urls_list = urls_list + slugify_keywords
            urls_list = ListHelper.merge_list(urls_list)
            urls_list.sort()

            conditions.append({
                'condition': {
                    'contains': urls_list
                },
                'property': 'properties.client.url'})
        if self.during_value is not None and self.during_value > 0:

            pv = {
                'during': {
                    'the_last': {
                        'unit': 'days',
                                'value': self.during_value
                    }
                },
                'event': 'Pageview',
                'frequency': {
                    'greater_than_or_equal_to': self.frequency_value
                },
                'where': {
                    'or': conditions
                }
            }
        else:
            pv = {
                'event': 'Pageview',
                'frequency': {
                    'greater_than_or_equal_to': self.frequency_value
                },
                'where': {
                    'or': conditions
                }
            }
        return pv

    def __create_cohort_videoview(self, slugify_keywords: Optional[List[str]] = None) -> Dict:

        conditions = []
        if slugify_keywords is None:
            slugify_keywords = []
        contains = []
        if self.keywords is not None:
            for keyword in self.keywords:
                if " " in keyword or "-" in keyword or len(keyword) > 7:
                    contains.append(keyword)
                else:
                    contains.append(f' {keyword} ')

            conditions.append({
                'condition': {
                    'contains': contains
                },
                'property': 'properties.videoTitle'})

        if self.during_value is not None and self.during_value > 0:

            vv = {
                'during': {
                    'the_last': {
                        'unit': 'days',
                                'value': self.during_value
                    }
                },
                'event': 'videoViews',
                'frequency': {
                    'greater_than_or_equal_to': self.frequency_value
                },
                'where': {
                    'or': conditions
                }
            }
        else:
            vv = {
                'event': 'videoViews',
                'frequency': {
                    'greater_than_or_equal_to': self.frequency_value
                },
                'where': {
                    'or': conditions
                }
            }
        return vv

    def __create_cohort_link_click(self,  slugify_keywords: Optional[List[str]] = None, dest_urls: List[str] = ['facebook.com', 'instagram.com', 'pinterest.com'],) -> Dict:
        keyword_slugs = []
        if slugify_keywords is not None:
            keyword_slugs = slugify_keywords
        if self.urls is not None:
            keyword_slugs = list(
                dict.fromkeys(keyword_slugs + self.urls))
        LinkClick = {
            'event': 'LinkClick',
            'frequency': {
                'greater_than_or_equal_to': 1
            },
            'where': {
                'and': [
                    {
                        'condition': {
                            'contains': dest_urls
                        },
                        'property': 'properties.dest_url'
                    },
                    {
                        'condition': {
                            'contains': keyword_slugs
                        },
                        'property': 'properties.client.url'
                    }
                ]
            }
        }
        return LinkClick

    def __create_cohort_engaged_time(self,  slugify_keywords: Optional[List[str]] = None) -> Dict:

        conditions = []
        if self.keywords is not None:
            conditions.append({
                'condition': {
                    'contains': [f' {keyword} ' for keyword in self.keywords]
                },
                'property': 'properties.article.title'})
            conditions.append({
                'condition': {
                    'contains': [f' {keyword} ' for keyword in self.keywords]
                },
                'property': 'properties.article.description'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.category'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.subcategory'})
            conditions.append({
                'condition': {
                    'list_contains': self.keywords
                },
                'property': 'properties.article.tags'})

        if self.taxonomy is not None:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})
        if self.obsidian_id is not None:
            conditions.append({
                'condition': {
                    'list_contains': [self.obsidian_id.upper()]
                },
                'property': 'properties.context.sg'})
        if (self.urls is not None) or (self.keywords is not None):
            urls_list = []
            if (self.urls is not None):
                urls_list = self.urls.copy()
            if (slugify_keywords is not None):
                urls_list = urls_list + slugify_keywords
            urls_list = ListHelper.merge_list(urls_list)
            urls_list.sort()

            conditions.append({
                'condition': {
                    'contains': urls_list
                },
                'property': 'properties.client.url'})

        engaged_time = {'engaged_time': {
            'seconds': {'greater_than': 30},
            'where': {'or': conditions}}
        }
        return engaged_time

    def __create_cohort_engaged_completion(self,  slugify_keywords: Optional[List[str]] = None) -> Dict:

        conditions = []
        if self.keywords is None and self.taxonomy is None and self.obsidian_id is None and self.urls is None:
            raise ValueError(
                'self.keywords is None and self.taxonomy is None and self.obsidian_id is None and self.urls is None')
        if self.keywords is not None:
            conditions.append({
                'condition': {
                    'contains': [f' {keyword} ' for keyword in self.keywords]
                },
                'property': 'properties.article.title'})
            conditions.append({
                'condition': {
                    'contains': [f' {keyword} ' for keyword in self.keywords]
                },
                'property': 'properties.article.description'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.category'})
            conditions.append({
                'condition': {
                    'equal_to': self.keywords
                },
                'property': 'properties.article.subcategory'})
            conditions.append({
                'condition': {
                    'list_contains': self.keywords
                },
                'property': 'properties.article.tags'})

        if self.taxonomy is not None:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})
        if self.obsidian_id is not None:
            conditions.append({
                'condition': {
                    'list_contains': [self.obsidian_id.upper()]
                },
                'property': 'properties.context.sg'})
        if self.urls is not None or self.keywords is not None:
            urls_list = []
            if self.urls is not None:
                urls_list = self.urls.copy()
            if slugify_keywords is not None:
                urls_list = urls_list + slugify_keywords
            urls_list = ListHelper.merge_list(urls_list)
            urls_list.sort()

            conditions.append({
                'condition': {
                    'contains': urls_list
                },
                'property': 'properties.client.url'})

        engaged_completion = {'engaged_completion':
                              {'completion': {'greater_than': 0.6},
                               'where': {'or': conditions}}}
        return engaged_completion

    def __create_cohort_slot_click(self) -> Dict:
        if self.segments is None:
            segments_list = []
        else:
            segments_list = self.segments.copy()

        segments_list.append(str(self.number))
        slot_click = {
            'event': 'GamLogSlotClicked',
            'frequency': {
                'greater_than_or_equal_to': 1
            },
            'where': {
                'condition': {
                    'condition': {
                        'equal_to': 'permutive'
                    },
                    'function': 'any',
                    'property': 'key',
                    'where': {
                        'condition': {
                                'list_contains': [str(segment) for segment in segments_list]
                        },
                        'property': 'value'
                    }
                },
                'property': 'properties.slot.targeting_keys'
            }
        }
        return slot_click

    def __create_cohort_transition(self) -> List[Dict]:
        transitions = []
        if self.segments is None:
            raise ValueError('self.segments is None')
        for segment in self.segments:
            segment_int = None
            if isinstance(segment, str):
                try:
                    segment_int = int(segment)
                except ValueError:
                    # If the value cannot be converted to an integer, skip to the next iteration
                    continue
            elif isinstance(segment, int):
                segment_int = segment

            transition = {
                'has_entered': {
                    'during': {
                        'the_last': {
                            'unit': 'days',
                                    'value': 30
                        }
                    },
                    'segment': segment_int
                }
            }
            if segment_int is not None:
                transitions.append(transition)
        return transitions

    def __create_second_party_segments(self) -> List[Dict]:
        second_party_condition = []
        if self.second_party_segments is None:
            raise ValueError('self.second_party_segments is None')
        for segment in self.second_party_segments:

            second_party = {"in_second_party_segment": {
                "provider": segment[0],
                "segment": segment[1]
            }}
            second_party_condition.append(second_party)
        return second_party_condition

    def __create_cohort_domains(self) -> Dict:
        domain_condition = {
            'event': 'Pageview',
            'frequency': {
                'greater_than_or_equal_to': 1
            },
            'where': {
                'condition': {
                    'contains': self.domains
                },
                'property': 'properties.client.domain'
            }
        }
        return domain_condition
# endregion

    @ staticmethod
    def slugify_keywords(keywords: List[str]) -> List[str]:
        logging.info("slugify_keywords")
        new_list = []
        for keyword in keywords:
            if isinstance(keyword, str):
                if len(keyword) > 0:
                    if keyword[0] != '/':
                        keyword_verso = keyword.strip()
                        keyword_verso = keyword_verso.lower()
                        keyword_verso = keyword_verso.replace('  ', ' ')
                        brut = urllib.parse.quote(keyword_verso)
                        if brut[0] != '-':
                            brut = '-'+brut
                        if brut[-1] != '-':
                            brut = brut+'-'
                        new_list.append(brut)
                        keyword_verso = keyword_verso.replace(' ', '-')
                        for items_in in ITEMS:
                            keyword_verso = keyword_verso.replace(
                                items_in, ITEMS[items_in])
                        keyword_verso = urllib.parse.quote(keyword_verso)
                        if keyword_verso[0] != '-':
                            keyword_verso = '-'+keyword_verso
                        if keyword_verso[-1] != '-':
                            keyword_verso = keyword_verso+'-'
                        new_list.append(keyword_verso)

        return ListHelper.merge_list(new_list)


@dataclass
class QueryList(List[Query]):
    def to_dataframe(self):
        query_list = []
        for definition in self:
            query_list.append(Query(**definition))  # type: ignore
        return pd.DataFrame(query_list)

    def __init__(self, queries: Optional[List[Query]]):
        if queries is not None:
            super().__init__(queries)

    @staticmethod
    def from_file(filepath: Optional[str] = None) -> 'QueryList':
        folder_name = "query"
        query_list = []
        files = List[str]
        if filepath is not None:
            files = [filepath]
        elif os.environ.get("DATA_PATH") is not None:
            files = glob.glob(
                f'{os.environ.get("DATA_PATH")}{folder_name}/*.json')
            files.sort()
        else:
            files = None
        if files is not None:
            for file_path in files:
                if file_path is not None and FileHelper.file_exists(file_path):
                    definitions = FileHelper.from_json(file_path)
                    if not isinstance(definitions, List):
                        definitions = [definitions]
                    for definition in definitions:
                        if definition.get("market", "CN") is not None:
                            query_list.append(Query(**definition))
            return QueryList(query_list)
        else:
            raise ValueError(f"No file")

    def to_query(self):
        query_dict = {}
        for query in self:

            if query.cohort_global is not None:
                if not hasattr(query_dict, query.cohort_global):
                    query_dict[query.cohort_global] = Query(
                        name=f"CN | {query.cohort_global} | Seed",
                        market="CN",
                        cohort_global=query.cohort_global,
                        segments=[],
                        accurate_segments=[],
                        volume_segments=[], obsidian_segments=[])
                cohort = query_dict[query.cohort_global]
                # segments
                children = []
                if query.number is not None:
                    cohort.segments = ListHelper.merge_list(cohort.segments,
                                                            str(query.number))
                    if isinstance(query.segments, list):
                        cohort.segments = ListHelper.merge_list(cohort.segments,
                                                                query.segments)

                        children = [
                            {"accurate_id": child.accurate_id, "volume_id": child.volume_id, "obsidian_id": child.obsidian_id} for child in self if str(child.number) in query.segments]
                        if len(children) > 0:
                            child_accurate = [child["accurate_id"] for child in children if child.get(
                                "accurate_id", None) is not None]
                            cohort.accurate_segments = ListHelper.merge_list(cohort.accurate_segments,
                                                                             [child["accurate_id"] for child in children if child.get("accurate_id", None) is not None])
                            cohort.volume_segments = ListHelper.merge_list(cohort.volume_segments,
                                                                           [child["volume_id"] for child in children if child.get("volume_id", None) is not None])
                            cohort.obsidian_segments = ListHelper.merge_list(cohort.obsidian_segments,
                                                                             [child["obsidian_id"] for child in children if child.get("obsidian_id", None) is not None])
                # accurate_segments
                if query.accurate_id is not None:
                    cohort.accurate_segments = ListHelper.merge_list(cohort.accurate_segments, [
                        query.accurate_id])

                # volume_segments
                if query.volume_id is not None:
                    cohort.volume_segments = ListHelper.merge_list(cohort.volume_segments, [
                        query.volume_id])

                if query.obsidian_id is not None:
                    cohort.obsidian_segments = ListHelper.merge_list(cohort.obsidian_segments, [
                        query.obsidian_id])

                if len(cohort.segments) == 0:
                    cohort.segments = None

                if len(cohort.accurate_segments) == 0:
                    cohort.accurate_segments = None

                if len(cohort.volume_segments) == 0:
                    cohort.volume_segments = None

                if len(cohort.obsidian_segments) == 0:
                    cohort.obsidian_segments = None

        return [query_dict[cohort_name]
                for cohort_name in query_dict]

    def to_file(self, filepath: str):
        FileHelper.to_json(self, filepath)

    def filter_by_workspace_id(self, workspace_id: str) -> 'QueryList':
        filtered_queries = [
            query for query in self if query.workspace_id == workspace_id]
        return QueryList(filtered_queries)

    @staticmethod
    def sync_definitions(workspaceList: 'WorkspaceList', file: Optional[str] = None,
                         new_tags=TAGS):
        folder_name = "query"
        if file is not None:
            files = [file]
        else:
            files = glob.glob(
                pathname=f"{os.environ.get('DATA_PATH')}{folder_name}/*.json")
        files.sort()
        for filepath in files:
            logging.info(filepath)
            definitions = FileHelper.from_json(filepath)
            definition_index = 0
            if not isinstance(definitions, List):
                definitions = [definitions]
            for definition in definitions:
                query = Query(**definition)
                if query.workspace_id:
                    api_key = None
                    query_ws = workspaceList.get_by_id(query.workspace_id)
                    if query_ws:
                        api_key = query_ws.privateKey
                        query.sync(api_key=api_key,
                                   new_tags=new_tags)
                    definitions[definition_index] = query
                    definition_index = definition_index + 1
            definitions = sorted(definitions, key=lambda d: d["name"])
            FileHelper.to_json(definitions, filepath)

    @staticmethod
    def sync_clicks(workspaceList: 'WorkspaceList',
                    file: Optional[str] = None,
                    new_tags=TAGS):
        folder_name = "query"
        if file is not None:
            files = [file]
        else:
            files = glob.glob(
                pathname=f"{os.environ.get('DATA_PATH')}{folder_name}/*.json")
        files.sort()
        for filepath in files:
            logging.info(filepath)
            definitions = FileHelper.from_json(filepath)
            if not isinstance(definitions, List):
                definitions = [definitions]
            for definition in definitions:
                query = Query(**definition)
                if query.workspace_id:
                    api_key = workspaceList.get_by_id(query.workspace_id)
                    if api_key is not None:
                        query.sync_clickers(api_key=api_key,
                                            new_tags=ListHelper.merge_list(new_tags, TAGS))


@dataclass
class Workspace(FileHelper):
    """
    Dataclass for the Workspace entity in the Permutive ecosystem.
    """
    name: str
    organizationID: str
    workspaceID: str
    privateKey: str

    @property
    def isTopLevel(self):
        if self.organizationID == self.workspaceID:
            return True
        return False

    @property
    def CohortAPI(self):
        return CohortAPI(self.privateKey)

    @property
    def AudienceAPI(self):
        return AudienceAPI(self.privateKey)

    @property
    def UserAPI(self):
        return UserAPI(self.privateKey)

    def sync_imports_cohorts(self,
                             import_detail: 'AudienceAPI.Import',
                             prefix: Optional[str] = None,
                             inheritance: bool = False,
                             masterKey: Optional[str] = None):
        cohorts_list = self.CohortAPI.list(include_child_workspaces=True)
        for import_detail in self.AudienceAPI.list_imports():
            if (inheritance and import_detail.inheritance) or (not inheritance and not import_detail.inheritance):
                self.sync_import_cohorts(import_detail=import_detail,
                                         prefix=prefix,
                                         cohorts_list=cohorts_list,
                                         masterKey=masterKey)

    def sync_import_cohorts(self,
                            import_detail: 'AudienceAPI.Import',
                            prefix: Optional[str] = None,
                            cohorts_list: Optional[List['CohortAPI.Cohort']] = None,
                            masterKey: Optional[str] = None):
        import_segments = self.AudienceAPI.list_segments(
            import_id=import_detail.id)
        if len(import_segments) == 0:
            return
        if not cohorts_list:
            cohorts_list = self.CohortAPI.list(include_child_workspaces=True)
        api_key = masterKey if masterKey is not None else self.privateKey
        q_provider_segments = Query(name=f"{prefix or ''}{import_detail.name}",
                                    tags=[import_detail.name,
                                          'automatic', 'imports'],
                                    second_party_segments=[])
        q_provider_segments.id = next((cohort.id for cohort in self.CohortAPI.list(
        ) if cohort.name == q_provider_segments.name), None)
        cohort_tags = next((cohort.tags for cohort in self.CohortAPI.list(
        ) if cohort.name == q_provider_segments.name), None)
        if q_provider_segments.tags:
            q_provider_segments.tags = ListHelper.merge_list(
                q_provider_segments.tags, cohort_tags)
        else:
            q_provider_segments.tags = cohort_tags
        for import_segment in import_segments:
            logging.info(
                f"AudienceAPI::sync_cohort::{import_detail.name}::{import_segment.name}")
            t_segment = (import_detail.code, import_segment.code)

            q_segment = Query(name=f"{prefix or ''}{import_detail.name} | {import_segment.name}",
                              description=f'{import_detail.name} ({import_detail.id}) : {import_segment.code} : {import_segment.name} ({import_segment.id})',
                              tags=[import_detail.name,
                                    '#automatic', '#imports'],
                              second_party_segments=[t_segment],
                              workspace_id=self.workspaceID)
            q_segment.id = next(
                (cohort.id for cohort in cohorts_list if cohort.name == q_segment.name), None)

            if q_segment.id:
                cohort_tags = next(
                    (cohort.tags for cohort in cohorts_list if cohort.id == q_segment.id), None)
            if q_segment.tags:
                q_segment.tags = ListHelper.merge_list(
                    q_segment.tags, cohort_tags)
            else:
                q_segment.tags = cohort_tags
            q_segment.sync(api_key=api_key)
            if not q_provider_segments.second_party_segments:
                q_provider_segments.second_party_segments = []
            q_provider_segments.second_party_segments.append(t_segment)
        q_provider_segments.sync(api_key=api_key)



    def sync_imports_segments(self):
        cohorts_list = self.CohortAPI.list(include_child_workspaces=True)
        for item in self.AudienceAPI.list_imports():
            self.sync_import_cohorts(import_detail=item,
                                     prefix=f"{self.name} | Import | ",
                                     cohorts_list=cohorts_list)


@dataclass
class WorkspaceList(List[Workspace]):

    def __init__(self, workspaces: Optional[List[Workspace]] = None):
        super().__init__(workspaces if workspaces is not None else [])
        self._id_map = {workspace.workspaceID: workspace for workspace in self}
        self._name_map = {workspace.name: workspace for workspace in self}

    def get_by_id(self, workspaceID: str) -> Optional[Workspace]:
        return self._id_map.get(workspaceID, None)

    def get_by_name(self, name: str) -> Optional[Workspace]:
        return self._name_map.get(name, None)

    def sync_imports_segments(self):
        for ws in self:
            ws.sync_imports_segments()

    @property
    def Masterworkspace(self) -> Workspace:
        for workspace in self:
            if workspace.isTopLevel:
                return workspace
        raise ValueError("No Top WS")
    
    def from_description(self):
        cohorts_list = self.Masterworkspace.CohortAPI.list(include_child_workspaces=True)
        for cohort in cohorts_list:
            if cohort.tags:
                if "from_description" in cohort.tags and cohort.description:
                    keywords = cohort.description.split(",")
                    query = Query(name=cohort.name, id=cohort.id,
                                  keywords=keywords)
                    query.sync(self.Masterworkspace.privateKey)

    def to_json(self, filepath: str):
        FileHelper.to_json(self, filepath=filepath)

    @staticmethod
    def from_json(filepath: Optional[str] = None) -> 'WorkspaceList':
        if filepath is None:
            filepath = os.environ.get("PERMUTIVE_APPLICATION_CREDENTIALS")
        if filepath is None:
            raise ValueError(
                'Unable to get PERMUTIVE_APPLICATION_CREDENTIALS from .env')

        workspace_list = FileHelper.from_json(filepath)
        if not isinstance(workspace_list, list):
            raise TypeError("Expected a list of workspaces from the JSON file")

        return WorkspaceList([Workspace(**workspace) for workspace in workspace_list])


class CohortAPI(APIRequestHandler):
    COHORT_API_VERSION = 'v2'

    @dataclass
    class Cohort(FileHelper):
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

    def __init__(self,
                 api_key
                 ) -> None:
        super().__init__(api_key=api_key,
                         api_endpoint=f'https://api.permutive.app/cohorts-api/{self.COHORT_API_VERSION}/cohorts/',
                         payload_keys=["name", "query", "description", "tags"])

    def list(self,
             include_child_workspaces=False) -> List['Cohort']:
        """
            Fetches all cohorts from the API.

            :return: List of all cohorts.
        """
        logging.info(f"CohortAPI::list")
        url = self.api_endpoint
        if include_child_workspaces:
            url = f"{self.gen_url(url)}include-child-workspaces=true"
        response = self.getRequest(url)
        return [CohortAPI.Cohort(**cohort) for cohort in response.json()]

    def get(self,
            cohort_id: str) -> 'Cohort':
        """
        Fetches a specific cohort from the API using its ID.

        :param cohort_id: ID of the cohort.
        :return: Cohort object or None if not found.
        """
        logging.info(f"CohortAPI::get::{cohort_id}")
        url = f"{self.api_endpoint}{cohort_id}"
        response = self.getRequest(url)

        return CohortAPI.Cohort(**response.json())

    def get_by_name(self,
                    cohort_name: str
                    ) -> Optional['Cohort']:
        '''
            Object Oriented Permutive Cohort seqrch
            :rtype: Cohort object
            :param cohort_name: str Cohort Name. Required
            :return: Cohort object
        '''
        logging.info(f"CohortAPI::get_by_name::{cohort_name}")

        for cohort in self.list(include_child_workspaces=True):
            if cohort_name == cohort.name and cohort.id is not None:
                return self.get(cohort_id=cohort.id)

    def get_by_code(self,
                    cohort_code: Union[int, str]) -> Optional['Cohort']:
        '''
        Object Oriented Permutive Cohort seqrch
        :rtype: Cohort object
        :param cohort_code: Union[int, str] Cohort Code. Required
        :return: Cohort object
        '''
        if type(cohort_code) == str:
            cohort_code = int(cohort_code)
        logging.info(f"CohortAPI::get_by_code::{cohort_code}")
        for cohort in self.list(include_child_workspaces=True):
            if cohort_code == cohort.code and cohort.id is not None:
                return self.get(cohort_id=cohort.id)

    def create(self,
               cohort: 'Cohort') -> 'Cohort':
        """
        Creates a new cohort.

        :param cohort: Cohort to be created.
        :return: Created cohort object.
        """
        if cohort.name is None:
            raise ValueError('name must be specified')
        if cohort.query is None:
            raise ValueError('query must be specified')
        logging.info(f"CohortAPI::create::{cohort.name}")

        url = f"{self.api_endpoint}"
        response = self.postRequest(url=url,
                                    data=self.to_payload(cohort))

        return CohortAPI.Cohort(**response.json())

    def update(self,
               cohort: 'Cohort') -> 'Cohort':
        """
        Updates an existing cohort.

        :param cohort_id: ID of the cohort to be updated.
        :param updated_cohort: Updated cohort data.
        :return: Updated cohort object.
        """
        logging.info(f"CohortAPI::update::{cohort.name}")
        if cohort.id is None:
            raise ValueError('id must be specified')
        url = f"{self.api_endpoint}{cohort.id}"

        response = self.patchRequest(url=url,
                                     data=self.to_payload(cohort))

        return CohortAPI.Cohort(**response.json())

    def delete(self,
               cohort: 'Cohort') -> None:
        """
        Deletes a specific cohort.
        :param cohort_id: ID of the cohort to be deleted.
        :return: None
        """
        logging.info(f"CohortAPI::delete::{cohort.id}")
        url = f"{self.api_endpoint}{cohort.id}"
        self.deleteRequest(url=url)

    def copy(self, cohort: 'Cohort', k2: Optional[str] = None) -> 'Cohort':
        """
        Meant for copying a cohort
        :param cohort_id: str the cohort's id to duplicat. Required
        :param k2: str the key to use for creating the copy. Optional. If not specified, uses the current workspace API key
        :return: Response
        :rtype: Response
        """
        logging.info(f"CohortAPI::copy")
        if not cohort.id:
            raise ValueError("cohort.id is None")
        new_cohort = self.get(cohort_id=cohort.id)
        if not new_cohort:
            raise ValueError(f"cohort::{cohort.id} does not exist")

        new_cohort.id = None
        new_cohort.code = None
        new_cohort.name = new_cohort.name + ' (copy)'
        new_description = "Copy of " + cohort.id
        if new_cohort.description is not None:
            new_cohort.description = new_cohort.description + new_description
        else:
            new_cohort.description = new_description
        if k2:
            return CohortAPI(api_key=k2).create(cohort=new_cohort)
        return self.create(cohort=new_cohort)


class AudienceAPI(APIRequestHandler):
    AUDIENCE_API_VERSION = 'v1'

    @dataclass
    class Import(FileHelper):
        """
        Dataclass for the Import in the Permutive ecosystem.
        """
        id: str
        name: str
        code: str
        relation: str
        identifiers: List[str]
        description: Optional[str] = None
        source: Optional['Source'] = None
        inheritance: Optional[str] = None
        segments: Optional[List['AudienceAPI.Segment']] = None
        updated_at: Optional[datetime] = datetime.now()

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

    @dataclass
    class Segment(FileHelper):
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

    def __init__(self,
                 api_key
                 ) -> None:
        super().__init__(api_key=api_key,
                         api_endpoint=f'https://api.permutive.app/audience-api/{self.AUDIENCE_API_VERSION}/imports',
                         payload_keys=['name', 'code', 'description', 'cpm', 'categories'])

    def list_imports(self) -> List['Import']:
        """
        Fetches all imports from the API.

        :return: List of all imports.
        """
        logging.info(f"AudienceAPI::list_imports")
        url = self.api_endpoint
        response = self.getRequest(url)
        imports = response.json()
        return [AudienceAPI.Import(**item) for item in imports['items']]

    def get_import(self,
                   import_id: str) -> 'Import':
        """
        Fetches a specific import by its id.

        :param import_id: ID of the import.
        :return: The requested Importt.
        """
        logging.info(f"AudienceAPI::get_import::{import_id}")
        url = f"{self.api_endpoint}/{import_id}"
        response = self.getRequest(url=url)
        if response is None:
            raise ValueError('Unable to get_import')
        return AudienceAPI.Import(**response.json())

    def create_segment(self, segment: 'Segment') -> 'Segment':
        """
        Creates a new segment

        :return: The created Segment.
        """
        logging.info(
            f"AudienceAPI::create_segment::{segment.import_id}::{segment.name}")
        url = f"{self.api_endpoint}/{segment.import_id}/segments"
        response = self.postRequest(
            url=url, data=self.to_payload(segment))
        if response is None:
            raise ValueError('Unable to create_segment')
        return AudienceAPI.Segment(**response.json())

    def update_segment(self, segment: 'Segment') -> 'Segment':
        """
        PATCH
        https://api.permutive.app/audience-api/v1/imports/{importId}/segments/{segmentId}
        Updates a segment for an import. The segment is identified by its globally unique public ID.
        https://developer.permutive.com/reference/patchimportsimportidsegmentssegmentid
        :param segment: AudienceAPI.Import.Segment to update
        :return: The updated Segment.
        """

        logging.info(
            f"AudienceAPI::update_segment::{segment.import_id}::{segment.name}")
        url = f"{self.api_endpoint}/{segment.import_id}/segments/{segment.id}"
        response = self.patchRequest(
            url=url,  data=self.to_payload(segment))
        if response is None:
            raise ValueError('Unable to update_segment')
        return AudienceAPI.Segment(**response.json())

    def delete_segment(self, segment: 'Segment') -> bool:
        """
        Deletes a specific segment by its id.

        :param import_id: ID of the import.
        :param segment_id: ID of the segment.
        :return: True if deletion was successful, otherwise False.
        """
        logging.info(
            f"AudienceAPI::delete_segment::{segment.import_id:}::{segment.id}")
        url = f"{self.api_endpoint}/{segment.import_id}/segments/{segment.id}"
        response = self.deleteRequest(url=url)
        return response.status_code == 204

    def get_segment(self,
                    import_id: str,
                    segment_id: str) -> 'Segment':
        """
        Fetches a specific segment by its id.
        https://developer.permutive.com/reference/getimportsimportidsegmentssegmentid
        :param import_id: ID of the import.
        :param segment_id: UUID of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_id::{import_id}::{segment_id}")
        url = f"{self.api_endpoint}/{import_id}/segments/{segment_id}"
        response = self.getRequest(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return AudienceAPI.Segment(**response.json())

    def get_segment_by_code(self,
                            import_id: str,
                            segment_code: str) -> 'Segment':
        """
        Fetches a specific segment by its code.
        https://developer.permutive.com/reference/getimportsimportidsegmentscodesegmentcode
        :param import_id: ID of the import.
        :param segment_code: Public code of the segment.
        :return: The requested Segment.
        """
        logging.info(
            f"AudienceAPI::get_segment_by_code::{import_id}::{segment_code}")
        url = f"{self.api_endpoint}/{import_id}/segments/code/{segment_code}"
        response = self.getRequest(url=url)
        if response is None:
            raise ValueError('Unable to get_segment')
        return AudienceAPI.Segment(**response.json())

    def list_segments(self,
                      import_id: str,
                      pagination_token: Optional[str] = None) -> List['Segment']:
        """
        Fetches all segments for a specific import.

        :param import_id: ID of the import.
        :return: List of all segments.
        """
        logging.info(f"AudienceAPI::list_segments::{import_id}")
        url = f"{self.api_endpoint}/{import_id}/segments"
        if pagination_token:
            url = f"{url}&pagination_token={pagination_token}"
        response = self.getRequest(url=url)
        if response is None:
            raise ValueError('Unable to list_segments')
        response_json = response.json()
        segments = [AudienceAPI.Segment(
            **segment) for segment in response_json['elements']]
        next_token = response_json.get(
            'pagination', {}).get('next_token', None)
        if next_token:
            logging.info(
                f"AudienceAPI::list_segments::{import_id}::{next_token}")
            segments += self.list_segments(import_id=import_id,
                                           pagination_token=next_token)
        return segments


class UserAPI(APIRequestHandler):
    USER_API_VERSION = 'v2.0'

    @dataclass
    class Identity:
        """
        Dataclass for the Source entity in the Permutive ecosystem.
        """
        user_id: str
        aliases: List['UserAPI.Identity.Alias']

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
            alias = UserAPI.Identity.Alias(id=alias_id,
                                           tag="uID")
            identity.aliases.append(alias)
        if "email_sha256" not in aliases_name and "uID" in aliases_name:
            tag = "uID"
            alias_id = next(
                (alias.id for alias in identity.aliases if alias.tag == tag), "")
            alias = UserAPI.Identity.Alias(id=alias_id,
                                           tag="email_sha256")
            identity.aliases.append(alias)

        return self.postRequest(
            url=url,
            data=self.to_payload(identity))
