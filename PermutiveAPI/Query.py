
import glob
import logging
import os
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd


from .CohortAPI import CohortAPI
from .Utils import ListHelper, FileHelper
from .Workspace import WorkspaceList

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

    def from_cohort_description(self, api_key):
        if self.id is not None:
            cohort = CohortAPI(api_key).get(self.id)
            if cohort is not None:
                self.number = cohort.code
                if cohort.description is not None:
                    description_list = [
                        keyword.strip() for keyword in cohort.description.split(",") if len(keyword.strip()) > 0]
                    if len(description_list) > 0:
                        self.keywords = description_list

    def sync_clickers(self, api_key, new_tags: List[str] = TAGS,  override_tags: bool = False):
        if self.name is not None:
            logging.info('segment: ' + self.name)
        cohort_api = CohortAPI(api_key)
        cohort = cohort_api.get_by_name(cohort_name=self.name + " | Clickers")
        if cohort is None:
            cohort = CohortAPI.Cohort(
                name=self.name + " | Clickers", query=self.__create_cohort_query_clickers(), tags=new_tags)
            cohort_api.create(cohort)
        else:
            cohort.query = self.__create_cohort_query_clickers()
            if not override_tags:
                new_tags = ListHelper.merge_list(new_tags, cohort.tags)
            cohort.tags = new_tags
            cohort_api.update(cohort)

    def sync(self, api_key: str, new_tags: List[str] = TAGS,  override_tags: bool = False):
        if self.name is not None:
            logging.info('segment: ' + self.name)

        cohort_api = CohortAPI(api_key)

        cohort = CohortAPI.Cohort(
            name=self.name, id=self.id, query=self.to_query(), tags=new_tags)

        if self.keywords is not None:
            cohort.description = ",".join(self.keywords)
        if self.id is None:
            if cohort.tags is not None:
                cohort.tags = ListHelper.merge_list(cohort.tags, TAGS)
            else:
                cohort.tags = TAGS
            cohort = cohort_api.create(cohort)
            if cohort is not None:
                self.id = cohort.id
                self.number = cohort.code
        else:
            if not override_tags:
                old_cohort = cohort_api.get(self.id)
                if old_cohort is not None:
                    if old_cohort.tags is not None:
                        cohort.tags = ListHelper.merge_list(
                            new_tags+old_cohort.tags)
            cohort = cohort_api.update(cohort)

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
    def sync_definitions(workspaceList: WorkspaceList, file: Optional[str] = None,
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
                    api_key = workspaceList.get_privateKey(
                        query.workspace_id)
                    if api_key is not None:
                        query.sync(api_key=api_key,
                                new_tags=new_tags)
                    definitions[definition_index] = query
                    definition_index = definition_index + 1
            definitions = sorted(definitions, key=lambda d: d["name"])
            FileHelper.to_json(definitions, filepath)

    @staticmethod
    def sync_clicks(workspaceList: WorkspaceList,
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
                    api_key = workspaceList.get_privateKey(
                        query.workspace_id)
                    if api_key is not None:
                        query.sync_clickers(api_key=api_key,
                                            new_tags=ListHelper.merge_list(new_tags, TAGS))
