import logging
import json
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, field

from collections import defaultdict
from collections.abc import Iterable

from . import FileHelper, ListHelper
from .Cohort import Cohort


ITEMS = {'ä': 'a',  'â': 'a', 'á': 'a', 'à': 'a', 'ã': 'a', 'ç': 'c', 'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
         'í': 'i',  'ï': 'i', 'ò': 'o', 'ó': 'o', 'õ': 'o', 'ô': 'o', 'ñ': 'n', 'ù': 'u', 'ú': 'u', 'ü': 'u'}


@dataclass
class Query():
    name: str
    workspace: Optional[str] = "CN"
    id: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    urls: Optional[List[str]] = None
    taxonomy: Optional[List[str]] = None
    segments: Optional[List[str]] = None
    second_party_segments: Optional[List[Tuple[str, str]]] = None
    third_party_segments: Optional[List[int]] = None
    domains: Optional[List[str]] = None
    number: Optional[str] = None
    during_value: int = 0
    frequency_value: int = 1
    page_view: bool = True
    link_click: bool = False
    slot_click: bool = False
    engaged_time: bool = False
    engaged_completion: bool = False
    workspace_id: Optional[str] = None
    tags: Optional[List[str]] = None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def sync(self, api_key: str):

        logging.debug('segment: ' + self.name)

        cohort = Cohort(
            name=self.name,
            id=self.id,
            description=self.description,
            query=self.to_permutive_query(),
            tags=self.tags)

        if self.keywords:
            cohort.description = ",".join(self.keywords)
        if self.id:
            cohort.update(privateKey=api_key)
        else:
            cohort.create(privateKey=api_key)
            self.id = cohort.id

    def to_permutive_query(self) -> Dict:
        query_list = []
        if self.keywords or self.taxonomy or self.urls:
            q_PageView = Query.PageView(keywords=self.keywords,
                                        taxonomy=self.taxonomy,
                                        frequency_value=self.frequency_value,
                                        during_value=self.during_value)
            query_list.append(q_PageView.to_query())
            if self.keywords:

                q_VideoView = Query.VideoView(keywords=self.keywords)
                query_list.append(q_VideoView.to_query())

                if self.engaged_time:
                    q = Query.EngagedTimeCondition(keywords=self.keywords)
                    query_list.append(q.to_query())

                if self.engaged_completion:
                    q = Query.EngagedCompletionCondition(
                        keywords=self.keywords)
                    query_list.append(q.to_query())

                if self.link_click:
                    q = Query.LinkClickCondition(self.keywords)
                    query_list.append(q.to_query())

        if self.slot_click:
            segments_list = []
            if self.segments:
                segments_list += self.segments
            if self.number:
                segments_list.append(self.number)
            if len(segments_list) > 0:
                q = Query.SlotClickCondition(values=segments_list)
                query_list.append(q.to_query())

        if self.segments:
            for segment in self.segments:
                q = Query.CohortTransitionCondition(segment=int(segment))
                query_list.append(q.to_query())

        if self.second_party_segments:
            for second_party_segment in self.second_party_segments:
                q = Query.SecondPartyTransitionCondition(provider=second_party_segment[0],
                                                         segment=second_party_segment[1])
                query_list.append(q.to_query())

        query = {
            'or': query_list
        }
        if self.domains:
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
            query = {'and': [query, domain_condition]}

        return query

    def merge(self, second_query: 'Query'):
        if second_query.segments:
            if self.segments:
                self.segments = ListHelper.merge_list(
                    self.segments, second_query.segments)
            else:
                self.segments = second_query.segments

        if second_query.keywords:
            if self.keywords:
                self.keywords = ListHelper.merge_list(
                    self.keywords, second_query.keywords)

            else:
                self.keywords = second_query.keywords
        if second_query.taxonomy:
            if self.taxonomy:
                self.taxonomy = ListHelper.merge_list(
                    self.taxonomy, second_query.taxonomy)

            else:
                self.taxonomy = second_query.taxonomy
        if second_query.urls:
            if self.urls:
                self.urls = ListHelper.merge_list(
                    self.urls, second_query.urls)

            else:
                self.urls = second_query.urls

        if second_query.second_party_segments:
            if self.second_party_segments:
                self.second_party_segments = ListHelper.merge_list(
                    self.second_party_segments, second_query.second_party_segments)
            else:
                self.second_party_segments = second_query.second_party_segments

        if second_query.third_party_segments:
            if self.third_party_segments:
                self.third_party_segments = ListHelper.merge_list(
                    self.third_party_segments, second_query.third_party_segments)
            else:
                self.third_party_segments = second_query.third_party_segments

        if second_query.domains:
            if self.domains:
                self.third_party_segments = ListHelper.merge_list(
                    self.domains, second_query.domains)
            else:
                self.domains = second_query.domains

    @staticmethod
    def values_to_condition(property: str,
                            operator: str,
                            values: List[str]) -> Dict:

        return {'condition': {operator: values},
                'property': property}

    @staticmethod
    def to_article_conditions(keywords: List[str]) -> List[Dict]:
        conditions = []
        contains = []
        for keyword in keywords:
            if " " in keyword or "-" in keyword or len(keyword) > 7:
                contains.append(keyword)
            else:
                contains.append(f' {keyword} ')
        for property in ['properties.article.title', 'properties.article.description']:
            conditions.append(Query.values_to_condition(property=property,
                                                        operator='contains',
                                                        values=contains))

        for property in ['properties.article.category', 'properties.article.subcategory']:
            conditions.append(Query.values_to_condition(property=property,
                                                        operator='equal_to',
                                                        values=keywords))

        conditions.append(Query.values_to_condition(property='properties.article.tags',
                                                    operator='list_contains',
                                                    values=keywords))

        return conditions

    def to_json(self, filepath: str):
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f,
                      ensure_ascii=False, indent=4, default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'Query':
        if not FileHelper.file_exists(filepath):
            raise ValueError(f'{filepath} does not exist')
        with open(file=filepath, mode='r') as json_file:
            return Query(**json.load(json_file))

    @dataclass
    class PageView:
        keywords: Optional[List[str]] = None
        taxonomy: Optional[List[str]] = None
        urls: Optional[List[str]] = None
        keyword_slugs: Optional[List[str]] = None
        frequency_value: int = 1
        frequency_operator: str = "greater_than_or_equal_to"
        during_value: int = 90
        during_the_last_unit: str = 'days'

        def to_query(self) -> Dict:
            conditions = []
            if self.keywords:
                conditions = Query.to_article_conditions(self.keywords)
            if self.taxonomy:
                conditions.append({
                    'condition': {
                        'list_contains': self.taxonomy
                    },
                    'property': 'properties.classifications_watson.taxonomy_labels'})
            if self.urls or self.keyword_slugs:
                urls_list = []
                if self.urls:
                    urls_list = self.urls.copy()
                if self.keyword_slugs:
                    urls_list = ListHelper.merge_list(
                        urls_list, self.keyword_slugs)
                urls_list = ListHelper.merge_list(urls_list)
                urls_list.sort()

                conditions.append({
                    'condition': {
                        'contains': urls_list
                    },
                    'property': 'properties.client.url'})

            page_view_query = {
                'event': 'Pageview',
                'frequency': {
                    self.frequency_operator: self.frequency_value
                },
                'where': {
                    'or': conditions
                }
            }

            if self.during_value > 0:
                page_view_query['during'] = {
                    'the_last': {
                        'unit': self.during_the_last_unit,
                        'value': self.during_value
                    }
                }

            return page_view_query

    @dataclass
    class VideoView:
        keywords: List[str]
        frequency_operator: str = 'greater_than_or_equal_to'
        frequency_value: int = 1

        during_the_last_value: int = 0
        during_the_last_unit: str = 'day'

        def to_query(self) -> Dict:
            condition_dict = []
            contains = []
            for keyword in self.keywords:
                if " " in keyword or "-" in keyword or len(keyword) > 7:
                    contains.append(keyword)
                else:
                    contains.append(f' {keyword} ')
            condition_dict = Query.values_to_condition(property='properties.videoTitle',
                                                       operator='contains',
                                                       values=contains)

            video_view_query = {
                'event': 'videoViews',
                'frequency': {
                    self.frequency_operator: self.frequency_value
                },
                'where': {
                    'or': [condition_dict]
                }
            }

            if self.during_the_last_value > 0:
                video_view_query['during'] = {
                    'the_last': {
                        'unit': self.during_the_last_unit,
                        'value': self.during_the_last_value
                    }
                }

            return video_view_query

    @dataclass
    class EngagedTimeCondition:
        keywords: List[str]
        operator: str = 'greater_than_or_equal_to'
        value: float = 30

        def to_query(self) -> Dict:
            conditions = Query.to_article_conditions(keywords=self.keywords)
            engaged_time = {'engaged_time': {
                'seconds': {self.operator: self.value},
                'where': {'or': conditions}}
            }
            return engaged_time

    @dataclass
    class EngagedCompletionCondition:
        keywords: List[str]
        operator: str = 'greater_than_or_equal_to'
        value: float = 0.6

        def to_query(self) -> Dict:
            conditions = Query.to_article_conditions(keywords=self.keywords)
            engaged_completion = {'engaged_completion':
                                  {'completion': {self.operator: self.value},
                                   'where': {'or': conditions}}}
            return engaged_completion

    @dataclass
    class LinkClickCondition:
        keywords: List[str]
        dest_urls: List[str] = field(default_factory=lambda: ['facebook.com',
                                                              'instagram.com',
                                                              'pinterest.com'])
        operator: str = 'greater_than_or_equal_to'
        frequency: int = 1

        def to_query(self) -> Dict:
            LinkClick = {
                'event': 'LinkClick',
                'frequency': {
                    self.operator: self.frequency
                },
                'where': {
                    'and': [Query.values_to_condition(property='properties.dest_url', operator='contains', values=self.dest_urls),
                            Query.values_to_condition(property='properties.client.url', operator='contains', values=self.keywords)]
                }
            }
            return LinkClick

    @dataclass
    class SlotClickCondition:
        values: List[Union[int, str]]
        key_name: str = 'permutive'
        operator: str = 'equal_to'
        frequency: int = 1

        def to_query(self) -> Dict[str, Any]:
            slot_click = {
                'event': 'GamLogSlotClicked',
                'frequency': {
                    self.operator: self.frequency
                },
                'where': {
                    'condition': {
                        'condition': {
                            'equal_to': self.key_name
                        },
                        'function': 'any',
                        'property': 'key',
                        'where': Query.values_to_condition(property='value', operator='list_contains', values=[str(value) for value in self.values])
                    },
                    'property': 'properties.slot.targeting_keys'
                }
            }
            return slot_click

    @dataclass
    class CohortTransitionCondition:
        segment: int
        transition_type: str = 'has_entered'
        unit: str = 'days'
        value: int = 0

        def to_query(self) -> Dict[str, Any]:
            condition = {'segment': self.segment}
            if self.value > 0:
                condition['during'] = {   # type: ignore
                    'the_last': {
                        'unit': self.unit,
                        'value': self.value
                    }
                }

            return {self.transition_type: condition}

    @dataclass
    class SecondPartyTransitionCondition:
        provider: str
        segment: str

        def to_query(self) -> Dict[str, Any]:
            return {
                "in_second_party_segment":
                    {'provider': self.provider,
                     'segment': self.segment
                     }
            }

    @dataclass
    class ThirdPartyTransitionCondition:
        @dataclass
        class ThirdPartySegment:
            provider: str
            segment: str

        third_party_segment: ThirdPartySegment

        def to_query(self) -> Dict[str, Any]:
            return {
                'in_third_party_segment':
                    {'provider': self.third_party_segment.provider,
                     'segment': self.third_party_segment.segment
                     }
            }


@dataclass
class QueryList(List[Query]):
    # Cache for each dictionary to avoid rebuilding
    _id_dictionary_cache: Dict[str, Query] = field(
        default_factory=dict, init=False)
    _name_dictionary_cache: Dict[str, Query] = field(
        default_factory=dict, init=False)
    _tag_dictionary_cache: Dict[str, 'QueryList'] = field(
        default_factory=dict, init=False)
    _workspace_dictionary_cache: Dict[str, 'QueryList'] = field(
        default_factory=dict, init=False)

    def rebuild_cache(self):
        """Rebuilds all caches based on the current state of the list."""
        self._id_dictionary_cache = {
            query.id: query for query in self if query.id}
        self._name_dictionary_cache = {
            query.name: query for query in self if query.name}
        self._tag_dictionary_cache = defaultdict(QueryList)
        self._workspace_dictionary_cache = defaultdict(QueryList)
        for query in self:
            if query.tags:
                for tag in query.tags:
                    self._tag_dictionary_cache[tag].append(query)
            if query.workspace:
                self._workspace_dictionary_cache[query.workspace].append(query)

    def append(self, query: Query):
        """Appends a Query to the list and updates the caches."""
        super().append(query)
        self.rebuild_cache()

    def extend(self, queries: Iterable[Query]):
        """Extends the list with an iterable of Queries and updates the caches."""
        super().extend(queries)
        self.rebuild_cache()

    @property
    def id_dictionary(self) -> Dict[str, Query]:
        # Check if the cache already has the id dictionary
        if not self._id_dictionary_cache:
            self._id_dictionary_cache = {
                query.id: query for query in self if query.id}
        return self._id_dictionary_cache

    @property
    def name_dictionary(self) -> Dict[str, Query]:
        # Check if the cache already has the name dictionary
        if not self._name_dictionary_cache:
            self._name_dictionary_cache = {
                query.name: query for query in self if query.name}
        return self._name_dictionary_cache

    @property
    def tag_dictionary(self) -> Dict[str, 'QueryList']:
        # Check if the cache already has the tag dictionary
        if not self._tag_dictionary_cache:
            r = defaultdict(QueryList)
            for query in self:
                if query.tags:
                    for tag in query.tags:
                        r[tag].append(query)
            self._tag_dictionary_cache = dict(r)
        return self._tag_dictionary_cache

    @property
    def workspace_dictionary(self) -> Dict[str, 'QueryList']:
        # Check if the cache already has the workspace dictionary
        if not self._workspace_dictionary_cache:
            r = defaultdict(QueryList)
            for query in self:
                if query.workspace:
                    r[query.workspace].append(query)
            self._workspace_dictionary_cache = dict(r)
        return self._workspace_dictionary_cache

    def __init__(self, queries: Optional[List[Query]] = None):
        """Initializes the QueryList with an optional list of Query objects."""
        if queries:
            super().__init__(queries)

    def to_json(self, filepath: str):
        """Saves the QueryList to a JSON file at the specified filepath."""
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(self, f, ensure_ascii=False, indent=4,
                      default=FileHelper.json_default)

    @staticmethod
    def from_json(filepath: str) -> 'QueryList':
        """Creates a new QueryList from a JSON file at the specified filepath."""
        query_list = FileHelper.from_json(filepath)
        return QueryList([Query(**query) for query in query_list])
