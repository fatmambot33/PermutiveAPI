import logging
import json
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, field

from collections import defaultdict
from collections.abc import Iterable
import urllib.parse

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
    accurate_id: Optional[str] = None
    volume_id: Optional[str] = None
    keywords: Optional[List[str]] = None
    urls: Optional[List[str]] = None
    taxonomy: Optional[List[str]] = None
    segments: Optional[List[str]] = None
    second_party_segments: Optional[List[Tuple[str, str]]] = None
    third_party_segments: Optional[List[int]] = None
    cohort_wrap: Optional[str] = None
    accurate_segments: Optional[List[str]] = None
    volume_segments: Optional[List[str]] = None
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

    @property
    def keyword_slugs(self) -> Optional[List[str]]:
        new_list = list()
        if self.keywords:
            for keyword in self.keywords:
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
            return ListHelper.merge_list(new_list)  # type: ignore
        return None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def sync_clickers(self, api_key):
        if not self.name:
            raise ValueError("self.name is None")

        logging.debug('segment: ' + self.name)

        cohort = Cohort.get_by_name(privateKey=api_key,
                                    name=self.name + " | Clickers")
        if cohort:
            cohort.query = self.__create_cohort_query_clickers()
            cohort.update(privateKey=api_key)

        else:
            cohort = Cohort(
                name=self.name + " | Clickers", query=self.__create_cohort_query_clickers())
            cohort.create(privateKey=api_key)

    def sync(self, api_key: str):

        logging.debug('segment: ' + self.name)

        cohort = Cohort(
            name=self.name,
            id=self.id,
            description=self.description,
            query=self.to_query(),
            tags=self.tags)

        if self.keywords:
            cohort.description = ",".join(self.keywords)
        if self.id:
            cohort.update(privateKey=api_key)
        else:
            cohort.create(privateKey=api_key)
            self.id = cohort.id

    def to_query2(self) -> Dict:
        query_list = []
        if self.keywords or self.taxonomy or self.urls:
            q_PageView = Query.PageView(keywords=self.keywords,
                                        keyword_slugs=self.keyword_slugs,
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
            query = {'and': [query, self.__create_cohort_domains()]}

        return query

    def to_query(self) -> Dict:
        query_list = []
        slugify_keywords = []
        if self.keywords or self.taxonomy or self.urls:
            if self.keywords:
                slugify_keywords = Query.slugify_keywords(self.keywords)
                query_list.append(self.__create_cohort_pageview(
                    slugify_keywords=slugify_keywords))
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
            for second_party_segment in self.second_party_segments:
                q = Query.SecondPartyTransitionCondition(provider=second_party_segment[0],
                                                         segment=second_party_segment[1])
                query_list.append(q.to_query())

        query = {
            'or': query_list
        }
        if self.domains:
            query = {'and': [query, self.__create_cohort_domains()]}

        return query

    def merge(self, wrap_query: 'Query'):
        if wrap_query.segments:
            if self.segments:
                self.segments = ListHelper.merge_list(
                    self.segments, wrap_query.segments)
            else:
                self.segments = wrap_query.segments

        if wrap_query.accurate_segments:
            if self.accurate_segments:
                self.accurate_segments = ListHelper.merge_list(
                    self.accurate_segments, wrap_query.accurate_segments)
            else:
                self.accurate_segments = wrap_query.accurate_segments

        if wrap_query.volume_segments:
            if self.volume_segments:
                self.volume_segments = ListHelper.merge_list(
                    self.volume_segments, wrap_query.volume_segments)
            else:
                self.volume_segments = wrap_query.volume_segments
        if wrap_query.keywords:
            if self.keywords:
                self.keywords = ListHelper.merge_list(
                    self.keywords, wrap_query.keywords)

            else:
                self.keywords = wrap_query.keywords
        if wrap_query.taxonomy:
            if self.taxonomy:
                self.taxonomy = ListHelper.merge_list(
                    self.taxonomy, wrap_query.taxonomy)

            else:
                self.taxonomy = wrap_query.taxonomy
        if wrap_query.urls:
            if self.urls:
                self.urls = ListHelper.merge_list(
                    self.urls, wrap_query.urls)

            else:
                self.urls = wrap_query.urls

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
            return condition

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
        if not slugify_keywords:
            slugify_keywords = []
        contains = []
        if self.keywords:
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

        if self.taxonomy:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})
        if (self.urls is not None) or (self.keywords is not None):
            urls_list = []
            if self.urls:
                urls_list = self.urls.copy()
            if self.keywords:
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
        if not slugify_keywords:
            slugify_keywords = []
        contains = []
        if self.keywords:
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
        if slugify_keywords:
            keyword_slugs = slugify_keywords
        if self.urls:
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
        if self.keywords:
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

        if self.taxonomy:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})

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
        if not self.keywords and not self.taxonomy and not self.urls:
            raise ValueError(
                'self.keywords is None and self.taxonomy is None and self.urls is None')
        if self.keywords:
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

        if self.taxonomy:
            conditions.append({
                'condition': {
                    'list_contains': self.taxonomy
                },
                'property': 'properties.classifications_watson.taxonomy_labels'})

        if self.urls is not None or self.keywords:
            urls_list = []
            if self.urls:
                urls_list = self.urls.copy()
            if slugify_keywords:
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
        if self.segments:
            segments_list = self.segments.copy()
        else:
            segments_list = []

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
        if not self.segments:
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
            if segment_int:
                transitions.append(transition)
        return transitions

    def __create_second_party_segments(self) -> List[Dict]:
        second_party_condition = []
        if not self.second_party_segments:
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
        logging.debug("slugify_keywords")
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
