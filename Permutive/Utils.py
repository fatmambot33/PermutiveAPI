import ast
import datetime
import json
import os
import pathlib
import re
import unicodedata
from glob import glob
from typing import List, Optional, Union


class FileHelper:
    @staticmethod
    def save_to_json(obj, filepath: str):

        def json_default(value):
            if isinstance(value, datetime.date):
                return dict(year=value.year, month=value.month, day=value.day)
            else:
                return value.__dict__
        FileHelper.check_filepath(filepath)
        with open(file=filepath, mode='w', encoding='utf-8') as f:
            json.dump(obj, f,
                      ensure_ascii=False, indent=4, default=json_default)

    @staticmethod
    def read_json(filepath: str):
        with open(file=filepath, mode='r') as json_file:
            return json.load(json_file)

    @staticmethod
    def check_filepath(filepath: str):
        if not os.path.exists(os.path.dirname(filepath)) and len(os.path.dirname(filepath)) > 0:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    @staticmethod
    def split_filepath(fullfilepath):
        p = pathlib.Path(fullfilepath)
        file_path = str(p.parent)+'/'
        file_name = p.name
        file_extension = ''
        for suffix in p.suffixes:
            file_name = file_name.replace(suffix, '')
            file_extension = file_extension+suffix
        return file_path, file_name, file_extension

    @staticmethod
    def file_exists(fullfilepath):
        file_path, file_name, file_extension = FileHelper.split_filepath(
            fullfilepath)

        if len(glob(file_path+file_name + '-*' + file_extension) + glob(file_path+file_name + file_extension)) == 0:
            return False
        return True


class StringHelper:
    @staticmethod
    def slugify(value, allow_unicode=False):
        """
        Taken from https://github.com/django/django/blob/master/django/utils/text.py
        Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
        dashes to single dashes. Remove characters that aren't alphanumerics,
        underscores, or hyphens. Convert to lowercase. Also strip leading and
        trailing whitespace, dashes, and underscores.
        """
        value = str(value)
        if allow_unicode:
            value = unicodedata.normalize('NFKC', value)
        else:
            value = unicodedata.normalize('NFKD', value).encode(
                'ascii', 'ignore').decode('ascii')
        value = re.sub(r'[^\w\s-]', '', value.lower())
        return re.sub(r'[-\s]+', '-', value).strip('-_')


class ListHelper:

    @staticmethod
    def chunk_list(lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    @staticmethod
    def convert_list(val):
        if isinstance(val, str):
            return ast.literal_eval(val)
        else:
            return val

    @staticmethod
    def merge_list(lst1: List, lst2: Optional[Union[int, str, List]] = None) -> List:
        if isinstance(lst2, str) or isinstance(lst2, int):
            lst2 = [lst2]
        if lst2 is None:
            lst2 = []
        lst = list(filter(None, list(dict.fromkeys(lst1+lst2)))).sort()
        return lst
