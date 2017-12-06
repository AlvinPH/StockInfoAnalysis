__author__ = 'phlai'

from datetime import datetime
from re import sub


class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def str2datetime(x):
    t = x.split('-')
    return datetime(int(t[0]), int(t[1]), int(t[2]))


def clear_comma(x):
    if isinstance(x, str):
        return sub("[, ]", "", x)
    else:
        return x