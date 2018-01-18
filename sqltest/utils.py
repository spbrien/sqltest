# -*- coding: utf-8 -*-

"""Utilities module."""

import hashlib

def query_constructor(query, params=None):
    return query % params if params else query


def get_hash(s):
    dm = hashlib.md5()
    m.update(s)
    return m.hexdigest()
