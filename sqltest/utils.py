# -*- coding: utf-8 -*-

"""Utilities module."""

def query_constructor(query, params=None):
    return query % params if params else query
