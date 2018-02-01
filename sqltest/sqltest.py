# -*- coding: utf-8 -*-

"""Main module."""

import inspect
import pickle
import md5
import os.path
from contextlib import contextmanager


from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from utils import query_constructor, get_hash

_ROOT = os.path.abspath(os.path.dirname(__file__))
CACHE_DIR = os.path.join(_ROOT, '.sqltest')


def cache_location(s):
     return os.path.join(CACHE_DIR, 'models-%s.pkl' % get_hash(s))


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class SQLT():

    def __init__(self, connection, force_cache_update=False):
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

        engine = create_engine(connection)
        metadata = MetaData()
        self.session_factory = sessionmaker(bind=engine)

        cache_file = cache_location(connection)
        if os.path.isfile(cache_file) and not force_cache_update:
            print "Retrieving models from cache..."
            with open(cache_file, 'rb') as input:
                metadata = pickle.load(input)
                Base = automap_base(metadata=metadata)
                Base.prepare()
                self.models = [i for i in Base.classes]
        else:
            metadata.reflect(engine)
            Base = automap_base(metadata=metadata)
            Base.prepare()
            self.models = [i for i in Base.classes]
            print "Cacheing models..."
            with open(cache_file, 'wb') as output:
                pickle.dump(metadata, output, pickle.HIGHEST_PROTOCOL)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = this.session_factory()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def get_table(self, name):
        tables = {
            table_class.__name__: table_class for table_class in self.models
        }
        return tables.get(name, None)

    def run(self, raw_query):
        with self.session_scope() as session:
            sql = text(raw_query)
            result_set = session.execute(sql)
            return [i for i in result_set]

    def checker(self, raw_query, check_function):
        with self.session_scope() as session:
            sql = text(raw_query)
            result_set = session.execute(sql)
            return check_function([i for i in result_set])

    def has_results(self, raw_query, params=None):
        def check_function(results):
            assert results and len(results) > 0, "This query returned zero results"
            return results

        query = query_constructor(raw_query, params) if params else raw_query
        return self.checker(query, check_function)

    def parameter_factory(self, query, replacement_func=None):
        def apply_replacement(q, params):
            return self.checker(q, replacement_func)

        def get_params(params, specific_func=None):
            q = query_constructor(query, params)
            if specific_func:
                replacement_func = specific_func
            return apply_replacement(q, params) if replacement_func else has_results(q, params)

        return get_params

    def has_items(self, table, id_column_name, id_list):
        with self.session_scope() as session:
            cls = self.get_table(table)
            current_ids = set([i[0] for i in session.query(getattr(cls, id_column_name)).all()])
            check_ids = set([i for i in id_list])

            intersection = current_ids.intersection(check_ids)
            difference = current_ids.difference(check_ids)

            return {
                'existing': intersection,
                'new': difference
            }
