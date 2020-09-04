import logging
import os

from rdflib.store import Store
import transaction
import ZODB
from ZODB.FileStorage import FileStorage
from zc.lockfile import LockError

from .ZODB import ZODBStore


L = logging.getLogger(__name__)


class _UnopenedStore(object):
    __slots__ = ()

    def __getattr__(self, *args):
        raise Exception('This FileStorageZODBStore has not been opened')


UNOPENED_STORE = _UnopenedStore()


class FileStorageZODBStore(Store):
    '''
    `~ZODB.FileStorage.FileStorage`-backed Store
    '''

    context_aware = True
    formula_aware = True
    graph_aware = True
    transaction_aware = True
    supports_range_queries = True

    def __init__(self, *args, **kwargs):
        super(FileStorageZODBStore, self).__init__(*args, **kwargs)
        self._store = UNOPENED_STORE

    def open(self, configuration, create=True):
        if isinstance(configuration, dict):
            url = configuration.get('url', None)
            if url is None:
                raise ValueError('FileStorageZODBStore configuration dict must have a "url" key')
            openstr = os.path.abspath(url)
            params = {k: v for k, v in configuration.items() if k != 'url'}
        elif isinstance(configuration, str):
            openstr = os.path.abspath(configuration)
            params = dict()
        else:
            raise TypeError(f'Not an expected configuration type: {configuration} of type {type(configuration)}')

        try:
            fs = FileStorage(openstr, **params)
        except IOError:
            L.exception("Failed to create a FileStorage")
            raise FileStorageInitFailed(openstr)
        except LockError:
            L.exception('Found database "{}" is locked when trying to open it. '
                    'The PID of this process: {}'.format(openstr, os.getpid()), exc_info=True)
            raise FileLocked('Database ' + openstr + ' locked')

        self._zdb = ZODB.DB(fs, cache_size=1600)
        self._conn = self._zdb.open()
        root = self._conn.root()
        if 'rdflib' not in root:
            root['rdflib'] = self._store = ZODBStore()
        else:
            self._store = root['rdflib']
        try:
            transaction.commit()
        except Exception:
            # catch commit exception and close db.
            # otherwise db would stay open and follow up tests
            # will detect the db in error state
            L.exception('Forced to abort transaction on ZODB store opening', exc_info=True)
            transaction.abort()
        transaction.begin()

    def close(self, commit_pending_transaction=True):
        if commit_pending_transaction:
            try:
                transaction.commit()
            except Exception:
                # catch commit exception and close db.
                # otherwise db would stay open and follow up tests
                # will detect the db in error state
                L.warning('Forced to abort transaction on ZODB store closing', exc_info=True)
                transaction.abort()
        self._conn.close()
        self._zdb.close()

        self._conn = None
        self._zdb = None

    def bind(self, prefix, namespace):
        self._store.bind(prefix, namespace)

    def namespace(self, prefix):
        return self._store.namespace(prefix)

    def prefix(self, namespace):
        return self._store.prefix(namespace)

    def namespaces(self):
        return self._store.namespaces()

    def rollback(self):
        self._store.rollback()

    def commit(self):
        self._store.commit()

    def addN(self, quads):
        self._store.addN(quads)

    def add(self, triple, context, quoted=False):
        self._store.add(triple, context, quoted=quoted)

    def contexts(self, triple):
        return self._store.contexts(triple)

    def triples(self, triple, context=None):
        return self._store.triples(triple, context)

    def triples_choices(self, triple, context=None):
        return self._store.triples_choices(triple, context=context)

    def remove(self, triplepat, context=None):
        self._store.remove(triplepat, context=context)

    def __len__(self, context=None):
        return self._store.__len__(context)

    def add_graph(self, graph):
        self._store.add_graph(graph)

    def remove_graph(self, graph):
        self._store.remove_graph(graph)


class OpenError(Exception):
    pass


class FileStorageInitFailed(OpenError):
    pass


class FileLocked(OpenError):
    pass
