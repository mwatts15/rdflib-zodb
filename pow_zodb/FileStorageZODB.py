import logging
import os

from rdflib.store import Store
import transaction
import ZODB
from ZODB.FileStorage import FileStorage
from zc.lockfile import LockError

from .ZODB import ZODBStore


L = logging.getLogger(__name__)


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
        self._store = None

    def open(self, configuration, create=True):
        openstr = os.path.abspath(configuration)

        try:
            fs = FileStorage(openstr)
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

    def bind(self, prefix, namespace):
        self._store.bind(prefix, namespace)

    def namespace(self, prefix):
        return self._store.namespace(prefix)

    def prefix(self, namespace):
        return self._store.prefix(namespace)

    def namespaces(self):
        for prefix, namespace in self._store.namespaces():
            yield prefix, namespace

    def rollback(self):
        self._store.rollback()

    def commit(self):
        self._store.commit()

    def addN(self, quads):
        self._store.addN(quads)

    def add(self, triple, context, quoted=False):
        self._store.add(triple, context, quoted=quoted)

    def contexts(self, triple):
        for c in self._store.contexts(triple):
            yield c

    def triples(self, triple, context=None):
        for t in self._store.triples(triple, context=context):
            yield t

    def triples_choices(self, triple, context=None):
        for t in self._store.triples_choices(triple, context=context):
            yield t

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
