# Author: Michel Pelletier

from rdflib.store import Store
from rdflib.events import Dispatcher
from rdflib import BNode

from persistent import Persistent
from persistent.dict import PersistentDict
import transaction
import logging

import contextlib
import itertools
from operator import itemgetter
from random import randrange

import BTrees
from BTrees.Length import Length
from BTrees.OOBTree import intersection as oo_intersection

ANY = Any = None
DEFAULT = BNode('ZODBStore:DEFAULT')
L = logging.getLogger(__name__)
# TODO:
#   * is zope.intids id search faster? (maybe with large dataset and actual
#     disk access?)


ID_SERIES = {'default': 0}
ID_MAX = 2 ** 48 - 1
ID_MASK = ID_MAX
SERIES_MAX = 2 ** 16 - 1


def register_id_series(name):
    if name in ID_SERIES:
        raise ValueError('The given name has already been registered')
    series = max(ID_SERIES.values()) + 1
    if series > SERIES_MAX:
        raise Exception('More than the allowed number of series has been registered')
    ID_SERIES[name] = series


def grouper(iterable, n):
    "Collect data into chunks of at most n elements"
    i = 0
    lst = []
    iterable = iter(iterable)
    while True:
        try:
            lst.append(next(iterable))
        except StopIteration:
            break

        if i > 0 and i % n == 0:
            yield lst
            lst = []
        i += 1

    if len(lst) != 0:
        yield lst


NO_ID = object()


def _fix_ctx(ctx):
    ctx = getattr(ctx, 'identifier', ctx)
    if ctx is None:
        ctx = DEFAULT
    return ctx


class ZODBStore(Persistent, Store):

    context_aware = True
    formula_aware = True
    graph_aware = True
    transaction_aware = True
    supports_range_queries = True

    family = BTrees.family64
    _context_lengths = None
    version = 2

    def __init__(self, configuration=None, identifier=None, family=None):
        super(ZODBStore, self).__init__(configuration, identifier)
        if family is not None:
            self.family = family
        self._namespace = self.family.OO.BTree()
        self._prefix = self.family.OO.BTree()
        self._int2obj = self.family.UO.BTree()
        self._int2obj[0] = self.family.UO.BTree()
        self._obj2int = self.family.UO.BTree()
        self._obj2int[0] = self.family.OU.BTree()
        # subject index key: sid val: enctriple
        self._subjectIndex = self.family.UO.BTree()
        # predicate index key: pid val: enctriple
        self._predicateIndex = self.family.UO.BTree()
        # object index key: oid val: enctriple
        self._objectIndex = self.family.UO.BTree()
        # context index key: cid val: enctriple
        self._contextIndex = self.family.UO.BTree()
        self._tripleContexts = self.family.OO.BTree()
        self._all_contexts = self.family.OO.TreeSet()
        self._defaultContexts = None
        self._context_lengths = self.family.OO.BTree()

    @property
    def dispatcher(self):
        if not hasattr(self, '_v_dispatcher'):
            self._v_dispatcher = Dispatcher()
        return self._v_dispatcher

    @dispatcher.setter
    def dispatcher(self, dispatcher):
        self._v_dispatcher = dispatcher

    @contextlib.contextmanager
    def _in_all_triples(self):
        self._v_all_triples = True
        try:
            yield
        finally:
            del self._v_all_triples

    @property
    def _is_in_all_triples(self):
        return getattr(self, '_v_all_triples', False)

    def rollback(self):
        transaction.abort()
        transaction.begin()

    def commit(self):
        transaction.commit()
        transaction.begin()

    def bind(self, prefix, namespace):
        self._prefix[namespace] = prefix
        self._namespace[prefix] = namespace

    def namespace(self, prefix):
        return self._namespace.get(prefix, None)

    def prefix(self, namespace):
        return self._prefix.get(namespace, None)

    def namespaces(self):
        for prefix, namespace in self._namespace.items():
            yield prefix, namespace

    def addN(self, quads):
        c1, c2, c3 = 0, 0, 0
        for qgroup in grouper(quads, 10000):
            encquads = list()
            for q in qgroup:
                ctx = _fix_ctx(q[3])
                Store.add(self, q[:3], q[3], False)
                encquads.append(((self._obj2id(q[0]),
                                  self._obj2id(q[1]),
                                  self._obj2id(q[2])),
                                self._obj2id(ctx),
                                ctx))

            for enctriple, cid, context in encquads:
                if context is not DEFAULT:
                    self._all_contexts.add(context)
                self._addTripleContext(enctriple, cid, False)
            c1 += self._addN_helper(encquads, self._subjectIndex, 0)
            c2 += self._addN_helper(encquads, self._predicateIndex, 1)
            c3 += self._addN_helper(encquads, self._objectIndex, 2)
        assert c1 == c2
        assert c2 == c3
        return c1

    def _addN_helper(self, encquads, index, p):
        res = 0
        for enctriple, __, __ in encquads:
            ind_id = enctriple[p]
            s = index.get(ind_id, None)
            if s is None:
                index[ind_id] = self.family.OO.Set((enctriple,))
                res += 1
            else:
                res += s.add(enctriple)
        return res

    def add(self, triple, context, quoted=False):
        Store.add(self, triple, context, quoted)
        context = getattr(context, 'identifier', context)
        if context is None:
            context = DEFAULT
        if context is not DEFAULT and context not in self._all_contexts:
            self._all_contexts.add(context)

        enctriple = self._encodeTriple(triple)
        sid, pid, oid = enctriple

        cid = self._obj2id(context)
        self._addTripleContext(enctriple, cid, quoted)

        if sid in self._subjectIndex:
            self._subjectIndex[sid].add(enctriple)
        else:
            self._subjectIndex[sid] = self.family.OO.Set((enctriple,))

        if pid in self._predicateIndex:
            self._predicateIndex[pid].add(enctriple)
        else:
            self._predicateIndex[pid] = self.family.OO.Set((enctriple,))

        if oid in self._objectIndex:
            self._objectIndex[oid].add(enctriple)
        else:
            self._objectIndex[oid] = self.family.OO.Set((enctriple,))

    def _objsInRange(self, rng):
        series_name = getattr(rng[0], 'zodb_id_series', 'default')
        series = ID_SERIES[series_name]
        return minmax(self._obj2int[series].values(min=rng[0], max=rng[1]))

    def remove(self, triplepat, context=None):
        Store.remove(self, triplepat, context)
        context = getattr(context, 'identifier', context)
        if context is None:
            context = DEFAULT
        defid = self._obj2id(DEFAULT)
        req_cid = self._obj2id(context)

        was_in_all_triples = False

        for triple, contexts in self.triples(triplepat, context):
            was_in_all_triples = self._is_in_all_triples
            enctriple = self._encodeTriple(triple)
            for cid in self._getTripleContexts(enctriple):
                if context is not DEFAULT and req_cid != cid:
                    continue
                self._removeTripleContext(enctriple, cid)

            ctxs = self._getTripleContexts(enctriple, skipQuoted=True)
            if defid in ctxs and (context is DEFAULT or len(ctxs) == 1):
                self._removeTripleContext(enctriple, defid)
            if len(self._getTripleContexts(enctriple)) == 0:
                # triple has been removed from all contexts
                sid, pid, oid = enctriple
                self._subjectIndex[sid].remove(enctriple)
                self._predicateIndex[pid].remove(enctriple)
                self._objectIndex[oid].remove(enctriple)

                del self._tripleContexts[enctriple]

        if triplepat == (None, None, None) and \
                context in self._all_contexts and \
                not self.graph_aware:
            # remove the whole context but not empty graphs
            self._all_contexts.remove(context)
            self._contextIndex.pop(req_cid)
        if was_in_all_triples:
            trips = self._contextIndex.get(req_cid, None)
            if trips is not None:
                if isinstance(triplepat[2], tuple):
                    raise NotImplementedError('Cannot remove ranges')
                else:
                    self._contextIndex.pop(req_cid)

    def triples(self, triplein, context=None):
        rng = None
        context = getattr(context, 'identifier', context)
        if context is not None:
            if context == self:  # hmm...does this really ever happen?
                context = None
        if context is None:
            context = DEFAULT

        cid = self._obj2id_finf(context)
        if cid is NO_ID:
            return self._emptygen()
        if isinstance(triplein[2], tuple):
            enctriple = (self._obj2id(triplein[0]),
                         self._obj2id(triplein[1]),
                         None)
            rng = triplein[2]
        else:
            enctriple = self._encodeTriple_finf(triplein)

        if NO_ID in enctriple:
            return self._emptygen()

        sid, pid, oid = enctriple

        # all triples case (no triple parts given as pattern)
        if sid is None and pid is None and oid is None:
            return self._all_triples(cid)

        # optimize "triple in graph" case (all parts given)
        if sid is not None and pid is not None and oid is not None:
            if sid in self._subjectIndex and \
                    enctriple in self._subjectIndex[sid] and \
                    self._tripleHasContext(enctriple, cid):
                return ((triplein, self._contexts(enctriple)) for i in [0])
            else:
                return self._emptygen()

        # remaining cases: one or two out of three given
        sets = []
        if sid is not None:
            sset = self._subjectIndex.get(sid)
            if not sset:
                return self._emptygen()
            sets.append(sset)
        if pid is not None:
            pset = self._predicateIndex.get(pid)
            if not pset:
                return self._emptygen()
            sets.append(pset)
        if oid is not None:
            oset = self._objectIndex.get(oid)
            if not oset:
                return self._emptygen()
            sets.append(oset)

        if rng is not None:
            rng = self._objsInRange(rng)

        # to get the result, do an intersection of the sets (if necessary)
        if len(sets) == 2:
            enctriples = oo_intersection(sets[0], sets[1])
        else:
            enctriples = set(sets[0])  # OOSet(sets[0])

        if rng is not None:
            return ((self._decodeTriple(enctriple),
                     self._contexts(enctriple))
                    for enctriple in enctriples
                    if self._tripleHasContext(enctriple, cid) and
                    enctriple[2] > rng[0] and enctriple[2] < rng[1])
        else:
            return ((self._decodeTriple(enctriple),
                     self._contexts(enctriple))
                    for enctriple in enctriples
                    if self._tripleHasContext(enctriple, cid))

    def contexts(self, triple=None):
        if triple is None or triple == (None, None, None):
            return (context for context in self._all_contexts)

        enctriple = self._encodeTriple(triple)
        sid, pid, oid = enctriple
        if ((sid in self._subjectIndex and
             enctriple in self._subjectIndex[sid])):
            return self._contexts(enctriple)
        else:
            return self._emptygen()

    def _context_length(self, cid):
        context_lengths = self._context_lengths
        if cid not in context_lengths:
            context_lengths[cid] = Length()
        return context_lengths[cid]

    def __len__(self, context=None):
        context = getattr(context, 'identifier', context)

        if context is None:
            context = DEFAULT
        cid = self._obj2id(context)
        res = self._context_length(cid)()
        return res

    def add_graph(self, graph):
        if not self.graph_aware:
            Store.add_graph(self, graph)
        else:
            self._all_contexts.add(getattr(graph, 'identifier', graph))

    def remove_graph(self, graph):
        if not self.graph_aware:
            Store.remove_graph(self, graph)
        else:
            self.remove((None, None, None), graph)
            try:
                self._all_contexts.remove(getattr(graph, 'identifier', graph))
            except KeyError:
                pass  # we didn't know this graph, no problem

    def _addTripleContext(self, enctriple, cid, quoted):
        """add the given context to the set of contexts for the triple"""
        defid = self._obj2id(DEFAULT)

        sid, pid, oid = enctriple
        sind = self._subjectIndex.get(sid, None)
        if sind is not None and enctriple in sind:
            # we know the triple exists somewhere in the store
            tripctx = self._tripleContexts.get(enctriple, None)
            if tripctx is None:
                # triple exists with default ctx info
                # start with a copy of the default ctx info
                tripctx = self._defaultContexts.copy()

            if cid not in tripctx:
                self._context_length(cid).change(1)
            tripctx[cid] = quoted
            if not quoted:
                if defid not in tripctx:
                    self._context_length(defid).change(1)
                tripctx[defid] = False
        else:
            self._context_length(cid).change(1)
            # the triple didn't exist before in the store
            dct = {cid: quoted}
            if not quoted:
                dct[defid] = False

            tripctx = PersistentDict(dct)

            if not quoted:
                self._context_length(defid).change(1)

        # if this is the first ever triple in the store, set default ctx info
        if self._defaultContexts is None:
            self._defaultContexts = tripctx

        # if the context info is the same as default, no need to store it
        if tripctx != self._defaultContexts:
            self._tripleContexts[enctriple] = tripctx
        for c in tripctx:
            ctx_idx = self._contextIndex.get(c, None)
            if ctx_idx is None:
                self._contextIndex[c] = ctx_idx = self.family.OO.Set((enctriple,))
            else:
                ctx_idx.add(enctriple)

    def _getTripleContexts(self, enctriple, skipQuoted=False):
        """return a list of (encoded) contexts for the triple, skipping
           quoted contexts if skipQuoted==True"""

        ctxs = self._tripleContexts.get(enctriple, self._defaultContexts)

        if not skipQuoted:
            return list(ctxs.keys())

        return [cid for cid, quoted in ctxs.items() if not quoted]

    def _getTripleContextsIter(self, enctriple):
        return (cid for cid, quoted
                in self._tripleContexts.get(enctriple, self._defaultContexts).items()
                if not quoted)

    def _tripleHasContext(self, enctriple, cid):
        """return True iff the triple exists in the given context"""
        ctxs = self._tripleContexts.get(enctriple, self._defaultContexts)
        return (cid in ctxs)

    def _removeTripleContext(self, enctriple, cid):
        """remove the context from the triple"""
        ctxs = self._tripleContexts.get(
            enctriple, self._defaultContexts).copy()
        del ctxs[cid]
        self._context_length(cid).change(-1)
        if ctxs == self._defaultContexts:
            del self._tripleContexts[enctriple]
        else:
            self._tripleContexts[enctriple] = ctxs
        if not self._is_in_all_triples:
            triples = self._contextIndex.get(cid, None)
            if triples is not None:
                triples.remove(enctriple)

    def _obj2id_finf(self, obj):
        if obj is None:
            return None
        series_name = getattr(obj, 'zodb_id_series', 'default')
        series = ID_SERIES[series_name]
        o2i = self._obj2int.get(series)
        if o2i is not None:
            return o2i.get(obj, NO_ID)
        return NO_ID

    def _obj2id(self, obj):
        """encode object, storing it in the encoding map if necessary, and
           return the integer key.

           New identifiers are generated in such a way as to increase the
           chance that identifers added at the same time will be clustered
           closer together in the btree. This relies on the assumption that
           data added within a single run of a program is likelier to be
           accessed together at a later time than other data.
        """
        if obj is None:
            return None

        series_name = getattr(obj, 'zodb_id_series', 'default')
        series = ID_SERIES[series_name]
        o2i = self._obj2int.get(series, None)
        if o2i is None:
            self._obj2int[series] = o2i = self.family.OU.BTree()

        nextid = o2i.get(obj, None)
        if nextid is None:
            try:
                nextid_int = self._v_next_id
            except AttributeError:
                self._v_next_id = nextid_int = randrange(0, ID_MAX)

            try:
                i2o = self._int2obj[series]
            except KeyError:
                self._int2obj[series] = i2o = self.family.UO.BTree()

            while i2o.insert(nextid_int, obj) == 0:
                nextid_int = randrange(0, ID_MAX)
            self._v_next_id = nextid_int + 1
            if self._v_next_id > ID_MAX:
                # Delete the next ID so we generate a new one next time round.
                del self._v_next_id
            o2i[obj] = nextid = (series << 48) | nextid_int
        return nextid

    def _make_object_slices(self, items, thresh=100000):
        items = ((ID_SERIES[getattr(x, 'zodb_id_series', 'default')], x)
                for x in items)
        items = sorted(items)
        items = itertools.groupby(items, key=itemgetter(0))
        for series, item_group in items:
            item_group = list(item_group)
            least = min(item_group)
            greatest = max(item_group)
            yield series, least[1], greatest[1]

    def _exo(self, index, triple, context, tidx, aidx, bidx):
        """
        Computes the triples_choices result for the given triple

        Parameters
        ----------
        index : UOBTree
            the index of the 'target' entry
        triple : tuple
            the triple to query against
        context : identifier
            the context for the query
        tidx : int
            'target' index. The one with the list
        aidx : int
            One of the other indices
        bidx : int
            One of the other indices
        """
        target = triple[tidx]
        if len(target) == 1:
            triple = tuple(s[0] if s is target else s for s in triple)
            return self.triples(triple, context)
        elif len(target) == 0:
            triple = tuple(None if s is target else s for s in triple)
            return self.triples(triple, context)
        elif None in target:
            # It's not clear whether we need to return the other entries based
            # on the 'fallback'/default implementation. I'm going with the
            # cheapest interpretation of just computing as if there were one
            # Null there
            triple = tuple(None if s is target else s for s in triple)
            return self.triples(triple, context)
        else:
            results = set()

            aid = self._obj2id_finf(triple[aidx])
            if aid is NO_ID:
                return self._emptygen()

            bid = self._obj2id_finf(triple[bidx])
            if bid is NO_ID:
                return self._emptygen()

            obj_ids = sorted(self._multiple_obj2id(target))

            if len(obj_ids) == 0:
                return self._emptygen()

            # I'm assuming that a bad context is an outside occurance which is
            # why I don't check it earlier than recoving the IDs from the
            # target
            cid = self._obj2id_finf(context)
            if cid is NO_ID:
                return self._emptygen()

            last = None
            newids = []
            for x in obj_ids:
                if x != last:
                    newids.append(x)
                last = x
            obj_ids = newids

            min_id = min(obj_ids)
            max_id = max(obj_ids)
            items = dict(index.iteritems(min=min_id, max=max_id))
            for y in obj_ids:
                yitems = items.get(y)
                if yitems:
                    if aid is None and bid is None:
                        results.add(yitems)
                    elif aid is None:
                        results.add(z for z in yitems if z[bidx] == bid)
                    elif bid is None:
                        results.add(z for z in yitems if z[aidx] == aid)
                    else:
                        results.add(z for z in yitems
                                    if (z[aidx] == aid and z[bidx] == bid))

            return ((self._decodeTriple(enctriple),
                     self._contexts(enctriple))
                    for enctriples in results
                    for enctriple in enctriples
                    if self._tripleHasContext(enctriple, cid))

    def triples_choices(self, triple, context=None):
        context = getattr(context, 'identifier', context)
        if context is not None:
            if context == self:
                context = None
        if context is None:
            context = DEFAULT

        subject, predicate, object_ = triple
        if isinstance(object_, list):
            assert not isinstance(
                subject, list), "object_ / subject are both lists"
            assert not isinstance(
                predicate, list), "object_ / predicate are both lists"
            if object_:
                return self._exo(self._objectIndex, triple, context, 2, 0, 1)
            else:
                return self.triples((subject, predicate, None), context)

        elif isinstance(subject, list):
            assert not isinstance(
                predicate, list), "subject / predicate are both lists"
            if subject:
                return self._exo(self._subjectIndex, triple, context,
                        0, 1, 2)
            else:
                return self.triples((None, predicate, object_), context)
        elif isinstance(predicate, list):
            if predicate:
                return self._exo(self._predicateIndex, triple, context,
                        1, 0, 2)
            else:
                return self.triples((subject, None, object_), context)
        else:
            return self._emptygen()

    def _multiple_obj2id(self, objs):
        """ Note that the semantics are different from obj2id: If there are
        'misses' here, there won't be any updates. Also, there's no option
        to 'fail' if there's a miss, although one can always check if the
        IDs returned and the objects passed in are the same in number.
        """
        slices = list(self._make_object_slices(objs))
        for series, least_obj, greatest_obj in slices:
            ii = self._obj2int[series].iteritems
            items = dict(ii(least_obj, greatest_obj))
            for j in objs:
                v = items.get(j)
                if v is not None:
                    yield v

    def _id2obj(self, id):
        series = id >> 48
        id_part = id & ID_MASK
        return self._int2obj[series][id_part] if id is not None else None

    def _encodeTriple_finf(self, triple):
        """ encode a whole triple, returning the encoded triple. Returns NO_IDs if a triple isn't found """
        return (self._obj2id_finf(triple[0]),
                self._obj2id_finf(triple[1]),
                self._obj2id_finf(triple[2]))

    def _encodeTriple(self, triple):
        """encode a whole triple, returning the encoded triple"""
        return (self._obj2id(triple[0]),
                self._obj2id(triple[1]),
                self._obj2id(triple[2]))

    def _decodeTriple(self, enctriple):
        """decode a whole encoded triple, returning the original triple"""
        try:
            return (self._id2obj(enctriple[0]),
                    self._id2obj(enctriple[1]),
                    self._id2obj(enctriple[2]))
        except TypeError as e:
            raise TypeError(enctriple) from e

    def _all_triples(self, cid):
        """return a generator which yields all the triples (unencoded) of
           the given context"""
        with self._in_all_triples():
            for enctriple in self._contextIndex.get(cid, ()):
                yield (self._decodeTriple(enctriple),
                       self._contexts(enctriple))

    def _contexts(self, enctriple):
        """return a generator for all the non-quoted contexts (unencoded)
           the encoded triple appears in"""
        return (self._id2obj(cid) for cid
                in self._getTripleContextsIter(enctriple)
                if cid is not DEFAULT)

    def _emptygen(self):
        """return an empty generator"""
        if False:
            yield


def minmax(data):
    """
    Computes the minimum and maximum values in one-pass using only
    1.5*len(data) comparisons
    """
    it = iter(data)
    try:
        lo = hi = next(it)
    except StopIteration:
        raise ValueError('minmax() arg is an empty sequence')
    for x, y in itertools.zip_longest(it, it, fillvalue=lo):
        if x > y:
            x, y = y, x
        if x < lo:
            lo = x
        if y > hi:
            hi = y
    return lo, hi
