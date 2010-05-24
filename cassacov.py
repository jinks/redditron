#!/usr/bin/env python

import re
import sys
import random
import itertools
from zlib import crc32

import pycassa
import cassandra.ttypes

chain_length = 5
# chains of longer lengths are weighted more heavily when picking the
# next follower. This list defines how heavily
chain_weights = range(1, chain_length+1)

# whether to append EndTokens to the end of token streams. Not doing
# so biases for longer comments
endtokens = False

def trace(fn):
    def _fn(*a, **kw):
        sys.stderr.write('%r(%r, %r)\n' % (fn, a, kw))
        ret = fn(*a, **kw)
        # sys.stderr.write('%r(%r, %r): %r\n' % (fn, a, kw, ret))
        return ret
    return _fn

class Cache(object):
    # Types:
    # * tokenlist() -> [Token]
    # * hashedtoken()
    def __init__(self, init_args):
        seed, keyspace, column_family,seen_cf = init_args.split(',')
        self.seeds = [seed]
        self.keyspace = keyspace
        self.column_family = column_family

        self.client = pycassa.connect_thread_local(self.seeds)
        self.cf = pycassa.ColumnFamily(self.client, self.keyspace,
                                       self.column_family)
        self.seen_cf = pycassa.ColumnFamily(self.client, self.keyspace,
                                            seen_cf)

    def _hash_tokens(self, tokens):
        """tokenlist() -> hashedtoken()"""
        return ' '.join(tok.tok.encode('utf-8') for tok in tokens)

    def get_followers(self, keys):
        """get_followers([tokenlist()]) -> dict(Token -> count)"""
        try:
            stored = self.cf.get(self._hash_tokens(keys), column_count=10*1000)
            # TODO: handle the case that there are more than 10k
            # columns available (the current behaviour is that we take
            # the 10k ASCIIbetically first ones)
            return dict((Token(k), int(v))
                        for (k, v)
                        in stored.iteritems())
        except (cassandra.ttypes.NotFoundException, KeyError, ValueError):
            return {}

    def incr_follower(self, preds, token):
        """incr_followers([token()], token())"""
        # these incrs are unsafe, but redditron is not a bank
        hpreds = self._hash_tokens(preds)
        try:
            existing = int(self.cf.get(hpreds)[token.tok])
        except (cassandra.ttypes.NotFoundException, KeyError, ValueError):
            existing = 0
        self.cf.insert(hpreds, {token.tok: str(existing+1)})

    def saw(self, key):
        # TODO: we have a way to clean up the Followers CF, but not
        # the Seen CF. This can be tricky because we want e.g. the
        # Twitter DM box to never be processed twice (because it has
        # stateful commands in it). This could be simplified to just
        # store a single key since both Twitter and reddit can say
        # "give me the messages that arrived after this ID', but
        # reddit's `before` parameter doesn't deal well with the case
        # that a lot of messages have arrived since the item in the
        # `before` param.
        self.seen_cf.insert(key, {'seen': '1'})

    def seen(self, key):
        try:
            return self.seen_cf.get(key, columns=['seen'])['seen'] == '1'
        except (cassandra.ttypes.NotFoundException, KeyError), e:
            return False

    def seen_iterator(self, it, key = lambda x: x):
        # this filter errs on the side of acking an item before it's
        # been processed.
        for x in it:
            seen_key = key(x)
            if not self.seen(seen_key):
                self.saw(seen_key)
                yield x

    def cleanup(self, decr):
        # Note! neither this nor incr_followers are atomic. We can
        # definitely get bad data this way if both are running at the
        # same time
        all_decrs = 0
        all_removals = 0
        all_keys_modified = 0
        for key, columns in self.cf.get_range(column_count = 10*1000):
            # TODO: detect that we got 10k columns and continue doing
            # requests until we've processed them all
            inserts = {}
            removals = []
            for fs, count in columns.iteritems():
                count = long(count)
                if count > decr:
                    inserts[fs] = str(count - decr)
                else:
                    removals.append(fs)

            if removals:
                # delete the keys for which decring their counts would
                # cause them to disappear
                self.cf.remove(key, removals)
                all_removals += len(removals)

            if inserts:
                # and decr the others
                self.cf.insert(key, inserts)
                all_decrs += len(inserts)

            if removals or inserts:
                all_keys_modified += 1

        return all_decrs, all_removals, all_keys_modified

class LookBehind(object):
    def __init__(self, size, init=[]):
        self.size = size
        self.data = []
        for x in init:
            self.data.append(x)

    def append(self, x):
        self.data.append(x)
        if len(self.data) > self.size:
            return self.data.pop(0)

    def __iter__(self):
        for x in reversed(self.data):
            yield x

    def __contains__(self, item):
        return item in self.data

    def __getitem__(self, n):
        return self.data[-(n+1)]

    def __len__(self):
        return len(self.data)

    def __bool__(self):
        return bool(self.data)

    def __repr__(self):
        return "LookBehind(%d, %r)" % (self.size, self.data)

class Token(object):
    types = dict(punc = re.compile(r'[?,!;:.()]').match,
                 word = re.compile(r'[A-Za-z0-9\'-]+').match,
                 whitespace = re.compile(r'|\s+').match)
    # must keep the splitter in sync with the types. None of these can
    # include pipes because we use them as a meta-character
    split_re = re.compile(r'(\s+|[A-Za-z0-9\'-]+|[?,!;:.()])')
    skip_re  = re.compile(r'^http.*') # tokens to skip when
                                      # tokenising. these are before
                                      # the split_re is applied
    capnexts = '?!.'
    nospaces_after = '('

    def __init__(self, tok, kind = None):
        self.tok = tok.lower()
        self.kind = kind or self._kind()

    def _kind(self):
        for (t, fn) in self.types.iteritems():
            if fn(self.tok):
                return t
        raise TypeError('Unknown token type %r' % self)

    @classmethod
    def tokenize(cls, text, beginend = True):
        """Given a string of text, yield the non-whitespace tokens
           parsed from it"""
        if beginend:
            yield BeginToken()
        for x in text.split(' '):
            if not cls.skip_re.match(x):
                for y in cls.split_re.split(x):
                    tok = cls(y)
                    if tok.kind != 'whitespace':
                        yield tok
        if beginend and endtokens:
            yield EndToken()

    def __repr__(self):
        return "Token(%r, %r)" % (self.tok, self.kind)

    def __eq__(self, other):
        return (isinstance(other, Token)
                and (self.tok, other.type) == (other.tok, other.type))


    @classmethod
    def detokenize(cls, tokens):
        """Given a stream of tokens, yield strings that concatenate to
           look like English sentences"""
        lookbehind = LookBehind(1)

        for tok in tokens:
            if isinstance(tok, BeginToken):
                continue
            elif isinstance(tok, EndToken):
                break

            text = tok.tok

            if (lookbehind
                and tok.kind == 'word'
                and lookbehind[0].tok not in cls.nospaces_after):
                yield ' '

            if not lookbehind or (lookbehind[0].tok in cls.capnexts):
                text = text[0].upper() + text[1:]

            yield text

            lookbehind.append(tok)

class BeginToken(Token):
    tok = 'BeginToken'
    kind = 'special'

    def __init__(self):
        pass
    def __repr__(self):
        return "BeginToken()"

class EndToken(Token):
    tok = 'EndToken'
    kind = 'special'

    def __init__(self):
        pass
    def __repr__(self):
        return "EndToken()"

def limit(it, lim=None):
    if lim == 0:
        return
    if lim is None:
        return it
    return itertools.islice(it, 0, lim)

def token_followers(tokens):
    """Given a list of tokens, yield tuples of lists of tokens (up to
       chain_length) and the tokens that follow them. e.g.:

       >>> list(token_followers([1,2,3,4,5]))
       [([1], 2),
        ([2], 3),
        ([1, 2], 3),
        ([3], 4),
        ([2, 3], 4),
        ([1, 2, 3], 4),
        ([4], 5),
        ([3, 4], 5),
        ([2, 3, 4], 5),
        ([1, 2, 3, 4], 5)]
    """
    # TODO: we could generate SkipTokens too to match 'i really like
    # bacon' to 'i don't like bacon'. At the loss of some accuracy we
    # could even match 'i like bacon' to 'i don't really like bacon'
    lookbehind = LookBehind(chain_length)
    for token in tokens:
        if lookbehind:
            for x in token_predecessors(lookbehind):
                yield x, token
            
        lookbehind.append(token)

def token_predecessors(lb):
    """Given a LookBehind buffer, yield all of the sequences of the
       last N items, e.g.

    >>> lb = LookBehind(5)
    >>> lb.append(1)
    >>> lb.append(2)
    >>> lb.append(3)
    >>> lb.append(4)
    >>> lb.append(5)
    >>> list(token_predecessors(lb))
    [[5], [4, 5], [3, 4, 5], [2, 3, 4, 5], [1, 2, 3, 4, 5]]
    """
    l = list(reversed(lb))
    for x in range(len(l)):
        yield tuple(l[-x-1:])

def _count_key(h, follower):
    return "%s_%s" % (h, crc32(follower))

def save_chains(cache, it):
    """Turn all of the strings yielded by `it' into chains and save
       them to memcached"""
    for cm in it:
        tokens = Token.tokenize(cm)
        followers = token_followers(tokens)
        for preds, token in followers:
            cache.incr_follower(preds, token)

def create_chain(cache):
    """Read the chains created by save_chains from memcached and yield
       a stream of predicted tokens"""
    lb = LookBehind(chain_length, [BeginToken()])

    while True:
        potential_followers = []
        all_preds = list(token_predecessors(lb))

        # build up the weights for the next token based on
        # occurrence-counts in the source data * the length
        # weight. build a list by duplicating the items according to
        # their weight. So given {a: 2, b: 3}, generate the list
        # [a, a, b, b, b]
        for preds in all_preds:
            for f, weight in cache.get_followers(preds).iteritems():
                potential_followers.extend([f] * (weight * chain_weights[len(preds)-1]))

        if not potential_followers:
            # no idea what the next token should be. This should only
            # happen if the storage backend has dumped the list of
            # followers for the previous token (since if it has no
            # followers, it would at least have an EndToken follower)
            break

        next = random.choice(potential_followers)

        if next.tok == EndToken.tok:
            break

        yield next

        lb.append(next)

def create_sentences(cache, length):
    """Create chains with create_chain and yield lines that look like
       English sentences"""
    while True:
        chain = limit(create_chain(cache), length)
        yield ''.join(Token.detokenize(chain))

def cleanup(cache, count):
    all_decrs, all_removals, all_keys_modified = cache.cleanup(decr=count)
    print ("%d columns decremented, %d columns removed, over %d rows"
           % (all_decrs, all_removals, all_keys_modified))

def main(memc, op, lim = None):
    cache = Cache(memc)

    try:
        if op == 'gen':
            lim = int(lim) if lim else None
            for x in limit(create_sentences(cache, 100), lim):
                if x:
                    print x
        elif op == 'cleanup':
            count = int(lim) if lim else 10
            cleanup(cache, count)
        else:
            print "Unknown op %r" % (op,)

    except KeyboardInterrupt:
        return

if __name__ == '__main__':
    main(*sys.argv[1:])
