#!/usr/bin/env python

import re
import sys
import random
import itertools
from zlib import crc32
from memcache import Client

chain_length = 5
# chains of longer lengths are weighted more heavily when picking the
# next follower. This list defines how heavily
chain_weights = range(1, chain_length+1)

# whether to append EndTokens to the end of token streams. Not doing
# so biases for longer comments
endtokens = False

class Cache(Client):
    def __init__(self, iden):
        Client.__init__(self, iden.split(','))

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
                for y in cls.split_re.split(text):
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
        """Given a stream of tokens, yield strings that look like
           English sentences"""
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
    tok = '\01'
    kind = 'special'

    def __init__(self):
        pass
    def __repr__(self):
        return "BeginToken()"

class EndToken(Token):
    tok = '\02'
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
        yield l[-x-1:]

def hash_tokens(tokens):
    return str(crc32(''.join(tok.tok.encode('utf8')
                             for tok in tokens)))

def _followers(cache, h):
    c = cache.get(h) or ''
    s = c or ''
    l = c.split('|')
    l = filter(None, l)
    return l

def _count_key(h, follower):
    return "%s_%s" % (h, crc32(follower))

def get_followers(cache, h):
    """Given a hash of a token or set of tokens, return a dict of all
       potential followers and their weights"""
    followers = _followers(cache, h)
    weight_keys = dict((_count_key(h, f), f)
                       for f in followers)
    weight_vals = cache.get_multi(weight_keys.keys())
    weights = dict((weight_keys[x], weight_vals[x])
                   for x in weight_vals
                   if weight_keys[x] > 0)
    return weights

def cleanup_counts(cache, followers_key):
    """To store the followers in memcached, we store one key
       containing a list of all of the followers, and then a key for
       each follower. This function deduplicates the list of followers
       and removes entries for which memcached has dropped the key
       containing the count"""

    followers = _followers(cache, followers_key)
    followers = set(followers)
    count_keys = [_count_key(followers_key, x)
                  for x in followers]
    existing_followers = cache.get_multi(count_keys)
    existing_followers = [x for x in followers
                          if existing_followers.get(_count_key(followers_key, x), 0) > 0]
    random.shuffle(existing_followers)
    if existing_followers:
        cache.set(followers_key, '|'.join(existing_followers)[:(1024*1024-1)])
    else:
        cache.delete(followers_key)

def save_chains(cache, it):
    """Turn all of the strings yielded by `it' into chains and save
       them to memcached"""
    for cm in it:
        tokens = Token.tokenize(cm)
        followers = token_followers(tokens)
        for preds, token in followers:
            text = token.tok.encode('utf8')
            followers_key = hash_tokens(preds)
            count_key = _count_key(followers_key, text)

            cache.add(followers_key, '')
            cache.append(followers_key, '|%s' % (text,))
            cache.add(count_key, 0)
            cache.incr(count_key)

            if random.randint(0, 100) == 0:
                cleanup_counts(cache, followers_key)

def create_chain(cache):
    """Read the chains created by save_chains from memcached and yield
       a stream of predicted tokens"""
    lb = LookBehind(chain_length, [BeginToken()])

    while True:
        preds = list(token_predecessors(lb))
        hashes = dict((hash_tokens(x), len(x))
                      for x in preds)
        # dict(hash -> dict(follower -> weight))
        #cached_hashes = cache.get_multi(hashes.keys())
        cached_hashes = dict((h, get_followers(cache, h))
                             for h in hashes)
        # remove the hashes with no followers
        cached_hashes = dict((h, fs) for (h, fs)
                             in cached_hashes.iteritems()
                             if fs)

        if not cached_hashes:
            # no idea what the next token should be. This should only
            # happen if the storage backend has dumped the list of
            # followers for the previous token (since if it has no
            # followers, it would at least have an EndToken follower)
            break

        # build up the weights for the next token based on
        # occurrence-counts in the source data * the length weight
        weights = {}
        for h, f_weights in cached_hashes.iteritems():
            for tok, weight in f_weights.iteritems():
                weights[tok] = (weights.get(tok, 0)
                                + weight * chain_weights[hashes[h]-1])

        # now with the finished weights, build a list by duplicating
        # the items according to their weight. So given {a: 2, b: 3},
        # generate the list [a, a, b, b, b]
        weighted_list = []
        for tok, weight in weights.iteritems():
            weighted_list.extend([tok] * weight)

        next = random.choice(weighted_list)

        if next == EndToken.tok:
            break

        token = Token(next)
        yield token

        lb.append(token)

def create_sentences(cache, length):
    """Create chains with create_chain and yield lines that look like
       English sentences"""
    while True:
        chain = limit(create_chain(cache), length)
        yield ''.join(Token.detokenize(chain))

def main(memc, lim = None):
    cache = Cache(memc)
    lim = int(lim)

    try:
        for x in limit(create_sentences(cache, 100), lim):
            print x
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main(*sys.argv[1:])
