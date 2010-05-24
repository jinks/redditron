#!/usr/bin/env python
from markov import Token

class Cassandra(object):
    # Types:
    # * tokenlist() -> [Token]
    # * hashedtoken()

    def __init__(self, init_args):
        import pycassa
        import cassandra.ttypes

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
