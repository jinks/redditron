#!/usr/bin/env python

import sys
from urllib2 import urlopen
import simplejson as json
import time

from markov import save_chains
from backends import Cassandra as Cache

def main(memc):
    cache = Cache(memc)
    comments = get_reddit_comments(cache)
    save_chains(cache, comments)

def get_reddit_comments(cache):
    """Continually yield new comment-bodies from reddit.com"""
    url = 'http://www.reddit.com/comments.json?limit=100'

    while True:
        s = urlopen(url).read().decode('utf8')

        js = json.loads(s)
        cms = js['data']['children']

        for cm in cache.seen_iterator(cms, lambda cm: cm['data']['id']):
            body = cm['data']['body']
            author = cm['data']['author']
            print 'Learning from %s: %r' % (author, body)
            yield body

        time.sleep(35)

if __name__=='__main__':
    main(*sys.argv[1:])
