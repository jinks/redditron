#!/usr/bin/env python

import sys
from urllib2 import urlopen
import simplejson as json
import time

from cassacov import Cache, save_chains

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
        bodies = {}

        for cm in cms:
            cm = cm['data']
            if cm.get('body', None):
                bodies[cm['id']] = cm['body']

        for k in cache.seen_iterator(bodies.keys()):
            body = bodies[k]
            # we have to pick between being able to sometimes yield
            # the same item twice, or sometimes never yielding an item
            # (e.g. if an exception is thrown before control is passed
            # back to us). we've chosen the former here
            yield body

        time.sleep(35)

if __name__=='__main__':
    main(*sys.argv[1:])
