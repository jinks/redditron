#!/usr/bin/python
# -*- coding: utf-8 -*-
import feedparser
import gobject
import random
import sys
import twitter
import urllib2
from markov import save_chains, create_sentences
from backends import Redis as Cache

urls = ['http://newsrss.bbc.co.uk/rss/newsonline_world_edition/africa/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/americas/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/asia-pacific/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/europe/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/middle_east/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/south_asia/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/uk_news/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/business/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/health/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/science/nature/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/technology/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/entertainment/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/talking_point/rss.xml',
        'http://newsrss.bbc.co.uk/rss/newsonline_world_edition/uk_news/magazine/rss.xml']

def parse_feed(cache, url):
    bodies = {}
    stat = []
    feed = feedparser.parse(url)
    for item in feed.entries:
        bodies[item['id']] = item.summary
        stat.append(len(item.summary.split(" ")))
    for k in cache.seen_iterator(bodies.keys()):
        body = bodies[k]
        print 'Learning from %r' % (body,)
        yield body
    print "Avg len:", sum(stat)/len(stat)

def update(cache):
    url = urls.pop(0)
    urls.append(url)
    news = parse_feed(cache, url)
    save_chains(cache, news)
    return True

MAX_LENGTH = 140
def tweet(cache, api):
    x = make_tweet(cache)
    if x:
        print 'tweeting: %r' % x
        try:
            api.PostUpdate(x)
            gobject.timeout_add(random.randint(30 * 60 * 1000, 60 * 60 * 1000), tweet, cache, api) # post one per rand(30-60) min
        except urllib2.HTTPError, e:
            print "Couldn't tweet, will retry", e
            gobject.timeout_add(120 * 1000, tweet, cache, api)
    else:
        gobject.timeout_add(120 * 1000, tweet, cache, api)
    return False # disable running gobject timer


def make_tweet(cache):
    for x in create_sentences(cache, 20):
        x = x.encode('utf-8')[:MAX_LENGTH].strip()
        return x

def main(memc, username = None, password=None):
    cache = Cache(memc)
    if username and password:
        print "User: %s ; PW: %s" %(username, '*' * len(password))
        api = twitter.Api(username=username,
                          password=password)
        gobject.timeout_add(1000, tweet, cache, api)
    else:
        api = twitter.Api()

    update(cache)
    gobject.timeout_add(300000, update, cache) # update 1 feed every 5 minutes
    loop = gobject.MainLoop()
    loop.run()

if __name__ == '__main__':
    main(*sys.argv[1:])

# vim: set sw=4 sts=4 et tw=132 :

