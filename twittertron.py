#!/usr/bin/env python

import re
import sys
import time

import twitter
import urllib2 # python-twitter throws exceptions from here

from memcov import Cache, save_chains, create_sentences, limit

MAX_LENGTH = 140

def get_twitter_status(cache, api):
    follow_cmd_re = re.compile('^@(%s) follow @?([A-Za-z0-9_]+)$' % api._username)

    def _seen_key(i):
        return str('seen_%s' % i.id)

    last = None

    while True:
        # the plural of status is status
        status = list(api.GetFriendsTimeline(since_id = last, count=200))
        seen = cache.get_multi([_seen_key(s)
                                for s in status])
        status = [s for s in status if _seen_key(s) not in seen]

        if status:
            print '%d new status' % len(status)

            for s in status:
                follow_cmd_match = follow_cmd_re.match(s.text)
                if follow_cmd_match:
                    # one of our friends has given us the command to
                    # follow someone else
                    newfriendname = follow_cmd_match.group(2)
                    try:
                        print 'Attempting to follow %r...' % (newfriendname,)
                        api.CreateFriendship(newfriendname)
                    except (ValueError, twitter.TwitterError, urllib2.HTTPError):
                        api.PostDirectMessage(s.user.name, "i can't follow %r" % newfriendname)
                else:
                    text = s.text.encode('utf8')
                    yield text

            cache.set_multi(dict((_seen_key(s), True)
                                 for s in status))

            last = status[-1].id

        # 35 looks to be optimal for preventing rate-limiting
        # http://apiwiki.twitter.com/Rate-limiting
        time.sleep(35)

def main(memc, op, username = '', password = ''):
    cache = Cache(memc)

    if username and password:
        api = twitter.Api(username=username,
                          password=password)
    else:
        api = twitter.Api()

    if op == 'save':
        status = get_twitter_status(cache, api)
        save_chains(cache, status)
    elif op == 'tweet':
        for x in create_sentences(cache, 100):
            x = x.encode('utf-8')[:MAX_LENGTH].strip()
            if x:
                print 'tweeting: %r' % x
                api.PostUpdate(x)

                time.sleep(60*60) # post one per hour

    else:
        raise ValueError('unkown op %r?' % op)

if __name__=='__main__':
    main(*sys.argv[1:])
