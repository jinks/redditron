#!/usr/bin/env python

import re
import sys
import time

import twitter
import urllib2 # python-twitter throws exceptions from here

from memcov import Cache, save_chains, create_sentences, limit

MAX_LENGTH = 140

def _seen_key(i):
    return str('seen_%s' % i.id)

def get_twitter_status(cache, api):
    follow_cmd_re = re.compile('^@(%s) follow @?([A-Za-z0-9_]+)$' % api._username.lower())

    last = None

    while True:
        # the plural of status is status
        status = list(api.GetFriendsTimeline(since_id = last, count=200))
        seen = cache.get_multi([_seen_key(s)
                                for s in status])
        status = [s for s in status if _seen_key(s) not in seen]

        if status:
            for s in status:
                # declare that we've seen it immediately so that if it
                # causes us to fail, we don't try again
                cache.set(_seen_key(s), True)

                follow_cmd_match = follow_cmd_re.match(s.text.lower())
                if follow_cmd_match:
                    # one of our friends has given us the command to
                    # follow someone else
                    newfriendname = follow_cmd_match.group(2)
                    try:
                        print 'Attempting to follow %r...' % (newfriendname,)
                        api.CreateFriendship(newfriendname)

                    except (ValueError, twitter.TwitterError, urllib2.HTTPError), e:
                        try:
                            api.PostDirectMessage(s.user.screen_name,
                                                  "i can't follow %r" % newfriendname)
                        except Exception, f:
                            print ("Couldn't reply to %r after failed instruction to follow %r (%r; %r)"
                                   % (s.user.screen_name, newfriendname, e, f))

                    else:
                        new_status = api.GetUserTimeline(newfriendname)
                        for user_status in new_status:
                            cache.set(_seen_key(ss), True)
                            text = user_status.text.encode('utf8')
                            print 'Learning from %s: %r...' % (newfriendname, text)
                            yield text

                elif s.user.screen_name.lower() != api._username.lower():
                    text = s.text.encode('utf8')
                    print 'Learning from %s: %r...' % (s.user.screen_name, text)
                    yield text

            last = status[-1].id

        # 35 looks to be optimal for preventing rate-limiting
        # http://apiwiki.twitter.com/Rate-limiting
        time.sleep(35)

def load_user(cache, api, newfriendname):
    status = api.GetUserTimeline(newfriendname, count=200)
    for s in status:
        if not cache.get(_seen_key(s)):
            cache.set(_seen_key(s), True)
            text = s.text.encode('utf-8')
            print 'Learning from %s: %r...' % (newfriendname, text)
            yield s.text.encode('utf-8')

def main(memc, op, username = '', password = '', newfriendname = ''):
    cache = Cache(memc)

    if username and password:
        api = twitter.Api(username=username,
                          password=password)
    else:
        api = twitter.Api()

    if op == 'save':
        status = get_twitter_status(cache, api)
        save_chains(cache, status)
    elif op == 'load_user':
        status = load_user(cache, api, newfriendname)
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
