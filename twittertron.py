#!/usr/bin/env python

import re
import sys
import time

import twitter
import urllib2 # python-twitter throws exceptions from here

from memcov import Cache, save_chains, create_sentences, limit

MAX_LENGTH = 140

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

    elif op == 'commands':
        process_commands(cache, api)

    elif op == 'tweet':
        while True:
            x = make_tweet(cache)
            if x:
                print 'tweeting: %r' % x
                api.PostUpdate(x)

                time.sleep(60*60) # post one per hour

    else:
        raise ValueError('unkown op %r?' % op)

def make_tweet(cache):
    for x in create_sentences(cache, 100):
        x = x.encode('utf-8')[:MAX_LENGTH].strip()
        return x

follow_cmd_re = re.compile('^follow @?([A-Za-z0-9_]+)$')
tweetme_cmd_re = re.compile('^tweetme$')
def process_commands(cache, api):

    while True:
        dms = api.GetDirectMessages()
        seen = cache.get_multi(map(_seen_key, dms))
        dms = filter(lambda dm: _seen_key(dm) not in seen, dms)

        for dm in dms:
            cache.set(_seen_key(dm), True)

            follow_cmd_match = follow_cmd_re.match(dm.text.lower())
            tweetme_cmd_match = not follow_cmd_match and tweetme_cmd_re.match(dm.text.lower())

            if follow_cmd_match:
                # one of our friends has given us the command to
                # follow someone else
                newfriendname = follow_cmd_match.group(1)
                try:
                    print '%r has instructed me to follow %r' % (db.sender_screen_name,
                                                                 newfriendname)
                    api.CreateFriendship(newfriendname)

                except (ValueError, twitter.TwitterError, urllib2.HTTPError), e:
                    print "couldn't follow %r" % (newfriendname,)

                else:
                    status = load_user(cache, api, newfriendname)
                    save_chains(cache, status)

            elif tweetme_cmd_match:
                # someone wants us to send them a one-time tweet
                tweet = make_tweet(cache)
                if tweet:
                    print 'Tweeting to %r: %r' % (dm.sender_screen_name, tweet)
                    try:
                        api.PostDirectMessage(dm.sender_screen_name, tweet)
                    except (ValueError, twitter.TwitterError, urllib2.HTTPError), e:
                        print "couldn't tweetme to %r (%r)" % (dm.sender_screen_name, tweet)

            else:
                print 'Unrecognised message from %r: %r' % (dm.sender_screen_name, dm.text)

        time.sleep(60)

def get_twitter_status(cache, api):
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

                if s.user.screen_name.lower() != api._username.lower():
                    text = s.text.encode('utf8')
                    print 'Learning from %s: %r' % (s.user.screen_name, text)
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
            print 'Learning from %s: %r' % (newfriendname, text)
            yield s.text.encode('utf-8')

def _seen_key(i):
    return str('seen_%s' % i.id)

if __name__=='__main__':
    main(*sys.argv[1:])
