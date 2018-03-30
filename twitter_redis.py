import datetime
import os
from time import strftime
from email.utils import parsedate
import json
import ast

import redis
import re
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

#Following keys and tokens are kept empty for privacy

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


class StdOutListener(StreamListener):
   
    count = 0
    redis_errors = 0
    allowed_redis_errors = 3
    twstring = ''
    tweets = []
    r = redis.StrictRedis('localhost',6378)
    total_tweets = 10

    def write_to_redis(self, tw):
        try:
            self.r.lpush('tweets', tw)
        except:
            print 'Problem adding tweets to Redis.'
            self.redis_errors += 1

    def redis_incrmin(self, minute):
        try:
            self.r.incr(minute)
        except:
            print 'Problem incrementing data in Redis.'
            self.redis_errors += 1

    def redis_incrhour(self, hour):
        try:
            self.r.incr(hour)
        except:
            print 'Problem incrementing data in Redis.'
            self.redis_errors += 1   

    def redis_addusers(self, username):
        try:
            self.r.set(username, 1)
        except:
            print 'Problem adding user data in Redis.'
            self.redis_errors += 1        

    def on_data(self, data):
        self.write_to_redis(data)
        dt = data.decode("utf-8")
        json_data = json.loads(dt)
        if "created_at" in json_data:
            timestamp = parsedate(json_data["created_at"])
            timetext = strftime("%Y-%m-%d-%H-%M", timestamp)
            self.redis_incrmin(timetext)
            timehour = strftime("%Y-%m-%d-%H", timestamp)
            self.redis_incrhour(timehour)
        if "user" in json_data:
            if "screen_name" in json_data["user"]:
                self.redis_addusers(json_data["user"]["screen_name"])
                print json_data["user"]["screen_name"]    
        self.count += 1
        if self.count > self.total_tweets:
            return False
        if self.redis_errors > self.allowed_redis_errors:
            print 'Many redis errors'
            return False
        if (self.count % 1000) == 0:
            print 'count is: %s at %s' % (self.count, datetime.datetime.now())
        return True

    def on_error(self, status):
        print "Error"

if __name__ == '__main__':
    print 'Streaming Begins'
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    stream = Stream(auth, listener)
    
    stream.filter(
                track=['bigdata', 'google', 'tech', 'apple', 'news', 'cloud',
                       'python', 'android', 'mobile', 'web']
                )    
    r = redis.StrictRedis('localhost',6378)
    result = r.lrange('tweets', 0, -1)
    p = re.compile(r'@([^\s:]+)')
    
    final_users = set()
    
    for i in result:
        users = p.findall(i)        
        for j in users:
            if "\\" not in j:
                final_users.add(j)
    print 'Streaming Ends'
    