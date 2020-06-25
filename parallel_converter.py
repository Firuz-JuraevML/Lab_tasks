# Author: Firuz Juraev

import pandas as pd
from os import listdir
import json_lines
import os
import numpy as np
from multiprocessing import  Pool

def list_files():
    files = []
    counter = 0
    for f in listdir("./"):
        if f.endswith('.' + "gz"):
            if os.path.exists(f[:f.index('.')] + '.csv'):
                continue
            files.append(f)
            counter = counter + 1

    print ("Files: " + str(counter))
    return files


def join_hashtags(hashtags):
    hashtag = ""
    if len(hashtags) > 0:
        for tag in hashtags:
            hashtag = hashtag + ' ' + tag['text']
    return hashtag


def unzip_files(files):
    counter = 0
    for f in files:
        df = pd.DataFrame(columns = ['id', 'created_at', 'text', 'lang', 'hashtags', 'user_id', 'user_name', 'user_location', 'followers_count', 'friends_count', 'listed_count',
        'user_created_at', 'favourites_count', 'verified', 'statuses_count', 'retweet_id', 'retweet_created_at', 'retweet_user_id' ])
        print (f)
        with json_lines.open(f) as file:
            for item in file:
                if (item['lang'] == 'en'):
                    retweet_id = None
                    retweet_created_at = None
                    retweet_user_id = None

                    if 'retweeted_status' in item:
                        retweet_id = item['retweeted_status']['id']
                        retweet_created_at = item['retweeted_status']['created_at']
                        retweet_user_id = item['retweeted_status']['user']['id']

                    new_row = pd.Series(data={'id':item['id'],'created_at':item['created_at'], 'text':item['full_text'],
                    'lang':item['lang'],'hashtags':join_hashtags(item['entities']['hashtags']),'user_id':item['user']['id'],
                    'user_name':item['user']['screen_name'],'user_location':item['user']['location'],
                    'followers_count':item['user']['followers_count'], 'friends_count':item['user']['friends_count'],
                    'listed_count':item['user']['listed_count'], 'user_created_at':item['user']['created_at'],
                    'favourites_count':item['user']['favourites_count'], 'verified':item['user']['verified'],
                    'statuses_count':item['user']['statuses_count'], 'retweet_id':retweet_id,
                    'retweet_created_at':retweet_created_at, 'retweet_user_id':retweet_user_id}, name='')
                    df = df.append(new_row, ignore_index=False)
                    counter = counter + 1
        print (counter)
        counter = 0
        file_name = f[:f.index('.')]
        df.to_csv(file_name + '.csv', index=False)


def parallelize_dataframe(file_list, func, n_cores=15):
    decompiled_apks = []
    df_split = np.array_split(np.array(file_list), n_cores)
    pool = Pool(n_cores)
    decompiled_apks.extend(pool.map(func, df_split))
    pool.close()


if __name__ == '__main__':
    files = list_files()
    parallelize_dataframe(files, unzip_files, 40)
