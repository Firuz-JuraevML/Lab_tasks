import pandas as pd 
from os import listdir 
import os


try:
    import lzma
except ImportError:
    from backports import lzma


def list_files(): 
    files = []
    counter = 0
    for f in listdir("./"):
      if f.endswith('.' + "xz"):
        files.append(f)
        counter=counter+1

    print(f"Files: {str(counter)}")
    return files

def converting_to_csv(files):
    counter = 0
    df = pd.DataFrame(columns = ['id', 'shortcode', 'taken_at_timestamp', 'text', 'owner_id', 'owner_posts',
        'owner_followers', 'owner_verified', 'comments_count', 'is_ad'])  

    comment_df = pd.DataFrame(columns = ['id', 'post_id', 'created_at', 'likes_count', 'owner_id', 
        'owner_verified', 'text'])

    for f in files: 
        post_id = None
        file_name = f[:f.index('.')] 

        text = "" 

        try: 
          text = open(file_name + ".txt", "r").read()
        except Exception as e:
          print(e)

        data = pd.read_json(lzma.open(f)) 

        print(f)

        post_id = data['node']['id'] 

        new_row = pd.Series(data={'id':post_id, 'shortcode':data['node']['shortcode'], 
          'taken_at_timestamp':data['node']['taken_at_timestamp'], 'text':text, 'owner_id':data['node']['owner']['id'],
          'owner_posts': data['node']['owner']['edge_owner_to_timeline_media']['count'], 
          'owner_followers':data['node']['owner']['edge_followed_by']['count'], 'owner_verified':data['node']['owner']['is_verified'], 
          'comments_count':data['node']['edge_media_preview_comment']['count'], 'is_ad':data['node']['is_ad']}) 

        df = df.append(new_row, ignore_index=True)

        if data['node']['edge_media_preview_comment']['count'] > 0: 
            try: 
                comments_file = "./" + file_name + "_comments.json"
                data_comments = pd.read_json(comments_file, encoding="utf8")
                for i in range(data_comments['id'].size):
                    row = data_comments.iloc[i]
                    new_comment = pd.Series(data={'id':row['id'], 'post_id':post_id, 'created_at':row['created_at'], 'likes_count':row['likes_count'],
                    'owner_id':row['owner']['id'], 'owner_verified':row['owner']['is_verified'], 'text':row['text']}) 

                    comment_df = comment_df.append(new_comment, ignore_index=True)
            except Exception as e:
              print(e)

        counter = counter + 1


    print(f"Posts: {str(counter)}")
    df.to_csv('instagram_posts.csv', index=False)
    comment_df.to_csv('instagram_posts_comments.csv', index=False)


if __name__ == '__main__':
    files = list_files()
    converting_to_csv(files)
