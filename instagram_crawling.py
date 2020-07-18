import pandas as pd 
import instaloader

# Authentication 
L = instaloader.Instaloader(download_pictures=False, download_videos=False, download_video_thumbnails=False, download_geotags=False)

# Optionally, login or load session
L.login('login', 'password')        # (login)
L.interactive_login('login')      # (ask password on terminal)
L.load_session_from_file('login')

ids = pd.read_json("list_shortcode.json", typ='series')
#instagram_ids = ids.to_frame()

# post = instaloader.Post.from_shortcode(L.context, 'B7L8JlkBIwl') 
# L.download_post(post, target="coronavirus") 

for i in ids[:4700]: 
    try: 
        post = instaloader.Post.from_shortcode(L.context, i) 
        L.download_post(post, target="coronavirus2")
    except: 
        print("Cannot load post")
