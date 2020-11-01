import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import pandas as pd
import demoji
import re
import pandas as pd

class Yt_scraper:
    def __init__(self):
        pass

    def get_access(self):
        # Provide path to the client_secret.json file which will be useful inthe authorization
        CLIENT_SECRETS_FILE = "client_secret.json"

        # Restrict Access and set YouTube Parameters
        SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
        API_SERVICE_NAME = 'youtube'
        API_VERSION = 'v3'

        # Build the service and get the access token
        def get_authenticated_service():
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_console()
            return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)

        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        service = get_authenticated_service()

        return service

    def search_yt(self, service):
        # Set the search query video of which comments will be extracted
        query = "jokowi maruf"
        query_results = service.search().list(part = 'snippet',q = query,
                                            order = 'relevance', 
                                            type = 'video',
                                            relevanceLanguage = 'id',
                                            safeSearch = 'moderate').execute()
        query_results['items']

        # Extract video details i.e. videoID, channel Name, videoTitle, videoDescription
        video_id = []
        channel = []
        video_title = []
        video_desc = []
        for item in query_results['items']:
            video_id.append(item['id']['videoId'])
            channel.append(item['snippet']['channelTitle'])
            video_title.append(item['snippet']['title'])
            video_desc.append(item['snippet']['description'])

        self.video_id = video_id
        self.channel = channel
        self.video_title = video_title
        self.video_desc = video_desc
        # Hasil bisa dipastikan berbeda ketika fungsi ini di run pada hari lain
        print(video_id)
        print(channel)
        print(video_title)

        return service

    def comments_yt01(self, service):
        # Hasil bisa dipastikan berbeda ketika fungsi ini di run pada hari lain
        # Ambil comment dari list video yang pertama, dengan video_id = '9hqWUmyLULo' channel Najwa Shihab
        video_id = self.video_id[0]
        channel = self.channel[0]
        video_title = self.video_title[0]
        video_desc = self.video_desc[0]
        
        # Extract Comments
        video_id_pop = []
        channel_pop = []
        video_title_pop = []
        video_desc_pop = []
        comments_pop = []
        comment_id_pop = []
        reply_count_pop = []
        like_count_pop = []

        comments_temp = []
        comment_id_temp = []
        reply_count_temp = []
        like_count_temp = []

        nextPage_token = None

        while 1:
            response = service.commentThreads().list(
                                part = 'snippet',
                                videoId = video_id,
                                maxResults = 100, 
                                order = 'relevance', 
                                textFormat = 'plainText',
                                pageToken = nextPage_token
                                ).execute()

            nextPage_token = response.get('nextPageToken')
            for item in response['items']:
                comments_temp.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])
                comment_id_temp.append(item['snippet']['topLevelComment']['id'])
                reply_count_temp.append(item['snippet']['totalReplyCount'])
                like_count_temp.append(item['snippet']['topLevelComment']['snippet']['likeCount'])
                comments_pop.extend(comments_temp)
                comment_id_pop.extend(comment_id_temp)
                reply_count_pop.extend(reply_count_temp)
                like_count_pop.extend(like_count_temp)

                video_id_pop.extend([video_id]*len(comments_temp))
                channel_pop.extend([channel]*len(comments_temp))
                video_title_pop.extend([video_title]*len(comments_temp))
                video_desc_pop.extend([video_desc]*len(comments_temp))

            if nextPage_token is None:
                break
        
        # Store comments in CSV file
        output_dict = {
            'Channel': channel_pop,
            'Video Title': video_title_pop,
            'Video Description': video_desc_pop,
            'Video ID': video_id_pop,
            'Comment': comments_pop,
            'Comment ID': comment_id_pop,
            'Replies': reply_count_pop,
            'Likes': like_count_pop
            }

        df_1 = pd.DataFrame(output_dict, columns = output_dict.keys())
        print(df_1)

        # Setelah diamati sekilas, terdapat comment yang terduplicat, cek ada berapa komen yang terduplicat
        duplicates = df_1[df_1.duplicated("Comment ID")]
        print("Jumlah komentar duplikat di dataframe =", duplicates.shape[0])
        print('---------------------------')
        print("jumlah komentar unik di dataframe =", df_1.shape[0] - duplicates.shape[0])
        print('---------------------------')
        final_df1 = df_1.drop_duplicates(subset=['Comment'])
        print(final_df1)

        # save to csv
        final_df1.to_csv("commentYT_01.csv",index = False)
        print('data sudah tersimpan')

    def comments_yt02(self, service):
        # Hasil bisa dipastikan berbeda ketika fungsi ini di run pada hari lain
        # Ambil comment dari list video yang kedua, dengan video_id = 'UxqLHmp_H9c' channel ILC Tv One
        video_id = self.video_id[1]
        channel = self.channel[1]
        video_title = self.video_title[1]
        video_desc = self.video_desc[1]
        
        # Extract Comments
        video_id_pop = []
        channel_pop = []
        video_title_pop = []
        video_desc_pop = []
        comments_pop = []
        comment_id_pop = []
        reply_count_pop = []
        like_count_pop = []

        comments_temp = []
        comment_id_temp = []
        reply_count_temp = []
        like_count_temp = []

        nextPage_token = None

        while 1:
            response = service.commentThreads().list(
                                part = 'snippet',
                                videoId = video_id,
                                maxResults = 100, 
                                order = 'relevance', 
                                textFormat = 'plainText',
                                pageToken = nextPage_token
                                ).execute()

            nextPage_token = response.get('nextPageToken')
            for item in response['items']:
                comments_temp.append(item['snippet']['topLevelComment']['snippet']['textDisplay'])
                comment_id_temp.append(item['snippet']['topLevelComment']['id'])
                reply_count_temp.append(item['snippet']['totalReplyCount'])
                like_count_temp.append(item['snippet']['topLevelComment']['snippet']['likeCount'])
                comments_pop.extend(comments_temp)
                comment_id_pop.extend(comment_id_temp)
                reply_count_pop.extend(reply_count_temp)
                like_count_pop.extend(like_count_temp)

                video_id_pop.extend([video_id]*len(comments_temp))
                channel_pop.extend([channel]*len(comments_temp))
                video_title_pop.extend([video_title]*len(comments_temp))
                video_desc_pop.extend([video_desc]*len(comments_temp))

            if nextPage_token is None:
                break
        
        # Store comments in CSV file
        output_dict = {
            'Channel': channel_pop,
            'Video Title': video_title_pop,
            'Video Description': video_desc_pop,
            'Video ID': video_id_pop,
            'Comment': comments_pop,
            'Comment ID': comment_id_pop,
            'Replies': reply_count_pop,
            'Likes': like_count_pop
            }

        df_2 = pd.DataFrame(output_dict, columns = output_dict.keys())
        print(df_2)

        # Setelah diamati sekilas, terdapat comment yang terduplicat, cek ada berapa komen yang terduplicat
        duplicates = df_2[df_2.duplicated("Comment ID")]
        print("Jumlah komentar duplikat di dataframe =", duplicates.shape[0])
        print('---------------------------')
        print("jumlah komentar unik di dataframe =", df_2.shape[0] - duplicates.shape[0])
        print('---------------------------')
        final_df2 = df_2.drop_duplicates(subset=['Comment'])

        # save to csv
        final_df2.to_csv("commentYT_02.csv", index = False)
        print('data sudah tersimpan')

    def concat_comment(self, file1, file2, raw_youtube):
        file1_raw = pd.read_csv(file1)
        file2_raw = pd.read_csv(file2)

        # concat dataframe
        all_file = pd.concat([file1_raw['Comment'], file2_raw['Comment']], ignore_index=True)

        # remove emoji
        demoji.download_codes()
        all_file = all_file.apply(lambda x: demoji.replace(x,"").lower())
        # pd.set_option('display.max_rows', all_file.shape[0]+1)
        print(all_file)

        # save to csv
        all_file.to_csv(raw_youtube, index=False)
        print('data sudah tersimpan')

file1 = "commentYT_01.csv"
file2 = "commentYT_02.csv"
raw_youtube = "raw_youtube.csv"

app = Yt_scraper()
data = app.get_access()
search_list = app.search_yt(data)
app.comments_yt01(search_list)
app.comments_yt02(search_list)
app.concat_comment(file1, file2, raw_youtube)

''' 
*** DISCLAIMER ***
Fungsi ini dijalankan dengan menggunakan Client ID OAuth 2.0 pada console developer google API,
serta menyertakan file client_secret.json berada dalam satu folder dengan file ini.
Klik link, selanjutnya login menggunakan akun email, dan meminta hak akses guna mengizinkan akun email untuk mendapatkan authorization code.
*** ROBY RIYANTO ***
'''