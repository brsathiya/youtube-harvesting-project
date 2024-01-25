import streamlit as st
import pandas as pd
import googleapiclient.discovery
from googleapiclient.discovery import build
import pymongo
from datetime import datetime
import isodate 
import pymysql
from sqlalchemy import create_engine 
from streamlit_option_menu import option_menu

#Mongodb python Connectivity

client = pymongo.MongoClient('mongodb+srv://brsathiya:root@cluster0.ryric7j.mongodb.net/')
mydb=client['youtubedata']  #database name
information=mydb.youtubedetails #collection name 

#Sql python connectivity

#creating engine 
engine=create_engine('mysql+pymysql://root:root1234@localhost/youtube',echo=False)

#cur.execute("create database youtube")
myconnection=pymysql.connect(host="localhost",user='root',passwd='root1234',database='youtube')
cur=myconnection.cursor()

with st.sidebar:
    selected = option_menu(None, ["Home","Migrate into mongodb and MySql","SQL Queries"], 
                           icons=["house-door-fill","cloud-upload","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "5000px"},
                                   "nav-link-selected": {"background-color": "red"}})

def api_connect():
    api_srevice_name = 'youtube'
    api_version = 'v3'
    API_KEY ='AIzaSyBqvRLUIzHV19D1iBK_NjQsbzY8VU3dcWo'
    youtube = googleapiclient.discovery.build(api_srevice_name,api_version,developerKey=API_KEY)
    return youtube

#Method 1 - Getting channel_details by youtube api

def channeldata(youtube,channel_id):
    finalchanneldata=[]
    request= youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
     )
    channel_data = request.execute()

    for i in range(len(channel_data.items())):
        finalinfo=dict(channel_id=channel_data['items'][0]['id'],
                       channel_name=channel_data['items'][0]['snippet']['title'],
                       channel_description=channel_data['items'][0]['snippet']['description'],
                       channel_view=channel_data['items'][0]['statistics']['viewCount'],
                       channel_subscriber=channel_data['items'][0]['statistics']['subscriberCount'],
                       channel_playlistid=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                       channel_videocount=channel_data['items'][0]['statistics']['videoCount'])
        finalchanneldata.append(finalinfo)
    
    return finalchanneldata



#Getting playlist id for to get playlist details from the youtube by using youtube api



#Method 2 - Getting playlist_details by youtube api

def playlist(youtube,channel_id):
    playlist=[]
    playlist_request = youtube.playlists().list(
         part="snippet,contentDetails",
         channelId=channel_id,
        maxResults=50
    )
    playlist_response = playlist_request.execute()
    

    for i in range(len(playlist_response.items())):
        play=dict(playlist_id=playlist_response['items'][0]['id'],
                  channel_id=playlist_response['items'][0]['snippet']['channelId'],
                  playlist_title=playlist_response['items'][0]['snippet']['localized']['title'],
                  playlist_count=playlist_response['items'][0]['contentDetails']['itemCount'],
                  playlist_publishedate=playlist_response['items'][0]['snippet']['publishedAt'])
        playlist.append(play)

    next_page_token=playlist_response.get('nextPageToken')
    more_pages=True
    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            playlist_request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response=playlist_request.execute()
            for i in range(len(playlist_response.items())):
                play=dict(playlist_id=playlist_response['items'][0]['id'],
                          channel_id=playlist_response['items'][0]['snippet']['channelId'],
                          playlist_count=playlist_response['items'][0]['contentDetails']['itemCount'],
                          playlist_title=playlist_response['items'][0]['snippet']['localized']['title'],
                          playlist_publishedate=playlist_response['items'][0]['snippet']['publishedAt'])
                playlist.append(play)
            next_page_token=playlist_response.get('nextPageToken')
    return playlist


#Method 3 - Getting video_id by using youtube api

def videoids_details(youtube,a):
    video_ids=[]
    request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=a,
            maxResults=50
    )
    channel_playlist = request.execute()
    
    for i in range(len(channel_playlist.items())):
        video_ids.append(channel_playlist['items'][0]['contentDetails']['videoId'])
        
    next_page_token=channel_playlist.get('nextPageToken')
    more_pages=True
    while more_pages:
        if next_page_token is None:
            more_pages=False
        else:
            request = youtube.playlistItems().list(
                       part="snippet,contentDetails",
                       playlistId=a,
                       maxResults=50,
                       pageToken=next_page_token
            )
            channel_playlist = request.execute()
            for i in range(len(channel_playlist.items())):
                video_ids.append(channel_playlist['items'][0]['contentDetails']['videoId'])

            next_page_token=channel_playlist.get('nextPageToken')
    return  video_ids

#videoids

#Method 4- Getting video_details by youtube api

def videodetails(youtube,videoids):
    all_video_stats=[]
    for i in range(0,len(videoids),50):
        video_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(videoids[i:i+50])
              )
        
        video_response= video_request.execute()
        
        for i in range(len(video_response.items())):
            video_stats=dict(video_id=video_response['items'][0]['id'],
                             channel_name=video_response['items'][0]['snippet']['channelTitle'],
                             video_title=video_response['items'][0]['snippet']['title'],
                             video_description=video_response['items'][0]['snippet']['description'],
                             video_published=video_response['items'][0]['snippet']['publishedAt'],
                             video_viewcount=video_response['items'][0]['statistics']['viewCount'],
                             video_likecount=video_response['items'][0]['statistics']['likeCount'],
                             video_commentcount=video_response['items'][0]['statistics']['commentCount'],
                             video_duration=video_response['items'][0]['contentDetails']['duration'],
                             video_thumbnail=video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                             video_captionStatus=video_response['items'][0]['contentDetails']['caption']
                            )
                             
                            
            all_video_stats.append(video_stats)
      
    return all_video_stats

#video_details

#Method 5 - Get comment_details by youtube api

def comments_details(youtube,video_id ):
    all_comments = []
    for video_id in video_id:
        try:
            comment_request = youtube.commentThreads().list(
                      part="snippet",
                      videoId=video_id,
                      maxResults=5
            )
            comment_response = comment_request.execute()
            for i in comment_response.items():
                comment=dict(comment_id = comment_response['items'][0]['snippet']['topLevelComment']['id'],
                             Video_id = comment_response['items'][0]['snippet']['videoId'],
                             comment_text= comment_response['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal'],
                             comment_author = comment_response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'], 
                             comment_publishedAt =comment_response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt'] 
                 )
                all_comments.append(comment)
    
        except:
            print('Could not get comments for video ' + video_id)
    return all_comments

#comment_details

#Method 6 - Merging all data into one 

def main(channel_id):
    youtube = api_connect()
    CD =  channeldata(youtube,channel_id) # Channel_details
    playlist_details = playlist(youtube,channel_id) #playlist_details
    videoids=videoids_details(youtube,CD[0]['channel_playlistid']) #videoids details
    video_details = videodetails(youtube,videoids)  # get_video_details
    comment_details = comments_details(youtube,videoids)  # get_comments_in_video

    data = {"Channel_Details": CD,
            "Playlist_Details": playlist_details,
            "Video_Details": video_details,
            "Comments_Details": comment_details}
    return data


#mycollection=mydb['youtubedetails']


# HOME PAGE    
if selected == "Home":
    # Title Image  
    st.title("DATA HARVESTING USING YOUTUBE API")
    st.markdown("## To create youtube api check out this [link](https://www.youtube.com/watch?v=TE66McLMMEw)")
    #st.image("title.png")
    #col1,col2 = st.columns(2,gap= 'medium')
    st.markdown("## :green[DOMAIN] : Youtube")
    st.markdown("## :orange[Technologies used]: Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.markdown("## :blue[Overview] : First we have to fetch the data from the youtube by using google youtube api. After fetching the data from the youtube we have to push the data into mongodb. Once we finished these steps then again we have to fetch the data from the mongodb and make it as a table format then insert into the MySql")
                                                                                 

#Get data and transform page

if selected=="Migrate into mongodb and MySql":
    st.title("Data harvesting")
    channel_id=st.text_input('Enter your channelID')
    if st.button('Get Data'):
        with st.spinner('Processing....'):
            st.write(main(channel_id))

    mycollection=mydb['youtubedetails']
    if st.button('Migrate to Mongodb'):
        with st.spinner('Please Wait for while...'):
            data=main(channel_id)
            count=False
            for i in mycollection.find():
                if i['Channel_Details'][0]['channel_id']==channel_id:
                    st.warning('channel already exist',icon="⚠️")
                    count=True
                    break

            if count==False:
                information.insert_one(data)
                st.success('Migrate into mongodb',icon="✅")

    
    st.markdown('## Select  channel name to insert into MySql')


    #Getting channel name from the mongodb
    ch=[]
    for i in information.find():
        ch_details=i['Channel_Details'][0]['channel_name']
        ch.append(ch_details)
        
    user_input=st.selectbox('select channel name',options=ch)
    
    channel_data=[]
    channel_sql=[]
    playlist_sql=[]
    video_sql=[]
    comment_sql=[]
    
    #append the channel details choose by the user 
    for i in mycollection.find():
        if i['Channel_Details'][0]['channel_name']==user_input:
            channel_data.append(i)
    if st.button('insert into sql'):
        cur.execute ('select channel_name from channel_details')
        channelnames=[i[0] for i in cur.fetchall()]
        if user_input in channelnames:
             st.warning('channel already inserted into MySql',icon="⚠️")
        else:
            with st.spinner('please wait for a while'):
                    #Reterving channel data
                    channel_details={'channel_name':channel_data['Channel_Details'][0]['channel_name'],
                                     'channel_id':channel_data['Channel_Details'][0]['channel_id'],
                                     'channel_description':channel_data['Channel_Details'][0]['channel_description'],
                                     'channel_view':channel_data['Channel_Details'][0]['channel_view'],
                                     'channel_subscriber':channel_data['Channel_Details'][0]['channel_subscriber'],
                                     'channel_playlsitid':channel_data['Channel_Details'][0]['channel_playlistid'],
                                     'channel_videocount':channel_data['Channel_Details'][0]['channel_videocount']}
                    channel_sql.append(channel_details)

                    #Reterving playlist data
                    for j in channel_data[0]['Playlist_Details']:
                         playlist_details={'playlist_id':j['playlist_id'],
                                           'channel_id':j['channel_id'],
                                           'playlist_count':j['playlist_count'],
                                           'playlist_title':j['playlist_title'],
                                           'playlist_publishedate':j['playlist_publishedate']}
                         playlist_sql.append(playlist_details)

                    #Reterving video data  
                    for j in channel_data[0]['Video_Details']:
                        video_details={'video_id':j['video_id'],
                                       'channel_name':j['channel_name'],
                                       'video_title':j['video_title'],
                                       'video_description':j['video_description'],
                                       'video_published':j['video_published'],
                                       'video_viewcount':j['video_viewcount'],
                                       'video_likecount':j['video_likecount'],
                                       'video_commentcount':j['video_commentcount'],
                                       'video_duration':j['video_duration'],
                                       'video_thumbnail':j['video_thumbnail']}
                        video_sql.append(video_details)
                        
                    #Reterving comment data
                    for j in channel_data[0]['Comments_Details']:
                        comment_details={'comment_id':j['comment_id'],
                                         'video_id':j['Video_id'],
                                         'comment_text':j['comment_text'],
                                         'comment_author':j['comment_author'],
                                         'comment_publishedAt':j['comment_publishedAt']}
                        comment_sql.append(comment_details)


                    #converting channel details into table
                    channel_sql=pd.DataFrame(channel_sql)  #channel_sql to dataframe
                    playlist_sql=pd.DataFrame(playlist_sql) #playlist_sql to dataframe
                    video_sql=pd.DataFrame(video_sql) #video_sql to dataframe
                    comment_sql=pd.DataFrame(comment_sql) #comment_swl to dataframe


                    #1.converting datatypes in channel_details
                    channel_sql['channel_view']=pd.to_numeric(channel_sql['channel_view'])
                    channel_sql['channel_subscriber']=pd.to_numeric(channel_sql['channel_subscriber'])
                    channel_sql['channel_videocount']=pd.to_numeric(channel_sql['channel_videocount'])

                    #2.converting datatype in playlist_details:

                    playlist_sql['playlist_count']=pd.to_numeric(playlist_sql['playlist_count'])
                    playlist_sql['playlist_publishedate']=pd.to_datetime(playlist_sql['playlist_publishedate'])

                    #3.converting datatype in video_details:

                    video_sql['video_published']=pd.to_datetime(video_sql['video_published'])
                    video_sql['video_viewcount']=pd.to_numeric(video_sql['video_viewcount'])
                    video_sql['video_likecount']=pd.to_numeric(video_sql['video_likecount'])
                    video_sql['video_commentcount']=pd.to_numeric(video_sql['video_commentcount'])

                    for i in range(len(video_sql['video_duration'])):
                        duration=isodate.parse_duration(video_sql['video_duration'].loc[i])
                        seconds=duration.total_seconds()
                        video_sql.loc[i,'video_duration']=int(seconds)

                    video_sql['video_duration']=pd.to_numeric(video_sql['video_duration'])

                    #4.converting datatype in comment_details:

                    comment_sql['comment_publishedAt']=pd.to_datetime(comment_sql['comment_publishedAt'])

                    #insert data into mysql

                    channel_sql.to_sql('channel_details',engine,if_exists='append',index=False) #channel data into sql
                    playlist_sql.to_sql('playlist_details',engine,if_exists='append',index=False) #playlist data into sql
                    video_sql.to_sql('video_details',engine,if_exists='append',index=False) #video data into sql
                    comment_sql.to_sql('comment_details',engine,if_exists='append',index=False) #comment data into sql
                    st.success('Successfully data push into MySql',icon="✅")

# MYSQL Queries page

if selected=='SQL Queries':
    st.write("## :black[Question are given below choose one you will get the answers]")
    question=['1. What are the names of all the videos and their corresponding channels?',
              '2. Which channels have the most number of videos, and how many videos do they have?',
              '3. What are the top 10 most viewed videos and their respective channels?',
              '4. How many comments were made on each video, and what are their corresponding video names?',
              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
              '6. What is the total number of likes for each video, and what are their corresponding video names?',
              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
              '8. What are the names of all the channels that have published videos in the year 2022?',
              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
              '10. Which videos have the highest number of comments, and what are their corresponding channel names?']
    questions=st.selectbox('Select questions',question)
    
    
    #1 question 
    if questions=='1. What are the names of all the videos and their corresponding channels?':
        if st.button('Get solution'):
            cur.execute("select video_title,channel_name from video_details")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_title','channel_name'],index=range(1, len(df) + 1)))
            st.success("DONE", icon="✅")


    #2 question
    elif questions=='2. Which channels have the most number of videos, and how many videos do they have?':
        if st.button('Get solution'):
            cur.execute("select channel_name,channel_videocount from channel_details order by channel_videocount desc")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','videocount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")


    #3 question
    elif questions=='3. What are the top 10 most viewed videos and their respective channels?':
        if st.button('Get solution'):
            cur.execute("select channel_name,video_viewcount from video_details order by video_viewcount desc limit 10")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_viewcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")
            

    #4 question
    elif questions=='4. How many comments were made on each video, and what are their corresponding video names?':
        if st.button('Get solution'):
            cur.execute("select video_title,video_commentcount from video_details order by video_viewcount desc")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_name','video_commentcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")
    
    #5 question
    elif questions=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        if st.button('Get solution'):
            cur.execute("select channel_name,video_likecount from video_details order by video_likecount desc limit 10")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_likecount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")


    #6 question
    elif questions=='6. What is the total number of likes for each video, and what are their corresponding video names?':
        if st.button('Get solution'):
            cur.execute("select video_title as video_name, video_likecount as Like_count from video_details;")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['video_name','likecount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")


    #7 question
    elif  questions=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        if st.button('Get solution'):
            cur.execute("select channel_name,channel_view from channel_details order by channel_view desc")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','channel_view'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")


    #8 question
    elif questions=='8. What are the names of all the channels that have published videos in the year 2022?':
        if st.button('Get solution'):
            cur.execute("select distinct(channel_name) from video_details where year(video_published)=2022 order by channel_name")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    
    #9 question
    elif questions=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        if st.button('Get solution'):
            cur.execute("select channel_name,avg(video_duration) as vd from video_details group by(channel_name) order by vd desc")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','average_duration'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    
    #10 question
    elif questions=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        if st.button('Get solution'):
            cur.execute("select channel_name,video_commentcount from video_details order by video_commentcount desc limit 100")
            df=[i for i in cur.fetchall()]
            st.dataframe(pd.DataFrame(df,columns=['channel_name','video_commentcount'],index=range(1,len(df)+1)))
            st.success("DONE", icon="✅")

    





    
            
 


