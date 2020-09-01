import streamlit as st
import pandas as pd
import numpy as np
import json
from pandas.io.json import json_normalize
import plotly_express as px
import os
import requests
import io
from io import StringIO
import boto3
import time

def _max_width_():
    max_width_str = f"max-width: 2000px;"
    st.markdown(
        f"""
    <style>
    .reportview-container .main .block-container{{
        {max_width_str}
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )


def fut_api(ut_sid):
    a = 1
    initial = 0
    after = 1
    df = pd.DataFrame()
    while initial!=after:
        initial = df.index.size
        url = 'https://utas.external.s3.fut.ea.com/ut/game/fifa20/club?sort=desc&sortBy=value&type=player&start='+str(a)+'&count=250'
        headers = {"X-UT-SID": ut_sid}
        req = requests.get(url, headers=headers)
        d=pd.read_json(req.text)
        try:
            df = pd.concat([df, json_normalize(d['itemData'])],sort=False)
        except:
            df=json_normalize(d['itemData'])
        a += 250
        after = df.index.size

    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')

    return df

def file_join(df, player_file):
    with open(player_file) as data_file:
        data = json.load(data_file)
    df_p=pd.io.json.json_normalize(data['Players'])

    df_l=pd.io.json.json_normalize(data['LegendsPlayers'])

    df_player=df_p.append(df_l,sort=False)
    df_player.columns = df_player.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('.', '_').str.replace(')', '')

    df=pd.merge(df,df_player,how='inner',left_on='assetid',right_on='id')
    df[['pace','sho','pass','dri','def','phy']] = pd.DataFrame(df.attributearray.values.tolist(), index=df.index)
    df[['games','goals','yellows','reds','tbd']] = pd.DataFrame(df.statsarray.values.tolist(), index= df.index)
    df['points'] = df['goals'] + df['assists']
    df['ppg'] = df['points'] / df['games']
    df['ppg']=df['ppg'].fillna(0)
    df['player_name'] = df['f'] + ' ' + df['l']
    df=df[['player_name','preferredposition','rating','pace','sho','pass','dri','def','phy','games','goals','assists','yellows','points','ppg','formation','untradeable','owners','cardsubtypeid','lastsaleprice','fitness','teamid','leagueid','nation','rareflag','playstyle','loyaltybonus','pile','skillmoves','weakfootabilitytypecode','attackingworkrate','defensiveworkrate','trait1','trait2','groups']]
    df_club = pd.read_csv('./fifa_club_db.csv')
    df_nation = pd.read_csv('./fifa_nation_db.csv')
    df_league = pd.read_csv('./fifa_league_db.csv')
    ##FOR LOCAL TESTING
    #df_club = pd.read_csv('../fifa-ultimate-team/fifa_club_db.csv')
    #df_nation = pd.read_csv('../fifa-ultimate-team/fifa_nation_db.csv')
    #df_league = pd.read_csv('../fifa-ultimate-team/fifa_league_db.csv')
    df=pd.merge(df, df_club, how='left', left_on='teamid', right_on='club_id')
    df=pd.merge(df, df_nation, how='left', left_on='nation', right_on='nation_id')
    df=pd.merge(df, df_league, how='left', left_on='leagueid', right_on='league_id')

    return df

def save_to_s3(df, team_name):
    bucket = 'fifaultimateteam'
    team_name = team_name.strip().lower().replace(' ', '_').replace('.', '_').replace(')', '').replace('&','')
    timestr = time.strftime("%Y%m%d_%H%M%S")
    csv_buffer = StringIO()

    df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    filename = team_name + '_' + timestr + '.csv'
    #filename = 'bovada_requests.csv'
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())

def get_from_s3(club_name):
    club_name = club_name.strip().lower().replace(' ', '_').replace('.', '_').replace(')', '').replace('&','')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('fifaultimateteam')
    max_date = pd.to_datetime('2001-01-01 00:00:00+00:00')
    return_file = ''
    for file in bucket.objects.all():
        if file.key.startswith(club_name):
            if file.last_modified > max_date:
                max_date = file.last_modified
                return_file = file.key
    df = get_s3_data('fifaultimateteam', return_file)
    return df

def get_s3_data(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def top_clubs_bar(df):
    h=px.bar(df.groupby(['club_abbrname']).size().to_frame('count').sort_values(['count'], ascending=False).reset_index().head(25), x='club_abbrname', y='count', title='Number of Players by Club')
    h.update_xaxes(title='Club')
    h.update_yaxes(title='# of Players')
    st.plotly_chart(h)

def clubs_rating(df):
    fig = px.scatter(df.groupby(['club_abbrname']).agg({'player_name':'size','rating':'mean'}).reset_index(drop=False).sort_values(['player_name'],ascending=False).head(100),
    x='player_name',
    y='rating',
    color='club_abbrname'
    )
    fig.update_xaxes(title='# of Players')
    fig.update_yaxes(title='Avg Rating')
    fig.update_traces(marker=dict(opacity=.8,size=10,line=dict(width=1,color='DarkSlateGrey')))
    st.plotly_chart(fig)

def top_leagues_bar(df):
    i=px.bar(df.groupby(['league_name']).size().to_frame('count').sort_values(['count'], ascending=False).reset_index().head(25), x='league_name', y='count', title='Number of Players by League')
    i.update_xaxes(title='League')
    i.update_yaxes(title='# of Players')
    st.plotly_chart(i)

def top_nations_bar(df):
    j=px.bar(df.groupby(['nation_abbrname']).size().to_frame('count').sort_values(['count'], ascending=False).reset_index().head(25), x='nation_abbrname', y='count', title='Number of Players by Nation')
    j.update_xaxes(title='Nation')
    j.update_yaxes(title='# of Players')
    st.plotly_chart(j)

def ppg_scatter(df):
    df['player_rating'] = df['player_name'] + ' - ' + df['rating'].astype('str')
    f = px.scatter(df.loc[df.games > 5], x='ppg', y='rating', color='preferredposition', title='Points Per Game by Rating', hover_name='player_rating', size='games',hover_data=['club_name','nation_name'])
    f.update_traces(marker=dict(opacity=.8,sizemin=1,line=dict(width=1,color='DarkSlateGrey')))
    st.plotly_chart(f)

def top_players_scatter(df):
    df['player_rating'] = df['player_name'] + ' - ' + df['rating'].astype('str')
    g = px.scatter(df.loc[df.points > 10], x='goals', y='assists', color='preferredposition', title='Goals / Assists for Top Players', hover_name='player_rating', size='games',hover_data=['club_name','nation_name'])
    g.update_traces(marker=dict(opacity=.8,sizemin=1,line=dict(width=1,color='DarkSlateGrey')))
    st.plotly_chart(g)

def games_rating_scatter(df):
    df['player_rating'] = df['player_name'] + ' - ' + df['rating'].astype('str')
    fig = px.scatter(df.loc[df.games > 10],
        x='games',
        y='rating',
        color='preferredposition',
        hover_name='player_rating',
        hover_data=['club_name','nation_name'],
        title='Games Played & Player Rating')
    fig.update_traces(marker=dict(opacity=.8,size=10,line=dict(width=1,color='DarkSlateGrey')))
    st.plotly_chart(fig)

def ga(event_category, event_action, event_label):
    st.write('<img src="https://www.google-analytics.com/collect?v=1&tid=UA-18433914-1&cid=555&aip=1&t=event&ec='+event_category+'&ea='+event_action+'&el='+event_label+'">',unsafe_allow_html=True)


def top_goalscorer(df):
    df['player_rating'] = df['player_name'] + ' - ' + df['rating'].astype('str')
    fig = px.bar(df.sort_values('goals',ascending=False).head(25),
            x='player_rating',
            y='goals',
            title='Top 25 Goalscorers')
    st.plotly_chart(fig)

def heading(df, club_name):
    st.title(club_name)
    st.markdown('Welcome to your FIFA Ultimate Team dashboard!')
    st.markdown('We analyzed your club. You currently have a total of '+ str(df.index.size) + ' players in your club.')
    st.markdown('Your team has an average rating of ' + str(round(df.rating.mean(),2)) + ' and a total of ' + str(df.goals.sum()) + ' goals in about '+ str(round(df.games.sum()/11,2)) + ' games.')
    st.markdown('Your club is made up of a total of '+ str(df.nation_name.nunique()) +' nations and '+ str(df.club_name.nunique()) +' clubs.')
    st.markdown('Feel free to look at some visualizations below created based on your team. If you have any feedback, feel free to reach out to me at aaronlytics@gmail.com')

def main():
    # _max_width_()
    st.title('FIFA Ultimate Team Stats Dashboard')
    ga('FIFA','Page Load', 'Page Load')
    genre = st.radio("Add a X-UT-SID or Search for you Club Name if you have searched your club before", ( 'Club Name','X-UT-SID'))

    if genre == 'X-UT-SID':
        x_ut_sid = st.text_input('X-UT-SID', '')
        club_name = st.text_input('Club Name', '')
        submit = st.button('Submit')
        ga('FIFA','UT-SID', 'Submit')
        if submit:
            try:
                df = fut_api(x_ut_sid)
                player_file='./fifa_players.json'
                df=file_join(df,player_file)
                save_to_s3(df, club_name)
                heading(df, club_name)
                st.write(df[['player_name','preferredposition','rating','pace','sho','pass','dri','def','phy','games','assists','yellows','points','ppg','formation','untradeable','owners','cardsubtypeid','rareflag','skillmoves','weakfootabilitytypecode','attackingworkrate','defensiveworkrate','club_name','nation_name','league_name']])
                top_goalscorer(df)
                ppg_scatter(df)
                top_players_scatter(df)
                games_rating_scatter(df)
                top_clubs_bar(df)
                clubs_rating(df)
                top_leagues_bar(df)
                top_nations_bar(df)
                ga('FIFA','UT-SID', 'Success')

            except:
                'Sorry, that UT SID failed'

    if genre == 'Club Name':
        club_name = st.text_input('Club Name -- type in your club name to search or hit Search to see Example United','Example United')
        submit = st.button('Search')
        if submit:
            ga('FIFA','Club Name', club_name)
            try:
                if len(club_name) > 3:
                    df = get_from_s3(club_name)
                    heading(df, club_name)
                    st.write(df[['player_name','preferredposition','rating','pace','sho','pass','dri','def','phy','games','assists','yellows','points','ppg','formation','untradeable','owners','cardsubtypeid','rareflag','skillmoves','weakfootabilitytypecode','attackingworkrate','defensiveworkrate','club_name','nation_name','league_name']])
                    top_goalscorer(df)
                    ppg_scatter(df)
                    top_players_scatter(df)
                    games_rating_scatter(df)
                    top_clubs_bar(df)
                    clubs_rating(df)
                    top_leagues_bar(df)
                    top_nations_bar(df)
                    ga('FIFA',club_name, 'Success')

                else:
                    'Club Name must be more than 3 characters'
            except:
                'Sorry could not retrieve club data for ' + club_name

if __name__ == "__main__":
    #execute
    main()
