from plexapi.server import PlexServer
from plexapi.server import PlexObject
from plexapi.alert import AlertListener
from plexapi.media import TranscodeSession
import json
import time
from datetime import datetime
from elasticsearch import Elasticsearch
import urllib.request
import config




es = Elasticsearch ([config.esurl], http_auth=('external', ''), scheme="https", port=443)


kickMessage = "You are currently transcoding which hurts the server. Please fix your settings and make sure you are using \"Maximum Quality\" or \"Original Quality\" on Remote Servers. This is a per client setting, so each new client you use needs to be configured."
location = { }
lastMessage = { }

def cleanUp():
        if now > lastRun + 3600:
            lastRun = now

def createIndex(index):
    settings={
        'mappings' : {
            'properties' : {
                '@timestamp' : { 
                    'type' : 'date', 
                    'format' : 'epoch_second'
                },
                'user.name' : { 'type' : 'keyword' },
                'user.city' : { 'type' : 'keyword' },
                'user.ip_addr' : { 'type' : 'ip' },
                'media.episode' : { 'type' : 'integer' },
                'media.season' : { 'type' : 'integer' },
                'media.title' : { 'type' : 'keyword' },
                'status' : { 'type' : 'keyword' },
                'location' : { 'type' : 'geo_point' },
            },
        },
    }
    try:
        es.indices.create(index=index, body=settings)
    except Exception as e:
        print (e)

def esWrite(userInfo,mediaInfo):
    doc = { 
        '@timestamp': time.time(),
        'user' : {
            'name' : userInfo['name'],
            'ip_addr' : userInfo['ip'],
        },
        'event' : userInfo['event'],
        'media' : { },
        'state' : mediaInfo['state'],
        'location' : { },
    }
    try:
        doc['media']['title'] = mediaInfo['title']
        if mediaInfo['mediaType'] == "episode":
            doc['media']['episode'] = mediaInfo['episode']
            doc['media']['season'] = mediaInfo['season']
    except:
        pass
    if userInfo['event'] == "playing" or userInfo['event'] == "transcode" or userInfo['event'] == "kick":
        if not userInfo['ip'] in location:
            with urllib.request.urlopen('http://ip-api.com/json/' + userInfo['ip']) as html:
                try:
                    response = (json.loads(html.read().decode('utf-8')))
                    doc['location']['lat'] = response['lat']
                    doc['location']['lon'] = response['lon']
                except Exception as e:
                    print(e)
                try:
                    location[userInfo['ip']] = {
                        'lat' : response['lat'],
                        'lon' : response['lon']
                    }
                except Exception as e:
                    print (e)
                location[userInfo['ip']] = { 
                    'lat' : response['lat'], 
                    'lon' : response['lon']
                }
        else:
            doc['location']['lat'] = location[userInfo['ip']]['lat']
            doc['location']['lon'] = location[userInfo['ip']]['lon']
    try:
        es.index(index=config.esindex, body=doc)
        print(doc)
    except Exception as e:
        print(e)
    



def cb(data):
    if data["type"] == "playing":
        for session in server.sessions():
            if str(session.sessionKey) == data["PlaySessionStateNotification"][0]["sessionKey"]:
                userInfo = {
                    'session' : session.sessionKey
                }
                mediaInfo = {
                    'mediaType' : session.METADATA_TYPE,
                    'state' : data["PlaySessionStateNotification"][0]["state"],
                }
              
                if session.METADATA_TYPE == "movie":
                    mediaInfo['title'] = session.title
                elif session.METADATA_TYPE == "episode":
                    mediaInfo['title'] = session.grandparentTitle
                    mediaInfo['season'] = (session.seasonEpisode).split("e")[0].replace('s','')
                    mediaInfo['episode'] = (session.seasonEpisode).split("e")[1]

                for player in session.players:
                    userInfo = { 
                        'name' : session.usernames[0],
                        'ip' : player.remotePublicAddress,
                        'sessionid' : session.sessionKey
                    }
                    userInfo['event'] = "playing"
                try:
                    for transcode in session.transcodeSessions:
                        
                        if transcode.container == "transcode":

                            userInfo['event'] = "transcode"
                            if not userInfo['name'] in config.exempt:
                                userInfo['event'] = "kick"
                                #session.stop(kickMessage)
                except:
                    pass
                try:
                    lastMessage[userInfo['sessionid']]
                except:
                    lastMessage[userInfo['sessionid']] = { 
                        "begin" : time.time(),
                        "state" : mediaInfo['state'],
                        "ip" : userInfo['ip'],
                        "event" : userInfo['event'],
                        "username" : userInfo['name']
                    }
                    esWrite(userInfo=userInfo,mediaInfo=mediaInfo)
                if not lastMessage[userInfo['sessionid']]["state"] == mediaInfo['state']:
                    lastMessage[userInfo['sessionid']] = { 
                        "begin" : time.time(),
                        "state" : mediaInfo['state'],
                        "ip" : userInfo['ip'],
                        "event" : userInfo['event'],
                        "username" : userInfo['name']
                    }
                    esWrite(userInfo=userInfo,mediaInfo=mediaInfo)




server = PlexServer(config.baseurl, config.token)
notifier = server.startAlertListener(callback=cb)

createIndex(config.esindex)
startTime = time.time()
lastRun = time.time()

while True:
    try:
        now = time.time()
        if now > lastRun + 3600:
            cleanUp()
        time.sleep(1)

    except KeyboardInterrupt:
        break

notifier.stop()