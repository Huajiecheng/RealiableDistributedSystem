#!/usr/bin/python
import sys
import json         #Maybe pickle perform better
import time
import hashlib
import random

'''
Level 1 message layout: Type Size Content
'''

MSG_PING        = 1
MSG_BEATING     = 2
MSG_UPDATESTATE = 3
MSG_WAKE        = 4
MSG_ERROR       = 5
MSG_FAULT       = 6
MSG_INIT        = 7         #used by server and lfd to login to repmgn
MSG_SUCCESS     = 8
MSG_ACTIVE      = 9
MSG_CHECKPOINT  = 10
MSG_INIT_PRIMARY= 11
MSG_RECOVERY    = 12
MSG_CHECK_REMIND= 13
MSG_QUIET       = 14
MSG_LFD_SERVER_INIT= 15


MSG_ROLE_SERVER     = 1
MSG_ROLE_CLIENT     = 2
MSG_ROLE_LFD        = 3
MSG_ROLE_REPMGN     = 4

def encodeMsg(Type, size, cont):
    msg = {
        "type": Type,
        "size": size,       #Currently as a redundant value
        "content": cont
    }
    return json.dumps(msg)

def decodeMsg(msg):
    obj = json.loads(msg)
    return obj["type"], obj["size"], obj["content"]

def encodeUpdateState(cid, sid, seq,timestamp, state):
    msg = {
        "cid" : cid,
        "sid" : sid,
        "seq" : seq,
        "timestamp": timestamp,
        "state": state
    }
    return json.dumps(msg)

def decodeUpdateState(msg):
    obj = json.loads(msg)
    return obj["cid"], obj['sid'], obj["seq"], obj["timestamp"],obj["state"]

def encodeHeartbeat(sid):
    msg = {
        "sid" : sid
    }
    return json.dumps(msg)

def decodeHeartbeat(msg):
    return json.loads(msg)["sid"]

def encodeWake(sid):
    return encodeHeartbeat(sid)

def decodeWake(msg):
    return decodeHeartbeat(msg)

def encodeInit(role, id):
    msg = {
        "role" : role,
        "id" : id
    }
    return json.dumps(msg)

def decodeInit(msg):
    obj = json.loads(msg)
    return obj["role"], obj["id"]

def inputState():
    
    time.sleep(1)  # Wait for some time
    
    salt = int(random.random() * 100)
    message = hashlib.sha256()
    message.update(bytes(salt))
    return message.hexdigest()[:10]
