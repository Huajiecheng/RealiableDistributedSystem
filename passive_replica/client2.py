#!/usr/bin/python3

import sys
import logging
import socket
import time
import threading
from datetime import datetime

# axillary lib is needed
from common import messager
from common import config

endpoint = []
seq = 1
recovering = False

def init():
    conf = config.read_config("config_client2.json")
    globals().update(conf)
    global server_address
    server_address = [(host, port1), (host, port2), (host, port3)]
    sudp = None
    stcp = [[1, socket.socket(socket.AF_INET, socket.SOCK_STREAM), False, 'b'],
            [2, socket.socket(socket.AF_INET, socket.SOCK_STREAM), False, 'b'],
            [3, socket.socket(socket.AF_INET, socket.SOCK_STREAM), False, 'b']]
    # connect to server
    for i in range(len(stcp)):
        stcp[i][1].connect(server_address[i])
        stcp[i][2] = True
        print("connecting to {} port {}".format(server_address[i][0], server_address[i][1]))

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.info('Socket(%s:%i) connected', host, port1)
    logging.info('Socket(%s:%i) connected', host, port2)
    logging.info('Socket(%s:%i) connected', host, port3)
    logging.info('CID: %i, SEQ: %i', cid, seq)

    return sudp, stcp


def stateUpdate(stcp):
    global seq
    global recovering
    global resend
    # Currently for easy debug -- Later will be feeded by script
    if recovering:
        state = resend
        print("send recover",state)
    else:
        state = messager.inputState()

    # send msg
    for i in stcp:
        # check connection
        if i[2]:
            timestamp = str(datetime.now())
            update_str = messager.encodeUpdateState(cid, i[0], seq, timestamp,state)
            if recovering:
                msg = messager.encodeMsg(messager.MSG_RECOVERY, len(update_str) + 1, update_str)
            else:
                msg = messager.encodeMsg(messager.MSG_UPDATESTATE, len(update_str) + 1, update_str)
            i[1].send(bytes(msg, 'utf-8'))
            if not recovering:
                print("msg send to server id:", i[0], state)
        else:
            # reconnect
            try:
                i[1] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                i[1].connect(server_address[i[0] - 1])
                i[2] = True
                timestamp = str(datetime.now())
                update_str = messager.encodeUpdateState(cid, i[0], seq, timestamp,state)
                if recovering:
                    msg = messager.encodeMsg(messager.MSG_RECOVERY, len(update_str) + 1, update_str)
                else:
                    msg = messager.encodeMsg(messager.MSG_UPDATESTATE, len(update_str) + 1, update_str)
                i[1].send(bytes(msg, 'utf-8'))
                if not recovering:
                    print("msg send to server id:", i[0],state)
            except socket.error:
                continue

    # duplicate detector
    recovering = True
    for i in range(len(stcp)):        
        if stcp[i][2]:
            try:
                data = stcp[i][1].recv(buffSize)
                # error
                if not data:
                    stcp[i][1].close()
                    stcp[i][2] = False                   
                    if stcp[i][3] == 'p':
                        recovering = True
                    stcp[i][3] = 'b'
                    continue

                i_type, i_len, i_c = messager.decodeMsg(data.decode('utf-8'))
                if i_type == messager.MSG_UPDATESTATE:
                    i_cid, i_sid, i_seq, i_timestamp,i_state = messager.decodeUpdateState(i_c)
                    # only implementaiton on reply
                    if i_state == "backup is sleeping":
                        continue
                    if i_seq >= seq:
                        print(messager.decodeUpdateState(i_c))
                        stcp[i][3] = 'p'
                        recovering = False
                        seq += 1
                    else:
                        print("duplicate reply detected:")
                        print(messager.decodeUpdateState(i_c))
            # error
            except socket.error:
                stcp[i][1].close()
                stcp[i][2] = False
                if stcp[i][3] == 'p':
                    recovering = True
                stcp[i][3] = 'b'
                continue
    if recovering:
        resend = state
        time.sleep(1)
    return

       

'''
Usage: ./client.py [cid]{seq}
Later cid and seq will be moved to config_client.json
'''
if __name__ == "__main__":

    sudp, stcp = init()
    while True:
        stateUpdate(stcp)

