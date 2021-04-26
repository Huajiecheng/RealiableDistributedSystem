#!/usr/bin/python3
import logging
import sys
import socket
import select
import threading
import time

# axillary lib is needed
from common import messager
from common import config

endpoint = []
wlist = []
xlist = []

num_blocked = 0
num_client = 0

ACTIVE = False
quiet_lock = False
BLOCK_FLAG = False

state = "aaa"
log = []
__lock = threading.Lock()

'''
Both UDP and TCP will be setup
'''


def init():
    conf = config.read_config("config_server2.json")
    # Make variable in JSON global
    globals().update(conf)

    sudp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sudp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sudp.bind((mask, port))
    sudp.settimeout(timeout)

    stcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    stcp.bind((mask, port))
    stcp.listen()

    endpoint.append(stcp)
    endpoint.append(sudp)

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.info('Socket(%s:%i) binded', host, port)

    return sudp, stcp


def login(sudp):
    return


# Mainly for heartbeating
# Maybe also for launch replica
def handleUDP(data):
    global ACTIVE
    global BLOCK_FLAG
    global log
    global state
    global quiet_lock
    global num_client
    global num_blocked

    sock = data
    tdata = threading.local()

    while True:
        try:
            tdata.data, tdata.addr = sock.recvfrom(buffSize)
        except socket.timeout:
            exit(0)

        tdata.t, tdata.s, tdata.c = messager.decodeMsg(tdata.data.decode('utf-8'))
        if tdata.t == messager.MSG_CHECKPOINT:
            with __lock:
                if tdata.s == 1:                    
                    tdata.data = messager.encodeMsg(messager.MSG_CHECKPOINT, 2, state)
                    sock.sendto(bytes(tdata.data, 'utf-8'), (tdata.c[0],tdata.c[1]))                    
                elif tdata.s == 2:
                    print("after checkpoint state is : ", tdata.c)
                else:                  
                    tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state = messager.decodeUpdateState(tdata.c)
                    print("checkpoint state is : ", tdata.state)
                    state = tdata.state
                    for i in range(len(log)):
                        if log[i][0] == tdata.cid and log[i][2] == tdata.seq:
                            break
                    log = log[i:]
                    print("obtain useful log:")
                    for i in log:
                        print(i)
                    for i in log:
                        state = i[4]
                        logging.info("client: %i ,server: %i, seq: %i, time: %s,state: %s", i[0], i[1], i[2],i[3], i[4])
                    log = []
                    quiet_lock = False
        if tdata.t == messager.MSG_LFD_SERVER_INIT:
            print('recv lfd init')
            sudp.sendto(tdata.data, tdata.addr)
        # Heartbeating
        if tdata.t == messager.MSG_PING:
            if sid == messager.decodeHeartbeat(tdata.c):
                tdata.data = messager.encodeMsg(messager.MSG_BEATING, tdata.s, tdata.c)
                sock.sendto(bytes(tdata.data, 'utf-8'), tdata.addr)
            # Should Never happen, but just in case
            else:
                tdata.data = messager.encodeMsg(messager.MSG_ERROR, tdata.s, tdata.c)
                sock.sendto(bytes(tdata.data, 'utf-8'), tdata.addr)

        elif tdata.t == messager.MSG_ACTIVE:
            with __lock:
                ACTIVE = True
            logging.info("Become active")

        elif tdata.t == messager.MSG_QUIET:
            with __lock:
                quiet_lock = True

            logging.info("Become quiet")

        elif tdata.t == messager.MSG_CHECK_REMIND:
            with __lock:
                tosid = int(tdata.c)

                print("sid:", tosid)
                if tosid == 1:
                    sendToCheck = 10087
                if tosid == 2:
                    sendToCheck = 10088
                if tosid == 3:
                    sendToCheck = 10089
                BLOCK_FLAG = True
                check_state = state
                print("checkpointing")
                print("send checkpoint. state:",check_state)
                
            #logging.info("%d %d", num_blocked, num_client)
            while num_blocked < num_client:
                continue
            #logging.info("%d %d", num_blocked, num_client)
            
            with __lock:
                tdata.c = messager.encodeUpdateState(log[0][0], log[0][1], log[0][2], log[0][3], check_state)
                msg = messager.encodeMsg(messager.MSG_CHECKPOINT, 0, tdata.c)
                sock.sendto(msg.encode('utf-8'), ("localhost", sendToCheck))
                BLOCK_FLAG = False
                log = []
                num_client = 0


def handleTCP(data):
    global state
    global log
    global num_client
    global num_blocked
    sock = data[0]
    tdata = threading.local()
    tdata.cid = 0

    recovered = False

    with __lock:
        num_client += 1

    while True:
        tdata.data = sock.recv(buffSize)

        if not tdata.data:
            # logging.info("CID: %i exit (0 is UNKNOWN)", tdata.cid)
            with __lock:
                num_client -= 1
            sock.close()
            return

        tdata.t, tdata.s, tdata.c = messager.decodeMsg(tdata.data.decode('utf-8'))
        # Update state, may be better to have a dedicate function
        if tdata.t == messager.MSG_UPDATESTATE:
            
            # cid, seq, state
            tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state = messager.decodeUpdateState(tdata.c)
            item = [tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state] 
            
            with __lock:
                if ACTIVE == False:
                    logging.error("NOT ACTIVE")
                    log.append([tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state])
                    print("log:")
                    for i in log:
                        print(i)
                    tdata.c = "SERVER NOT ACTIVE"
                    tdata.data = messager.encodeMsg(messager.MSG_ERROR, len(tdata.c), tdata.c)
                    sock.send(bytes(tdata.data, 'utf-8'))
    
                    #with __lock:
                        #num_client -= 1
                    #sock.close()
                    continue

            with __lock:
                # Send
                if quiet_lock:
                    logging.info("NOT ACTIVE")
                    log.append([tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state])
                    print("log:")
                    for i in log:
                        print(i)                    
                    tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                    sock.send(bytes(tdata.data, 'utf-8'))
                    continue

            

            __lock.acquire()  # Racing Condition
            state = tdata.state
            logging.info("client: %i ,server: %i, seq: %i, time: %s,state: %s", tdata.cid, tdata.sid, tdata.seq,tdata.ts, state)              
            

            if BLOCK_FLAG:
                num_blocked += 1
                log.append([tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state])
                recovered = True
            __lock.release()
            
            while BLOCK_FLAG:  # Busy wait for very short period
                continue

            with __lock:              
                tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                sock.send(tdata.data.encode('utf-8'))
                if recovered:
                    time.sleep(0.2)
                    recovered = False


if __name__ == "__main__":

    sudp, stcp = init()

    while True:
        readset, wlist, xlist = select.select(endpoint, wlist, xlist)

        # time.sleep(1)

        for sock in readset:
            if stcp is sock:
                sock = stcp.accept()
                logging.info("TCP Entry spawning thread")
                t_tcp = threading.Thread(target=handleTCP, args=(sock,), daemon=True)
                t_tcp.start()

            elif sudp is sock:
                t_udp = threading.Thread(target=handleUDP, args=(sudp,), daemon=True)
                t_udp.start()

