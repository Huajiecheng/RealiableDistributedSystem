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

ACTIVE = True

state = "aaa"
__lock = threading.Lock()

primaryFlag = False  ## flag to decide whether it is primary
Checkpoint_turn = 0
#log state: (replied,0) (unreplied,1) (recovery done,2)
Log = []

'''
Both UDP and TCP will be setup
'''


def init():
    conf = config.read_config("config_server2.json")
    # Make variable in JSON global
    globals().update(conf)
    if len(sys.argv) == 2:
        globals()['checkpoint_frequency'] = int(sys.argv[1]);

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

    # while True:
    #     c = messager.encodeInit(messager.MSG_ROLE_SERVER, sid)
    #     msg = messager.encodeMsg(messager.MSG_INIT, len(c)+1, c)
    #     sudp.sendto(bytes(msg, "utf-8"), ((ip_repmgn, port_repmgn)))
    #
    #     try:
    #         data, addr = sudp.recvfrom(buffSize)
    #     except socket.timeout:
    #         continue
    #
    #     t, s, c = messager.decodeMsg(data.decode('utf-8'))
    #     if (t != messager.MSG_SUCCESS):
    #         continue
    #
    #     logging.info('Login to repmgn')
    #     break

    return sudp, stcp


# Mainly for heartbeating
# Maybe also for launch replica
def handleUDP(data):
    global ACTIVE
    global primaryFlag
    global state
    global Checkpoint_turn
    global Log
    global state
    sock = data
    tdata = threading.local()

    while True:
        try:
            tdata.data, tdata.addr = sock.recvfrom(buffSize)
        except socket.timeout:
            exit(0)

        tdata.t, tdata.s, tdata.c = messager.decodeMsg(tdata.data.decode('utf-8'))

        # intial primary
        if tdata.t == messager.MSG_INIT_PRIMARY:
            primaryFlag = True
            print("I am primary!")
            # checkpointing sent if it is primary
        # wake up to be primary and recover
        if tdata.t == messager.MSG_WAKE:
            # recover
            with __lock:
                print("i am waking up")
                primaryFlag = True
                print("Recovery start. Prune log and process unreplied requests")
                if Log==[]:
                    print("Recovery done. Current state:",state)
        #initial checkpoint
        if tdata.t == messager.MSG_CHECKPOINT and primaryFlag == True:
            with __lock:
                msg = messager.encodeMsg(messager.MSG_CHECKPOINT, len(state) + 1, state)
                sock.sendto(bytes(msg, 'utf-8'), (tdata.c[0],tdata.c[1]))                      
        # recv checkpoint it is a backup
        if tdata.t == messager.MSG_CHECKPOINT and primaryFlag == False:
            with __lock:
                state = tdata.c
                Log = []
                print('After checkpointing, current state is: ', state)
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
            exit(0)


def handleTCP(data, udp):
    global state
    global Checkpoint_turn
    global Log
    sock = data[0]
    tdata = threading.local()
    tdata.cid = 0
    checkpoint_count = 0

    while True:
        tdata.data = sock.recv(buffSize)

        if not tdata.data:
            logging.info("CID: %i exit (0 is UNKNOWN)", tdata.cid)
            sock.close()
            return

        tdata.t, tdata.s, tdata.c = messager.decodeMsg(tdata.data.decode('utf-8'))

        # Update state, may be better to have a dedicate function
        if tdata.t == messager.MSG_UPDATESTATE:

            # Client side do not have a way to handle this at this point
            if ACTIVE == False:
                logging.error("NOT ACTIVE")
                tdata.c = "SERVER NOT ACTIVE"
                tdata.data = messager.encodeMsg(messager.MSG_ERROR, len(tdata.c), tdata.c)
                # sock.send(bytes(tdata.data, 'utf-8'))
                sock.close()
                exit(0)

            # cid, seq, state
            tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state = messager.decodeUpdateState(tdata.c)

            __lock.acquire()  # Racing Condition

            # state = tdata.state
            # logging.info("client: %i ,server: %i, seq: %i, state: %s", tdata.cid, tdata.sid,tdata.seq, state)
            # sock.send(tdata.c.encode('utf-8'))
            # Checkpoint_turn = True
            ##checkpoint_count += 1

            if primaryFlag == True:
                state = tdata.state
                logging.info("client: %i ,server: %i, seq: %i, time: %s, state: %s", tdata.cid, tdata.sid, tdata.seq,
                             tdata.ts, state)
                tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                sock.send(tdata.data.encode('utf-8'))
                Checkpoint_turn += 1
            else:
                logging.info("backup is sleeping")
                tdata.c = messager.encodeUpdateState(tdata.cid, tdata.sid, tdata.seq, tdata.ts, "backup is sleeping")
                tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                sock.send(tdata.data.encode('utf-8'))
                Log.append([tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state,0])
                print("log:")
                for i in Log:
                    print(i[:5])

            __lock.release()

        # recovery request
        elif tdata.t == messager.MSG_RECOVERY:
            #print("recovering")
            # cid, seq, state
            tdata.cid, tdata.sid, tdata.seq, tdata.ts, tdata.state = messager.decodeUpdateState(tdata.c)

            

            if primaryFlag == True:
                while True:
                    #process log
                    flag = False
                    for i in range(len(Log)):
                        if Log[i][0] == tdata.cid and Log[i][2] == tdata.seq:
                            #current first
                            if i==0 or Log[i-1][5]==0 or Log[i-1][5]==2:
                                flag = True
                                break
                            else:
                                break
                    if flag:
                        break
                    time.sleep(0.2)                                
                with __lock:
                    #process log
                    for i in range(len(Log)):
                        if Log[i][0] == tdata.cid and Log[i][2] == tdata.seq: 
                            state = Log[i][4]
                            Log[i][5] = 2
                            logging.info("client: %i ,server: %i, seq: %i, time: %s, state: %s", Log[i][0], Log[i][1], Log[i][2], Log[i][3], state)
                            tdata.c = messager.encodeUpdateState(Log[i][0], Log[i][1], Log[i][2], Log[i][3], state)
                            tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                            sock.send(tdata.data.encode('utf-8'))
                            Checkpoint_turn += 1
                            #prune log
                            if i == (len(Log)-1):
                                Log = []
                                print("Recovery done. Current state:",state)
                            break
                        elif Log[i][5] == 0:
                            state = Log[i][4]
                            Log[i][5] = 2
                
            else:
                with __lock:
                    tdata.c = messager.encodeUpdateState(tdata.cid, tdata.sid, tdata.seq, tdata.ts, "backup is sleeping")
                    tdata.data = messager.encodeMsg(messager.MSG_UPDATESTATE, 0, tdata.c)
                    sock.send(tdata.data.encode('utf-8'))
                    #mark as unreplied
                    for i in range(len(Log)):
                        if Log[i][0] == tdata.cid and Log[i][2] == tdata.seq: 
                            Log[i][5] = 1



            # send checkpoint out if it is the primary and meets the frequency
        if primaryFlag and (Checkpoint_turn >= checkpoint_frequency):
            with __lock:
                checkpoint = messager.encodeMsg(messager.MSG_CHECKPOINT, len(state) + 1, state)
                sendCheck(udp, checkpoint)
                Checkpoint_turn = 0


def sendCheck(sock, checkpoint):
    if sid == 1:
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10089))
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10088))
    elif sid == 2:
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10089))
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10087))
    elif sid == 3:
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10088))
        sock.sendto(checkpoint.encode('utf-8'), ("localhost", 10087))


if __name__ == "__main__":

    sudp, stcp = init()

    while True:
        readset, wlist, xlist = select.select(endpoint, wlist, xlist)

        # time.sleep(1)

        for sock in readset:
            if stcp is sock:
                sock = stcp.accept()
                logging.info("TCP Entry spawning thread")
                t_tcp = threading.Thread(target=handleTCP, args=(sock, sudp), daemon=True)
                t_tcp.start()

            elif sudp is sock:
                t_udp = threading.Thread(target=handleUDP, args=(sudp,), daemon=True)
                t_udp.start()

