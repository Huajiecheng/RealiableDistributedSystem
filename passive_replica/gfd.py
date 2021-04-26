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

_primary = []
_backup = []
_lfd = []


init_flag=True

def online(instance):
    if instance in _primary:
        return True
    elif instance in _backup:
        return True
    elif instance in _lfd:
        return True

    return False

def init():
    conf = config.read_config("config_gfd.json")
    globals().update(conf)

    sudp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sudp.bind((mask, port))

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.info('Socket(%s:%i) binded', mask, port)
    
    return sudp

#Currently make no threading on it 
def handleUDP(sock):

    while True:
        data, addr = sock.recvfrom(buffSize)
        sock.sendto(data,(ip_repmgn,port_repmgn))

        t, s, c = messager.decodeMsg(data.decode('utf-8'))

        if (t == messager.MSG_INIT):
            role, ID = messager.decodeInit(c)
            factive = False
            
            idaddr = {"id": ID, "addr": addr}

            if online(idaddr):
                logging.error("Already login")
                continue

            if (role == messager.MSG_ROLE_SERVER):
                if _primary:
                    if ID == 1:
                        msg = messager.encodeMsg(messager.MSG_CHECKPOINT, 1, ("localhost",10087))
                    elif ID == 2:
                        msg = messager.encodeMsg(messager.MSG_CHECKPOINT, 1, ("localhost",10088))
                    elif ID == 3:
                        msg = messager.encodeMsg(messager.MSG_CHECKPOINT, 1, ("localhost",10089)) 
                    if int(_primary[0]['id']) == 1:
                        sock.sendto(bytes(msg,'utf-8'),('localhost',10087))
                    elif int(_primary[0]['id']) == 2:
                        sock.sendto(bytes(msg, 'utf-8'), ('localhost', 10088))
                    elif int(_primary[0]['id']) == 3:
                        sock.sendto(bytes(msg, 'utf-8'), ('localhost', 10089))                                       
                    _backup.append(idaddr)
                else:
                    _primary.append(idaddr)
                factive = True

            elif (role == messager.MSG_ROLE_LFD):
                _lfd.append(idaddr)

            msg = messager.encodeMsg(messager.MSG_SUCCESS, 1, "")
            sock.sendto(bytes(msg, 'utf-8'), addr)
            logging.info('id: %d login', ID)
            print("membership:")
            print("Current primary:")
            for p in _primary:
                print(p)
            print("Current backup:")
            for b in _backup:
                print(b)

            if factive:
                msg = messager.encodeMsg(messager.MSG_ACTIVE, 1, "")
                sock.sendto(bytes(msg, 'utf-8'), addr)
                logging.info('id: %d activing', ID)
                if _primary:
                    firstPrimaryMessage = messager.encodeMsg(messager.MSG_INIT_PRIMARY,1,"")
                    if int(_primary[0]['id']) == 1:
                        sock.sendto(bytes(firstPrimaryMessage,'utf-8'),('localhost',10087))
                    if int(_primary[0]['id']) == 2:
                        sock.sendto(bytes(firstPrimaryMessage, 'utf-8'), ('localhost', 10088))
                    if int(_primary[0]['id']) == 3:
                        sock.sendto(bytes(firstPrimaryMessage, 'utf-8'), ('localhost', 10089))
        #server down with lfd message 
        if (t == messager.MSG_FAULT):
            role, ID = messager.decodeInit(c)

            if (role == messager.MSG_ROLE_LFD):
                for s in _primary:
                    if s["id"] == ID:
                        _primary.remove(s) # primary done
                        if _backup:  # if there is still backup alive
                            _primary.append(_backup[0]) #wake up one of the backup and make it primary
                            _backup.pop(0)
                            msg = messager.encodeMsg(messager.MSG_WAKE, 1, '')
                            if int(_primary[0]['id']) == 1:
                                sock.sendto(msg.encode("utf-8"), ("localhost", 10087))
                            elif int(_primary[0]['id']) == 2:
                                sock.sendto(msg.encode("utf-8"), ("localhost", 10088))
                            elif int(_primary[0]['id']) == 3:
                                sock.sendto(msg.encode("utf-8"), ("localhost", 10089))
                            break
                for b in _backup:
                    if b["id"] == ID:
                        _backup.remove(b)
                        break

            ###
            ## c = messager.encodeInit(messager.MSG_ROLE_REPMGN, ID)

            ###
            print("server down. ID:",ID)
            print("membership:")
            print("Current primary:")
            for p in _primary:
                print(p)
            print("Current backup:")
            for b in _backup:
                print(b)

def lfd_ping(lfd_id,lfd_addr):
    sudp_ping = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sudp_ping.settimeout(1)
    while True:
        msg = messager.encodeMsg(20, 0, "")
        sudp_ping.sendto(msg.encode('utf-8'),lfd_addr)
        try:
            sudp_ping.recvfrom(buffSize)

            logging.info("lfd %d login",lfd_id)
            break
        except socket.timeout:
            continue
    while True:
        while True:
            time.sleep(interval)
            sudp_ping.sendto(msg.encode('utf-8'),lfd_addr)
            tried = 0
            while tried < retry:
                try:
                    sudp_ping.recvfrom(buffSize)
                    logging.info("lfd %d beating freq: %d retry: %d", lfd_id,interval, retry)
                    break
                except socket.timeout:
                    logging.info("No response, try %d times", tried)
                    tried += 1
                # Should never goes here
                except socket.error:
                    logging.error("Critical error")
            if tried == 3:
                logging.error("NO Heartbeating")
                print("lfd %d down",lfd_id)
                break;




if __name__ == "__main__":

    sudp = init()
    lfd1_addr = ('localhost', 10000)
    lfd2_addr = ('localhost', 10001)
    lfd3_addr = ('localhost', 10002)
    tudp1 = threading.Thread(target=lfd_ping, args=(1, lfd1_addr), daemon=True)
    tudp1.start()
    tudp2 = threading.Thread(target=lfd_ping, args=(2, lfd2_addr), daemon=True)
    tudp2.start()
    tudp3 = threading.Thread(target=lfd_ping, args=(3, lfd3_addr), daemon=True)
    tudp3.start()
    handleUDP(sudp)
