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

_active = []
_replica = []
_lfd = []

def online(instance):
    if instance in _active:
        return True
    elif instance in _replica:
        return True
    elif instance in _lfd:
        return True

    return False

def init():
    conf = config.read_config("config_repmgn.json")
    globals().update(conf)

    sudp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sudp.bind((mask, port))

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.info('Socket(%s:%i) binded', mask, port)
    return sudp

def handleUdp(sock):
    while True:

        data, addr = sock.recvfrom(buffSize)
        print("msg recv")
        t, s, c = messager.decodeMsg(data.decode('utf-8'))

        if (t == messager.MSG_INIT):
            role, ID = messager.decodeInit(c)
            factive = False

            idaddr = {"id": ID, "addr": addr}

            if online(idaddr):
                logging.error("Already login")
                continue

            if (role == messager.MSG_ROLE_SERVER):
                if (len(_active) > 3):
                    _replica.append(idaddr)
                else:
                    _active.append(idaddr)
                    factive = True

            elif (role == messager.MSG_ROLE_LFD):
                _lfd.append(idaddr)

            msg = messager.encodeMsg(messager.MSG_SUCCESS, 1, "")


            logging.info('id: %d login', ID)
            print("membership:")
            for s in _active:
                print("server id:", s["id"])

            if factive:
                msg = messager.encodeMsg(messager.MSG_ACTIVE, 1, "")
                logging.info('id: %d activing', ID)
                print()
        ## recovery
        if (t == messager.MSG_RECOVERY):

            role, ID = messager.decodeInit(c)
            factive = False

            idaddr = {"id": ID, "addr": addr}


            if online(idaddr):
                logging.error("Already login")
                continue

            if (role == messager.MSG_ROLE_SERVER):
                if (len(_active) > 3):
                    _replica.append(idaddr)
                else:
                    _active.append(idaddr)
                    factive = True

            elif (role == messager.MSG_ROLE_LFD):
                _lfd.append(idaddr)

            msg = messager.encodeMsg(messager.MSG_SUCCESS, 1, "")


            logging.info('id: %d login', ID)
            print("membership:")
            for s in _active:
                print("server id:", s["id"])

            if factive:
                msg = messager.encodeMsg(messager.MSG_ACTIVE, 1, "")
                logging.info('id: %d activing', ID)
                print()
            if _active[0]['id'] == 1:
                checkSendFromPort = 10087
            if _active[0]['id'] == 2:
                checkSendFromPort = 10088
            if _active[0]['id'] == 3:
                checkSendFromPort = 10089
            print("when recovery rempn active list:", _active)
            msg = messager.encodeMsg(messager.MSG_CHECK_REMIND, 1, _active[-1]['id'])


        # server down with lfd message
        if (t == messager.MSG_FAULT):
            role, ID = messager.decodeInit(c)

            if (role == messager.MSG_ROLE_LFD):
                for s in _active:
                    if s["id"] == ID:
                        _active.remove(s)
                        break;
            print("server down. ID:", ID)
            print("membership:")
            for s in _active:
                print("server id:", s["id"])
            print()


if __name__ == "__main__":

    sudp = init()
    handleUdp(sudp)
