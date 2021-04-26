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

init_flag = True


def online(instance):
    if instance in _primary:
        return True
    elif instance in _backup:
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


# Currently make no threading on it
def handleUDP(sock):
    while True:
        data, addr = sock.recvfrom(buffSize)

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
                    _backup.append(idaddr)
                else:
                    _primary.append(idaddr)
                factive = True

            elif (role == messager.MSG_ROLE_LFD):
                _lfd.append(idaddr)

            msg = messager.encodeMsg(messager.MSG_SUCCESS, 1, "")

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

                logging.info('id: %d activing', ID)

        # server down with lfd message
        if (t == messager.MSG_FAULT):
            role, ID = messager.decodeInit(c)

            if (role == messager.MSG_ROLE_LFD):
                for s in _primary:
                    if s["id"] == ID:
                        _primary.remove(s)  # primary done
                        if _backup:  # if there is still backup alive
                            _primary.append(_backup[0])  # wake up one of the backup and make it primary
                            _backup.pop(0)
                            msg = messager.encodeMsg(messager.MSG_WAKE, 1, '')

                            break
                for b in _backup:
                    if b["id"] == ID:
                        _backup.remove(b)
                        break

            ###
            ## c = messager.encodeInit(messager.MSG_ROLE_REPMGN, ID)

            ###
            print("server down. ID:", ID)
            print("membership:")
            print("Current primary:")
            for p in _primary:
                print(p)
            print("Current backup:")
            for b in _backup:
                print(b)


if __name__ == "__main__":
    sudp = init()
    handleUDP(sudp)
