#!/usr/bin/python3
import logging
import sys
import socket
import time
import threading

# axillary lib is needed
from common import messager
from common import config

ACTIVE = False


def init():
    conf = config.read_config("config_server3.json")
    globals().update(conf)

    if len(sys.argv) == 3:
        globals()['interval'] = int(sys.argv[1])
        globals()['timeout'] = int(sys.argv[2])

    sudp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.info('Socket created')
    sudp.settimeout(timeout)

    return sudp


def ping(sock):
    global ACTIVE
    addr = ((host, port))

    while True:
        time.sleep(interval)

        ping_str = messager.encodeHeartbeat(sid)
        msg = messager.encodeMsg(messager.MSG_PING, len(ping_str) + 1, ping_str)
        sock.sendto(bytes(msg, 'utf-8'), addr)

        tried = 0
        while tried < retry:
            try:
                data, addr = sock.recvfrom(buffSize)
                logging.info("beating freq: %d retry: %d", interval, retry)
                if (not ACTIVE):
                    c = messager.encodeInit(messager.MSG_ROLE_SERVER, sid)
                    msg = messager.encodeMsg(messager.MSG_INIT, len(c) + 1, c)
                    sudp.sendto(bytes(msg, "utf-8"), ((ip_gfd, port_gfd)))
                    logging.info("server %d active", sid)
                    ACTIVE = True
                break
            except socket.timeout:
                logging.info("No response, try %d times", tried)
                tried += 1

            # Should never goes here
            except socket.error:
                logging.error("Critical error")

        if tried == 3:
            logging.error("NO Heartbeating")
            if ACTIVE:
                c = messager.encodeInit(messager.MSG_ROLE_LFD, sid)
                msg = messager.encodeMsg(messager.MSG_FAULT, len(c) + 1, c)
                sudp.sendto(bytes(msg, "utf-8"), ((ip_gfd, port_gfd)))
                ACTIVE = False
            break;

def handle_gfd_ping():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('localhost',port_lfd))
    tdata = threading.local()
    while True:
        tdata.data, tdata.addr = sock.recvfrom(buffSize)
        sock.sendto(tdata.data,tdata.addr)


if __name__ == "__main__":
    sudp = init()

    logging.info("timeout: %d, interval: %d", timeout, interval)

    tudp2 = threading.Thread(target=handle_gfd_ping, args=(), daemon=True)
    tudp2.start()

    ## keep sending message to server until get responses, then break
    while True:
        msg = messager.encodeMsg(messager.MSG_LFD_SERVER_INIT, 0, "")
        sudp.sendto(msg.encode('utf-8'), ((host, port)))

        try:
            data, addr = sudp.recvfrom(buffSize)
            break
        except socket.timeout:
            continue

    while True:
        ping(sudp)


