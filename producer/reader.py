import serial.tools.list_ports
import json
import time
import argparse
import os
import random

REMOVE_LOGFILE = os.environ.get('REMOVE_LOGFILE', False)
PRODUCER_LOGFILE = os.environ.get('LOGFILE', 'logs/data.log')

def find_serial_port(pattern):
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if pattern in port.description:  # Adjust this condition based on your device
            return port.device
    print("Port not found.")
    exit(1)

def read_from_serial(port, logfile, remove_logfile=False):
    if not logfile:
        print("No logfile specified.")
        return
    
    if port is None:
        print("No serial port specified.")
        return

    ser = serial.Serial(port, baudrate=9600)  # Adjust the baudrate as per your device
    #append to macs.log
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, logfile)
    # with open(path, 'a') as f: create a new file if not exist
    try:
        f = open(path, 'a')
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path))
    f = open(path, 'a')
    while True:
        try:
            data = ser.readline().decode().strip()
            print(data)
            f.write(str(data)+'\n')
            f.flush()
        except KeyboardInterrupt:
            print("\nExiting...")
            f.close()
            # remove the log file
            if remove_logfile: os.remove(path)
            break
    ser.close()

if __name__ == '__main__':
    port = find_serial_port('BLE')
    read_from_serial(port, PRODUCER_LOGFILE, REMOVE_LOGFILE)

