import serial.tools.list_ports
import json
import time
import argparse
import os
import random

REMOVE_LOGFILE = os.environ.get('REMOVE_LOGFILE', False)
PRODUCER_LOGFILE = os.environ.get('LOGFILE', 'logs/macs.log')

def csv_to_json(csv_str):
    try:
        values = csv_str.split(',')
        return {
            'mac': values[0],
            'rssi': int(values[1]),
            #'timestamp': int(time.time())
        }
    except Exception as e:
        pass

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
            data = json.dumps(csv_to_json(ser.readline().decode().strip()))
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

def read_from_file(filename, logfile, remove_logfile=False):
    if not logfile:
        print("No logfile specified.")
        return

    try:
        input_f = open(filename, 'r')
    except FileNotFoundError:
        print(f"File {filename} not found.")
        return

    #append to macs.log
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, logfile)
    # with open(path, 'a') as f: create a new file if not exist
    try:
        log_f = open(path, 'a')
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path))
    log_f = open(path, 'a')
    while True:
        for line in input_f:
            try:
                print(line, end='')
                log_f.write(str(line))
                log_f.flush()
                # sleep for a random time between 0 and 0,5 seconds
                time.sleep(random.random()/2)
            except KeyboardInterrupt:
                print("\nExiting...")
                log_f.close()
                input_f.close()
                # remove the log file
                if remove_logfile: os.remove(path)
                exit(0)
    
if __name__ == '__main__':
    # read command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-r','--remove', help='Remove the logfile after exiting', action='store_true', default=REMOVE_LOGFILE)
    parser.add_argument('-d','--dummy', help='Generate dummy data. If a filename is provided, the data will be read from the file', nargs='?', const=True, default=False)
    parser.add_argument('-l','--logfile', help=f'Path to the logfile (default: {PRODUCER_LOGFILE})', default=PRODUCER_LOGFILE)
    parser.add_argument('-p','--port', help='Serial port to read from')
    parser.add_argument('-P','--pattern', help='Pattern to search for in the serial port description', default='BLE')

    args = parser.parse_args()
    if args.dummy == True:
        #TODO: generate dummy data
        print("Random Dummy data (not implemented yet)")
    elif args.dummy:
        # read and print every line from the given dummyfile to the log file
        try:
            read_from_file(args.dummy, args.logfile, args.remove)
        except FileNotFoundError:
            print(f"File {args.dummy} not found.")
            exit(1)
    else:
        port = find_serial_port(args.pattern) if not args.port else args.port
        read_from_serial(port, args.logfile, args.remove)

