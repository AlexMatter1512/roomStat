import serial.tools.list_ports
import json
import os
import csv

REMOVE_LOGFILE = os.environ.get('REMOVE_LOGFILE', False)
PRODUCER_LOGFILE = os.environ.get('LOGFILE', 'logs/training.csv')

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
    csvwriter = csv.writer(f)

    headers = ['light', 'temperature', 'humidity', 'comfortable']
    # write the headers to the file in csv format
    csvwriter.writerow(headers)

    while True:
        try:
            data = ser.readline().decode().strip()
            #decode json
            data = json.loads(data)
            # add the comfortable column
            comfortable = 1
            if data['light'] < 20:
                comfortable = 0
            if data['temperature'] > 30:
                comfortable = 0
            if data['humidity'] > 60:
                comfortable = 0
            data['comfortable'] = comfortable
            # write to the file in csv format
            csvwriter.writerow([data['light'], data['temperature'], data['humidity'], data['comfortable']])
            print(data)
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

