import serial.tools.list_ports
import json
import time
import os

REMOVE_LOGFILE = True
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

def find_serial_port():
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if 'BLE' in port.description:  # Adjust this condition based on your device
            return port.device
    return None

def read_from_serial():
    port = find_serial_port()
    if port is None:
        print("No serial port found.")
        return

    ser = serial.Serial(port, baudrate=9600)  # Adjust the baudrate as per your device
    #append to macs.log
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, PRODUCER_LOGFILE)
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
            print("Exiting...")
            f.close()
            # remove the log file
            if REMOVE_LOGFILE: os.remove(path)
            break
    ser.close()

if __name__ == '__main__':
    read_from_serial()
