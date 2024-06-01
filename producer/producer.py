import serial.tools.list_ports
import json

def csv_to_json(csv_str):
    values = csv_str.split(',')
    return {
        'mac': values[0],
        'rssi': int(values[1]),
        # 'timestamp': int(values[2])
    }

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
    with open('macs.log', 'a') as f:
        while True:
            try:
                data = json.dumps(csv_to_json(ser.readline().decode().strip()))
                print(data)
                f.write(str(data)+'\n')
                f.flush()
            except KeyboardInterrupt:
                break
    ser.close()

read_from_serial()