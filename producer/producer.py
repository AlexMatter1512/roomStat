import serial.tools.list_ports

def find_serial_port():
    ports = serial.tools.list_ports.comports()
    for port in ports:
        if 'USB' in port.description:  # Adjust this condition based on your device
            return port.device
    return None

def read_from_serial():
    port = find_serial_port()
    if port is None:
        print("No serial port found.")
        return

    ser = serial.Serial(port, baudrate=9600)  # Adjust the baudrate as per your device
    while True:
        try:
            data = ser.readline().decode().strip()
            print(data)
        except KeyboardInterrupt:
            break

    ser.close()

read_from_serial()