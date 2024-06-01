import os
import random

def generate_mac_address():
    mac = [random.randint(0x00, 0xff) for _ in range(6)]
    return ':'.join(map(lambda x: f'{x:02x}', mac))

def generate_fake_log_file(file_path, num_entries):
    if os.path.exists(file_path):
        with open(file_path, 'r') as existing_file:
            existing_content = existing_file.read()
    
    with open(file_path, 'a') as file:
        for _ in range(num_entries):
            mac_address = generate_mac_address()
            file.write(f'{mac_address}\n')
        
        if os.path.exists(file_path):
            file.write(existing_content)

# Usage example
file_path = 'mac_file.txt'
num_entries = 100
generate_fake_log_file(file_path, num_entries)