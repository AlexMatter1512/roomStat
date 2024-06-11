import csv
import random

def generate_random_data(num_rows):
    data = []
    for _ in range(num_rows//2):
        # all values have a precision of 2 decimal places
        # devices = random.randint(50, 100)  # Assuming devices range from 0 to 100
        light = round(random.uniform(0, 20), 2)
        temperature = round(random.uniform(30, 50), 2)
        humidity = round(random.uniform(60, 100), 2)
        comfortable = 0
        # data.append([devices, light, temperature, humidity, comfortable])
        data.append([light, temperature, humidity, comfortable])


    for _ in range(num_rows//2):
        # devices = random.randint(0, 50)
        light = round(random.uniform(20, 4096), 2)
        temperature = round(random.uniform(0, 30), 2)
        humidity = round(random.uniform(0, 60), 2)
        comfortable = 1
        # data.append([devices, light, temperature, humidity, comfortable])
        data.append([light, temperature, humidity, comfortable])

    # adding some other 100 rows with random values
    for _ in range(500):
        # devices = random.randint(0, 100)
        light = round(random.uniform(0, 4096), 2)
        temperature = round(random.uniform(0, 50), 2)
        humidity = round(random.uniform(0, 100), 2)

        comfortable = 1
        # if devices > 50:
        #     comfortable = 0
        if light > 20:
            comfortable = 0
        if temperature > 30:
            comfortable = 0
        if humidity > 60:
            comfortable = 0
        # data.append([devices, light, temperature, humidity, comfortable])
        data.append([light, temperature, humidity, comfortable])
    
    return data

def write_csv(filename, data):
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # csvwriter.writerow(['devices', 'light', 'temperature', 'humidity', 'comfortable'])
        csvwriter.writerow(['light', 'temperature', 'humidity', 'comfortable'])
        csvwriter.writerows(data)

if __name__ == "__main__":
    num_rows = 1000  # Adjust the number of rows as needed
    data = generate_random_data(num_rows)
    write_csv('random_data.csv', data)
