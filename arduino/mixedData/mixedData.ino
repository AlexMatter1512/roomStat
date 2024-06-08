#include <ArduinoBLE.h>
#include <Arduino_APDS9960.h>
#include <Arduino_HS300x.h>

typedef struct mixedData
{
  arduino::String mac;
  int rssi;
  int light;
  float temperature;
  float humidity;
} mixedData_t;
mixedData_t data;
int r, g, b, c = 0;

void printJsonData(){
  Serial.print("{\"mac\": \"");
  Serial.print(data.mac);
  Serial.print("\", ");
  Serial.print("\"rssi\": ");
  Serial.print(data.rssi);
  Serial.print(", ");
  Serial.print("\"light\": ");
  Serial.print(data.light);
  Serial.print(", ");
  Serial.print("\"temperature\": ");
  Serial.print(data.temperature);
  Serial.print(", ");
  Serial.print("\"humidity\": ");
  Serial.print(data.humidity);
  Serial.println("}"); 
}

void readColors(){
  if (APDS.colorAvailable()) {    
    APDS.readColor(r, g, b, c);
    data.light = c;
  }
}

void readHS300x(){
  data.temperature = HS300x.readTemperature();
  data.humidity = HS300x.readHumidity();
}

void bleDiscoveredCallback(BLEDevice peripheral) {
  data.mac = peripheral.address();
  data.rssi = peripheral.rssi();
}

void setup() {
  Serial.begin(9600);
  while (!Serial);

  // begin initialization
  if (!BLE.begin()) {
    Serial.println("starting Bluetooth® Low Energy module failed!");
    while (1);
  }
  if (!APDS.begin()) {
    Serial.println("Error initializing APDS-9960 sensor.");
    while (1);
  }
  if (!HS300x.begin()) {
    Serial.println("Failed to initialize humidity temperature sensor!");
    while (1);
  }

  Serial.println("Bluetooth® Low Energy Central scan");
  BLE.setEventHandler(BLEDiscovered, bleDiscoveredCallback);
  // start scanning for peripheral
  BLE.scan(true);
}

void loop() {
  readColors();
  BLE.poll();
  readHS300x();
  printJsonData();
}
