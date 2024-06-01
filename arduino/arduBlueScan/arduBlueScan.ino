#include <ArduinoBLE.h>

void setup() {
  Serial.begin(9600);
  while (!Serial);

  // begin initialization
  if (!BLE.begin()) {
    Serial.println("starting Bluetooth® Low Energy module failed!");

    while (1);
  }

  Serial.println("Bluetooth® Low Energy Central scan");
  BLE.setEventHandler(BLEDiscovered, bleDiscoveredCallback);
  // start scanning for peripheral
  BLE.scan(true);
}

void loop() {
  BLE.poll();
}

void bleDiscoveredCallback(BLEDevice peripheral) {
  Serial.print(peripheral.address());
  Serial.print(",");
  Serial.println(peripheral.rssi());
}
