import json
import random
import time
import paho.mqtt.client as mqtt

# --- CONFIGURATION ---
MQTT_BROKER = "broker.hivemq.com"      # Change to your broker IP (e.g., "192.168.1.50" or "://hivemq.com")
MQTT_PORT = 1883
MQTT_TOPIC = "PRESENCE/BLR/MANSARVOVAR/A4562/STATUS"

random_delays = [ 2.2, 2.8, 4.4,5.0 ,6.4, 7.5, 4.3, 3.0, 3.5,3.3, 3.8 ,3.8, 2.8, 2.0,
3.4, 3.9, 6.1, 4.2, 2.6, 2.8, 3.1, 6.6, 3.0, 5.0,  2.9, 3.8, 4.0, 3.2, 4.5, 2.5, 4.2,
5.9, 4.2, 3.9, 4.3, 4.3, 3.5, 5.3, 4.5, 5.8, 4.2, 6.0, 6.1, 3.8, 3.5, 5.8, 2.7, 3.5, 4.0,
3.8, 4.4, 5.7, 4.8, 1.5, 3.9, 3.0, 2.5, 2.9, 3.6, 4.0, 3.4, 3.6, 5.2, 4.6, 5.6, 3.8, 4.3,
4.2,
2.7,
4.1,
4.3,
7,
3.7,
2.2,
3.3,
4.4,
5.4,
3.1,
3.2,
3.8,
2.7,
3.4,
2.9,
3.9,
1.9,
2.2,
2.7,
6.2,
7.5,
2.6,
3.6,
2.8,
3.7,
4.9,
4.5,
3.5,
3.8,
3.7,
4.3,
3.3,
4.6,
5.2,
4.1,
4,
3.1,
4.4,
3.9,
6.5,
5.9,
3.1,
2.7,
5.4,
4.3,
4.6,
1.9,
2.7,
3.4,
4.3,
4.1,
3.6,
3.9,
4.2,
5.8,
3.7,
4.2,
4.4,
4.4,
6.1,
2.2,
5,
3.6,
3.9,
2.7,
3.6,
4.7,
6.1,
5.4,
4.5,
6.1,
4.5,
4.1,
3.2,
5.2,
2.6,
4.9,
4.9,
6.2,
4.9,
2.1,
3.9,
2.6,
4.8,
3.2,
2.8,
4,
2.5,
3.2,
4.4,
4.4,
4.1,
4.5,
5.9,
1.5,
3.3,
5.8,
4.4,
5,
3.3,
5,
5.3,
4.3,
4.3,
3,
3.8,
3.8,
4.5,
5.2,
2.7,
3.1,
3,
4.2,
5,
4,
5.6,
5.0,
5.0,
4.7,
3.8,
1.6,
5.9,
3.5,
4.1,
2.5,
2.1,
3.0,
4.5,
4.6,
2.3,
4.1,
3.4,
4.6,
4.8,
4.9,
4.3,
4.6,
3.2,
3.8,
4.2,
5.0,
3.9,
5.0,
1.6,
2.4,
2.2,
3.2,
4.9,
5.3,
5.6,
3.9,
5.6,
4.1,
3.0,
2.9,
3.6,
5.0,
6.1,
2.3,
2.8,
4.6,
3.8,
1.8,
5.1,
2.7,
3.2,
4.5,
3.8,
4.4,
4.4,
5.6,
2.2,
5.3,
3.6,
5.3,
4.5,
3.8,
4.4,
4.1,
4.2,
5.3,
3,
4.4,
4,
4.4,
1.5,
2.7,
3.4,
3.6,
6.3,
4.4,
3.5,
3.7,
4.2,
4.7,
3.6,
4.0,
7.1,
4.3,
5.7,
4.1,
4.7,
3.8,
3.1,
4.5,
3.0,
4.8,
5.3,
4.5,
3.6,
5.1,
1.8,
3.6,
1.6,
2.2,
5.1,
3.1,
2.2,
3.3,
6.9,
4.5,
2.1,
3.6,
4.1,
3.9,
4.9,
4.4,
4.8,
6.0,
2.8,
2.9,
5.6,
3.8,
5.7,
4.2,
4.3,
5.9,
3.6,
2.9,
3.5,
4.8,
3.4,
5.4,
3.1,
2.0,
3.0,
4.3,
3.8,
4.2,
2.8,
4.8,
4.4,
6.4,
3.9,
3.6,
3.1,
2.8,
3.9,
4.4,
4.1,
3.1,
5.1,
3.2,
4.7,
4.2,
2.3,
3.9,
2.3,
5.6,
5.5,
3.7,
4.4,
2.6,
2.7,
4.9,
6.4,
2.6,
3,
4.3,
5.9,
2.5,
6.0,
2.0,
2.0,
2.5,
3.5,
3.4,
4.3,
1.9,
4.6,
3.3,
5.5,
3.2,
3.2,
4.5,
2.0,
4.6,
2.8,
4.3,
2.3

]


# Define the two alternating JSON payloads
payload_toilet_detected = {
    'sensor_room': 'Toilet-1', 
    'status': 'DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}

payload_toilet_not_detected = {
    'sensor_room': 'Toilet-1', 
    'status': 'NOT_DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}


# Define the two alternating JSON payloads
payload_LivingRoom_detected = {
    'sensor_room': 'LivingRoom', 
    'status': 'DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}

payload_LivingRoom_not_detected = {
    'sensor_room': 'LivingRoom', 
    'status': 'NOT_DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}

# Define the two alternating JSON payloads
payload_Kitchen_detected = {
    'sensor_room': 'Kitchen', 
    'status': 'DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}

payload_Kitchen_not_detected = {
    'sensor_roomKitchen': 'Kitchen', 
    'status': 'NOT_DETECTED', 
    'Moving Target': True, 
    'Moving Target Dist': 201, 
    'Stationary Target': False, 
    'Stationary Target Dist': -1, 
    'Wifi': -63
}



# --- MQTT CALLBACKS ---
def on_connect(client, userdata, flags, rc):
    """Callback triggered when connecting to the MQTT Broker."""
    if rc == 0:
        print("✅ Successfully connected to MQTT Broker!")
    else:
        print(f"❌ Connection failed with return code {rc}")

def SimulateToilet():
            # 1. Transmit DETECTED payload
            print(f"📤 Transmitting payload: STATUS = DETECTED Toilet")
            client.publish(MQTT_TOPIC, json.dumps(payload_toilet_detected), qos=1)
            
            # Wait for a random time between 10 and 20 seconds
            idx = random.randint(0,len(random_delays)-2)
            
            wait_time_1 = random_delays[idx] * 2
            print(f"⏳ Waiting for {wait_time_1:.2f} seconds...")
            time.sleep(wait_time_1)

            # 2. Transmit NOT_DETECTED payload
            print(f"📤 Transmitting payload: STATUS = NOT_DETECTED")
            client.publish(MQTT_TOPIC, json.dumps(payload_toilet_not_detected), qos=1)
            
            # Wait for a random time between 10 and 15 seconds
            wait_time_2 = random.uniform(15, 60)
            wait_time_2 = wait_time_2 + wait_time_1  # Introducing random delay 
            
            print(f"⏳ Waiting for {wait_time_2:.2f} seconds...\n")
            time.sleep(wait_time_2)
    
            return
            
    
def SimulateKitchen():
            # 1. Transmit DETECTED payload
            print(f"📤 Transmitting payload: STATUS = DETECTED Kitchen")
            client.publish(MQTT_TOPIC, json.dumps(payload_Kitchen_detected), qos=1)
            
            # Wait for a random time between 10 and 20 seconds
            idx = random.randint(0,len(random_delays)-2)
            
            wait_time_1 = random_delays[idx] * 2
            print(f"⏳ Waiting for {wait_time_1:.2f} seconds...")
            time.sleep(wait_time_1)

            # 2. Transmit NOT_DETECTED payload
            print(f"📤 Transmitting payload: STATUS = NOT_DETECTED")
            client.publish(MQTT_TOPIC, json.dumps(payload_Kitchen_not_detected), qos=1)
            
            # Wait for a random time between 10 and 15 seconds
            wait_time_2 = random.uniform(15, 60)
            wait_time_2 = wait_time_2 + wait_time_1  # Introducing random delay 
            
            print(f"⏳ Waiting for {wait_time_2:.2f} seconds...\n")
            time.sleep(wait_time_2)
    
            return    
    

    
def SimulateLivingRoom():
            # 1. Transmit DETECTED payload
            print(f"📤 Transmitting payload: STATUS = DETECTED: LivingRoom")
            client.publish(MQTT_TOPIC, json.dumps(payload_LivingRoom_detected), qos=1)
            
            # Wait for a random time between 10 and 20 seconds
            idx = random.randint(0,len(random_delays)-2)
            
            wait_time_1 = random_delays[idx] * 2
            print(f"⏳ Waiting for {wait_time_1:.2f} seconds...")
            time.sleep(wait_time_1)

            # 2. Transmit NOT_DETECTED payload
            print(f"📤 Transmitting payload: STATUS = NOT_DETECTED")
            client.publish(MQTT_TOPIC, json.dumps(payload_LivingRoom_not_detected), qos=1)
            
            # Wait for a random time between 10 and 15 seconds
            wait_time_2 = random.uniform(15, 60)
            wait_time_2 = wait_time_2 + wait_time_1  # Introducing random delay 
            
            print(f"⏳ Waiting for {wait_time_2:.2f} seconds...\n")
            time.sleep(wait_time_2)
    
            return    
   

    
    
    
def main():
    # Initialize the client (Using MQTTv311 protocol)
    
    client.on_connect = on_connect

    print(f"⚙️ Connecting to broker {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(f"❌ Could not connect to broker: {e}")
        return

    # Start the background loop to handle network traffic
    client.loop_start()

    print("🚀 MQTT Simulator started. Press Ctrl+C to stop.\n")
    idx = 0
    try:
        while True:
                  SimulateToilet()
                  SimulateKitchen()
                  SimulateLivingRoom()
                  

    except KeyboardInterrupt:
        print("\n🛑 Stopping simulator gracefully...")
    finally:
        # Clean cleanup
        client.loop_stop()
        client.disconnect()
        print("🏁 Disconnected from MQTT broker.")

if __name__ == "__main__":
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    main()
 
 https://i2otlabs-my.sharepoint.com/:v:/p/girish_kumar/IQBI2cYyP_15T42_6B1Bx8vvAQTFd6VgRrPew69BBgA9_Hc?nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJPbmVEcml2ZUZvckJ1c2luZXNzIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXciLCJyZWZlcnJhbFZpZXciOiJNeUZpbGVzTGlua0NvcHkifX0&e=PinDXw