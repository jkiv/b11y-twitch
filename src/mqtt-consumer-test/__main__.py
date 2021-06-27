import paho.mqtt.client as mqtt

# Consuming client
#  - subscribe to "echo" topic on connect
#  - print each "echo" topic payload

def on_connect(client, userdata, flags, rc):
    print(f'Connected with result code {rc}', flush=True)
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe('b11y/dev/*')

def on_message(client, userdata, msg):
    print(f'{msg.topic}: {msg.payload}', flush=True)

if __name__ == '__main__':
    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(username='mqtt_anonymous', password='mqtt_anonymous')
    
    client.connect("localhost", 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()
