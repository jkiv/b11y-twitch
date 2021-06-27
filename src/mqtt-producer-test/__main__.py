import paho.mqtt.client as mqtt

# Publishing client
#  - read a line of input
#  - publish line of input on "echo" topic

def on_connect(client, userdata, flags, rc):
    print(f'Connected with result code {rc}', flush=True)

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
    while True:
        client.loop()

        topic, payload = input('(topic, payload) >').split(' ', 1)

        if payload.lower() in ['quit', 'exit', 'q']:
            break

        print(f'topic: {topic}, payload: {payload}', flush=True)
        client.publish(topic, payload)