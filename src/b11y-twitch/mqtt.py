import asyncio
import paho.mqtt.client as mqtt

class MQTTBridge:
    '''\
    Handles sending and receiving MQTT messages from MQTT server.

    Uses two internal asyncio.Queue's to store incoming and outgoing MQTT
    messages.
    '''

    def __init__(self, initial_subscriptions, mqtt_client=None):

        if mqtt_client is None:
            self.client = mqtt.Client()
        else:
            self.client = mqtt_client

        self.in_queue = asyncio.Queue()
        self.out_queue = asyncio.Queue()
        self.subscriptions = initial_subscriptions # TODO refactor for appending prefix

        def _on_connect_closure(client, userdata, flags, rc):
            print(f'Connected to MQTT ({rc=})...', flush=True)
            for topic in self.subscriptions:
                print(f'Subscribing to MQTT topic \'{topic}\'', flush=True)
                client.subscribe(topic)

        def _on_message_closure(client, userdata, message):
            print(f'MQTT message received: {message.topic}: {message.payload}', flush=True)
            self.in_queue.put_nowait((message.topic, message.payload,))

        self.client.on_connect = _on_connect_closure
        self.client.on_message = _on_message_closure

    def subscribe(self, topic):
        print(f'Subscribing to MQTT topic \'{topic}\'', flush=True)
        self.subscriptions.append(topic)
        self.client.subscribe(topic)

    def unsubscribe(self, topic):
        print(f'Unubscribing from MQTT topic \'{topic}\'', flush=True)

        if topic in self.subscriptions:
            self.subscriptions.remove(topic)

        self.client.unsubscribe(topic)

    async def put(self, topic, payload):
        print(f'Enqueueing: {topic=}, {payload=}')
        await self.out_queue.put((topic, payload,))

    async def get(self):
        topic, payload = await self.in_queue.get()
        print(f'Dequeueing: {topic=}, {payload=}', flush=True)
        return (topic, payload,)

    async def publish_messages_forever(self):
        
        loop = asyncio.get_running_loop()

        while loop.is_running():
            topic, payload = await self.out_queue.get()

            print(f'Publishing MQTT message: {topic=}, {payload=}', flush=True)

            self.client.publish(topic, payload)
            self.out_queue.task_done()

    async def loop_forever(self):

        def _blocking_loop(client):
            return client.loop()

        loop = asyncio.get_running_loop()

        while loop.is_running():
            await loop.run_in_executor(None, _blocking_loop, self.client) 

    def connect(self, host, port, timeout, username=None, password=None, blocking=True):

        if username is not None or password is not None:
            self.client.username_pw_set(username=username, password=password)

        if not blocking: 
            # FIXME doesn't work?
            self.client.connect_async(host, port, timeout)
        else:
            self.client.connect(host, port, timeout)

def concat_topic(*parts):
    return '/'.join(part.strip('/') for part in parts)