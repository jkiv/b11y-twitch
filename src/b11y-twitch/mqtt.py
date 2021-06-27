import asyncio
import paho.mqtt.client as mqtt

def _on_connect(client, userdata, flags, rc, subscriptions):
    print(f'Connected to MQTT ({rc=})...', flush=True)
    for topic in subscriptions:
        print(f'Subscribing to MQTT topic \'{topic}\'')
        client.subscribe(topic)

def _on_message(client, userdata, msg, queue):
    print(f'MQTT message received: {msg.topic}: {msg.payload}', flush=True)
    queue.put_nowait((msg.topic, msg.payload,))

class MQTTBridge:
    '''\
    Handles sending and receiving MQTT messages from MQTT server.

    Uses two internal asyncio.Queue's to store incoming and outgoing MQTT
    messages.
    '''

    def __init__(self, in_queue, out_queue, subscriptions, client=None):

        if client is None:
            self.client = mqtt.Client()
        else:
            self.client = client

        self.in_queue = in_queue
        self.out_queue = out_queue
        self.subscriptions = subscriptions

        # Wrap extended callbacks to capture extra parameters
        def _on_connect_closure(client, userdata, flags, rc):
            return _on_connect(client, userdata, flags, rc, self.subscriptions)

        def _on_message_closure(client, userdata, message):
            return _on_message(client, userdata, message, self.in_queue)

        self.client.on_connect = _on_connect_closure
        self.client.on_message = _on_message_closure

    async def _send_messages_forever(self):
        
        loop = asyncio.get_running_loop()

        while loop.is_running():
            try:
                topic, message = await self.out_queue.get()
            except asyncio.TimeoutError:
                continue

            print(f'Publishing MQTT message: {topic=}, {message=}', flush=True)

            self.client.publish(topic, message)
            self.out_queue.task_done()

    async def _loop_forever(self):

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

    async def run(self):
        await asyncio.gather(
            self._send_messages_forever(),
            self._loop_forever()
        )