import asyncio
import paho.mqtt.client as mqtt

def _on_connect(client, userdata, flags, rc, subscriptions):
    print(f'Connected with result code {rc}', flush=True)
    for topic in subscriptions:
        client.subscribe(topic)

def _on_message(client, userdata, msg, queue):
    print(f'MQTT message received: {msg.topic}: {msg.payload}', flush=True)
    queue.put_nowait((msg.topic, msg.payload,))

class MQTTBridge:

    def __init__(self, in_queue, out_queue, subscriptions, client=None):

        if client is None:
            self.client = mqtt.Client()
        else:
            self.client = client

        self.in_queue = in_queue
        self.out_queue = out_queue
        self.subscriptions = subscriptions

        def _on_connect_closure(client, userdata, flags, rc):
            return _on_connect(client, userdata, flags, rc, self.subscriptions)

        def _on_message_closure(client, userdata, message):
            return _on_message(client, userdata, message, self.in_queue)

        self.client.on_connect = _on_connect_closure
        self.client.on_message = _on_message_closure

    async def _handle_messages_forever(self):
        
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

    async def run(self):
        await asyncio.gather(
            self._handle_messages_forever(),
            self._loop_forever()
        )