import asyncio
import os
import paho.mqtt.client as mqtt
import toml
from twitchio.ext import commands

class Bot(commands.Bot):

    def __init__(self, read_queue, write_queue, **config):
        self._read_queue = read_queue
        self._write_queue = write_queue
        self._config = config

        super().__init__(
            irc_token=config['irc_token'],
            client_id=config['client_id'],
            nick=config['nick'],
            prefix=config['prefix'],
            initial_channels=config['initial_channels']
        )

        # Clear secrets in config for good measure
        self._config['irc_token'] = '***' if 'irc_token' in self._config and self._config['irc_token'] else '(empty)'
        self._config['client_id'] = '***' if 'client_id' in self._config and self._config['client_id'] else '(empty)'

    async def event_ready(self):
        print(f'[{self.nick}] Ready for service...', flush=True)

    async def event_message(self, message):
        print(f'[{message.timestamp}] {message.author.name}: {message.content}', flush=True)

        if message.author.is_mod:
            await self.handle_commands(message)

    @commands.command(name='ping')
    async def ping(self, ctx):
        await ctx.send(f'pong @{ctx.author.name}!')

    @commands.command(name='mqtt')
    async def mqtt_echo(self, ctx):
        timestamp = ctx.message.timestamp
        user = ctx.message.author.name

        try:
            _, topic, payload = ctx.message.content.split(' ', 2)
        except:
            return # Malformed command

        print(f'mqtt_echo: Enqueueing \'[{timestamp}] {user}: {topic=}, {payload=}\'', flush=True)
        await self._write_queue.put((topic, payload))

async def _handle_twitch_to_mqtt_forever(mqtt_client, read_queue):
    loop = asyncio.get_running_loop()

    while loop.is_running():
        # Publish messages from read_queue
        topic, message = await read_queue.get()
        print(f'twitch->mqtt: {topic=}, {message=}', flush=True)
        mqtt_client.publish(topic, message)
        read_queue.task_done()

def on_mqtt_connect_cb(client, userdata, flags, rc):
    print(f'Connected with result code {rc}', flush=True)
    client.subscribe('b11y/dev/*')

def on_mqtt_message_cb(client, userdata, msg, write_queue):
    print(f'{msg.topic}: {msg.payload}', flush=True)
    write_queue.put_nowait((msg.topic, msg.payload,))

async def _handle_mqtt_to_twitch_forever(twitch_client, read_queue):
    loop = asyncio.get_running_loop()

    while loop.is_running():
        topic, message = await read_queue.get()

        # TODO how to send a message via twitch_client?
        print(f'Received MQTT message: {topic=}, {message=}', flush=True)
        print('  - TODO handle properly')

        read_queue.task_done()
    
async def _mqtt_loop(mqtt_client):
    loop = asyncio.get_running_loop()
    # run_in_executor?
    rc = 0
    while rc == 0:
        rc = mqtt_client.loop()

async def main(config):

    twitch_to_mqtt_queue = asyncio.Queue()
    mqtt_to_twitch_queue = asyncio.Queue()

    loop = asyncio.get_running_loop()
    twitch_client = Bot(mqtt_to_twitch_queue, twitch_to_mqtt_queue, loop=loop, **config)

    # Set up MQTT
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_mqtt_connect_cb
    mqtt_client.on_message = lambda client, userdata, message: on_mqtt_message_cb(client, userdata, message, mqtt_to_twitch_queue)

    mqtt_client.username_pw_set(username='mqtt_anonymous', password='mqtt_anonymous')

    mqtt_client.connect_async("localhost", 1883, 60)
    mqtt_client.loop_start()

    try:
        await asyncio.gather(
            _handle_twitch_to_mqtt_forever(mqtt_client, twitch_to_mqtt_queue),
            _handle_mqtt_to_twitch_forever(twitch_client, mqtt_to_twitch_queue),
            twitch_client.start()
        )
    except Exception as e:
        #_log.error(f'Fatal error: {e!r}')
        #_log.error(f'Quitting...')
        loop.stop()
    
    mqtt_client.loop_stop()

if __name__ == '__main__':

    # Load config.toml from default location
    config_path = '~/.b11y/config.toml'
    config_path = os.path.abspath(os.path.expanduser(config_path))

    with open(config_path, 'r') as f:
        config_all = toml.load(f)

    # Get first section in config.toml as configuration
    config = config_all[next(iter(config_all))]

    # Make rocket go now
    asyncio.run(main(config))