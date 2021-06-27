import asyncio
import os
import toml
from twitchio.ext import commands

from . import mqtt

class Bot(commands.Bot):

    def __init__(self, in_queue, out_queue, **config):
        self._in_queue = in_queue
        self._out_queue = out_queue
        self._config = config

        self._mods_only = True

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

        if message.author.is_mod or not self._mods_only:
            await self.handle_commands(message)

    @commands.command(name='ping')
    async def ping(self, ctx):
        await ctx.send(f'pong @{ctx.author.name}!')

    @commands.command(name='togglemod')
    async def toggle_mod(self, ctx):
        self._mods_only = not self._mods_only

    @commands.command(name='mqtt')
    async def mqtt_echo(self, ctx):
        timestamp = ctx.message.timestamp
        user = ctx.message.author.name

        try:
            _, topic, payload = ctx.message.content.split(' ', 2)
        except:
            print(f'Malformed !mqtt command: {ctx.message.content}', flush=True)
            return # Malformed command

        print(f'Enqueueing \'{topic=}, {payload=}\' from {user} @ {timestamp}', flush=True)

        self._out_queue.put_nowait((topic, payload,))

async def _handle_mqtt_to_twitch_forever(twitch_client, queue, channel_name):
    loop = asyncio.get_running_loop()

    while loop.is_running():
        try:
            topic, payload = await asyncio.wait_for(queue.get(), 1)
        except asyncio.TimeoutError:
            continue

        print(f'Pulled MQTT payload off internal queue: {topic=}, {payload=}', flush=True)

        channel = twitch_client.get_channel(channel_name)
        await channel.send(f'<mqtt> {topic=}, {payload=}')

        queue.task_done()

async def main(config):

    twitch_config = config['twitch']
    mqtt_config = config['mqtt']

    loop = asyncio.get_running_loop()

    # Set up internal message queues
    twitch_to_mqtt_queue = asyncio.Queue()
    mqtt_to_twitch_queue = asyncio.Queue()

    # Set up TwitchIO client
    twitch_client = Bot(mqtt_to_twitch_queue, twitch_to_mqtt_queue, loop=loop, **twitch_config)

    # Set up MQTT client
    mqtt_bridge = mqtt.MQTTBridge(mqtt_to_twitch_queue, twitch_to_mqtt_queue, mqtt_config['subscriptions'])

    # TODO maybe make this part of the MessageBridge API?
    mqtt_client = mqtt_bridge.client
    mqtt_client.username_pw_set(username=mqtt_config['username'], password=mqtt_config['password'])
    mqtt_client.connect(mqtt_config['host'], mqtt_config['port'], mqtt_config['timeout'])

    try:
        tasks = await asyncio.gather(
            mqtt_bridge.run(),
            # TODO for each channel in twitch_config['initial_channels']
            _handle_mqtt_to_twitch_forever(twitch_client, mqtt_to_twitch_queue, twitch_config['initial_channels'][0]),
            twitch_client.start()
        )
        
        tasks.cancel()
    except Exception as e:
        print(f'Exception: {e!r}', flush=True)
        loop.stop()

if __name__ == '__main__':

    # Load config.toml from default location
    config_path = '~/.b11y/config.toml'
    config_path = os.path.abspath(os.path.expanduser(config_path))

    with open(config_path, 'r') as f:
        config = toml.load(f)

    # Run
    asyncio.run(main(config))
