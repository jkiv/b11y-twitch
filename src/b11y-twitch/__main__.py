import asyncio
import os
import toml

from . import mqtt
from . import b11y

class Bot(b11y.B11yBase):

    def __init__(self, twitch_client, mqtt_bridge, topic_prefix=None):
        super().__init__(twitch_client, mqtt_bridge)

        self.topic_prefix = topic_prefix

        self.add_twitch_command_handler('ping', self.twitch_ping)
        self.add_twitch_command_handler('mqttping', self.twitch_mqttping)

        self.add_topic_handler('pong', self.mqtt_pong, topic_prefix=topic_prefix)

    async def twitch_ping(self, ctx):
        '''\
        Simple ping/pong command.
        '''
        await ctx.send(f'@{ctx.author.name} pong!')

    async def twitch_mqttping(self, ctx):
        '''\
        Ping/pong using MQTT server.
        '''
        channel = ctx.channel.name
        user = ctx.message.author.name

        print(f'MQTT ping command from {user} in {channel}', flush=True)

        topic = 'pong'

        if self.topic_prefix is not None:
            topic = mqtt.concat_topic(self.topic_prefix, topic)

        await self._mqtt.put(topic, f'{channel} {user}')
    
    async def mqtt_pong(self, topic, payload):
        '''\
        Handle receiving "pong" message from MQTT server.
        '''

        payload = payload.decode('utf-8')
        channel_name, author = payload.split(' ', 1)

        print(f'Handling {topic}, {author} in {channel_name}', flush=True)

        channel = self._twitch.get_channel(channel_name)
        await channel.send(f'@{author} pong!')

async def main(config):

    twitch_config = config['twitch'] # TODO update base config
    mqtt_config   = config['mqtt']   # TODO update base config

    loop = asyncio.get_running_loop()

    # Set up TwitchIO client
    twitch_client = b11y.B11yTwitchBot(loop=loop, **twitch_config)

    # Set up MQTT bridge
    mqtt_bridge = mqtt.MQTTBridge(mqtt_config['subscriptions'])

    mqtt_bridge.connect(
        mqtt_config['host'],
        mqtt_config['port'],
        mqtt_config['timeout'],
        mqtt_config['username'],
        mqtt_config['password']
    )

    # Set up bot
    bot = Bot(twitch_client, mqtt_bridge, topic_prefix=mqtt_config['topic_prefix'])

    try:
        await bot.run()
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