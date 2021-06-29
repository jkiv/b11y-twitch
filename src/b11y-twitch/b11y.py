
import asyncio
from twitchio.ext import commands

from . import mqtt

class B11yTwitchBot(commands.Bot):
    def __init__(self, **config):
        self.config = config
        self.channels = config['channels']

        super().__init__(
            irc_token=config['irc_token'],
            client_id=config['client_id'],
            nick=config['nick'],
            prefix=config['prefix'],
            initial_channels=config['channels']
        )

        # Clear secrets in config for good measure
        self.config['irc_token'] = '***' if 'irc_token' in self.config and self.config['irc_token'] else '(empty)'
        self.config['client_id'] = '***' if 'client_id' in self.config and self.config['client_id'] else '(empty)'
    
    async def event_ready(self):
        pass # Rely on B11yBase to wire up

    async def event_message(self, ctx):
        pass # Rely on B11yBase to wire up

class B11yBase:
    
    def __init__(self, twitch_client, mqtt_bridge):

        self._twitch = twitch_client
        self._mqtt = mqtt_bridge

        self._topic_handlers = {}
        # twitch_client manages twitch handlers

        if twitch_client is not None:
            self._twitch.listen('event_ready')(self._on_twitch_ready)
            self._twitch.listen('event_message')(self._on_twitch_message)

    async def _on_twitch_ready(self):
        print(f'Connected to Twitch...', flush=True)

    async def _on_twitch_message(self, message):
        mod_status = '[M] ' if message.author.is_mod else ''
        print(f'[{message.timestamp}] {mod_status}{message.author.name}: {message.content}', flush=True)

        if message.author.is_mod:
            await self._twitch.handle_commands(message)

    async def _dispatch_topic_message(self, topic, payload):
        if topic in self._topic_handlers:
            print(f'Dispatching handler for {topic=}.', flush=True)
            await self._topic_handlers[topic](topic, payload)
        else:
            known_topics = ', '.join(self._topic_handlers.keys())
            print(f'Unhandled topic {topic}. Known topics: {known_topics}', flush=True)

    async def _dispatch_topic_messages_forever(self):
        loop = asyncio.get_running_loop()

        while loop.is_running():
            topic, payload = await self._mqtt.get()
    
            print(f'Received MQTT message: {topic=}, {payload=}', flush=True)

            await self._dispatch_topic_message(topic, payload)
            
            self._mqtt.in_queue.task_done() # TODO uh? wat?

    def add_topic_handler(self, topic, fn, topic_prefix=None):
        if topic_prefix is not None:
            topic = mqtt.concat_topic(topic_prefix, topic)
        
        self._topic_handlers[topic] = fn

        self._mqtt.subscribe(topic)

    def remove_topic_handler(self, topic, topic_prefix=None):
        if topic_prefix is not None:
            topic = mqtt.concat_topic(topic_prefix, topic)
        
        if topic in self._mqtt_handlers:
            del self._topic_handlers[topic]
        
        self._mqtt.unsubscribe(topic)

    def add_twitch_command_handler(self, command,  fn, requires_mod=False, requires_streamer=False, allowlist=None):
        # TODO add requires_mod, etc. -> generic checks
        self._twitch.command(name=command)(fn)

    def remove_twitch_command_handler(self, command):
        self._twitch.remove_command(command)

    async def run(self):
        await asyncio.gather(
            self._twitch.start(),
            self._mqtt.loop_forever(),
            self._mqtt.publish_messages_forever(),
            self._dispatch_topic_messages_forever()
        )
