import os
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
        # TODO context here?
        pass

    async def event_message(self, message):
        print(message.content, flush=True)
        await self.handle_commands(message)

    @commands.command(name='ping')
    async def ping(self, ctx):
        await ctx.send(f'pong @{ctx.author.name}!')

    @commands.command(name='password')
    async def password(self, ctx):
        await ctx.send(f'hunter2')

if __name__ == '__main__':

    config_path = '~/.b11y/config.toml'
    config_path = os.path.abspath(os.path.expanduser(config_path))

    with open(config_path, 'r') as f:
        config_all = toml.load(f)

    config = config_all[next(iter(config_all))]

    bot = Bot(None, None, **config)
    bot.run()