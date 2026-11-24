'''An extention containing functionality exclusive to the bot owner'''

import logging
from discord import app_commands, Interaction
from discord.ext import commands
from discodrome import DiscodromeClient
from util import env

logger = logging.getLogger(__name__)

class OwnerCog(commands.Cog):
    ''' A Cog containing owner specific commands '''
    
    bot: DiscodromeClient
    
    def __init__(self, bot: DiscodromeClient):
        self.bot = bot
    
    async def interaction_check(self, interaction: Interaction):
        return interaction.user.id == env.DISCORD_OWNER_ID

    async def cog_app_command_error(self, interaction: Interaction, error: app_commands.AppCommandError):
        ''' Answers the interaction when the owner check fails (otherwise Discord shows "did not respond") '''
        if isinstance(error, app_commands.CheckFailure):
            logger.warning("Owner command refused for user %s (DISCORD_OWNER_ID is %s)", interaction.user.id, env.DISCORD_OWNER_ID)
            await interaction.response.send_message("Only the bot owner can use this command.", ephemeral=True)
        else:
            logger.error("Owner command failed: %s", error)
            if interaction.response.is_done():
                await interaction.followup.send(f"Command failed: {error}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Command failed: {error}", ephemeral=True)

    @app_commands.command(name="sync", description="Sync slash commands")
    async def sync(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        await self.bot.sync_command_tree()
        await interaction.followup.send("Commands synced", ephemeral=True)

async def setup(bot: DiscodromeClient):
    ''' Setup function for the owner.py cog '''

    await bot.add_cog(OwnerCog(bot))
