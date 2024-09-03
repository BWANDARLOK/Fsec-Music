 
import asyncio
import os
from datetime import datetime, timedelta
from typing import Union

from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup
from pytgcalls.exceptions import NoActiveGroupCall, AlreadyJoinedError, InvalidMTProtoClient
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream, AudioQuality, VideoQuality, Update
from pytgcalls.types.stream import StreamAudioEnded

import config
from Fsecmusic import LOGGER, YouTube, app, YTB
from Fsecmusic.misc import db
from Fsecmusic.utils.database import (
    add_active_chat,
    add_active_video_chat,
    get_lang,
    get_loop,
    group_assistant,
    is_autoend,
    music_on,
    remove_active_chat,
    remove_active_video_chat,
    set_loop,
)
from Fsecmusic.utils.exceptions import AssistantErr
from Fsecmusic.utils.formatters import check_duration, seconds_to_min, speed_converter
from Fsecmusic.utils.inline.play import stream_markup
from Fsecmusic.utils.stream.autoclear import auto_clean
from Fsecmusic.utils.thumbnails import get_thumb
from strings import get_string

autoend = {}
counter = {}
loop = asyncio.get_event_loop_policy().get_event_loop()


async def _clear_(chat_id):
    db[chat_id] = []
    await remove_active_video_chat(chat_id)
    await remove_active_chat(chat_id)


class CallManager:
    def __init__(self):
        self.userbots = []
        for i in range(1, 6):
            userbot = Client(
                name=f"FSEC{i}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=str(getattr(config, f"STRING{i}")),
            )
            call_instance = PyTgCalls(userbot, cache_duration=100)
            self.userbots.append(call_instance)

    async def pause_stream(self, chat_id: int):
        assistant = await group_assistant(self, chat_id)
        await assistant.pause_stream(chat_id)

    async def resume_stream(self, chat_id: int):
        assistant = await group_assistant(self, chat_id)
        await assistant.resume_stream(chat_id)

    async def stop_stream(self, chat_id: int):
        assistant = await group_assistant(self, chat_id)
        try:
            await _clear_(chat_id)
            await assistant.leave_group_call(chat_id)
        except Exception as e:
            LOGGER(__name__).error(f"Error stopping stream: {e}")

    async def stop_stream_force(self, chat_id: int):
        for call_instance in self.userbots:
            try:
                await call_instance.leave_group_call(chat_id)
            except Exception as e:
                LOGGER(__name__).error(f"Error in force stopping stream: {e}")
        await _clear_(chat_id)

    async def speedup_stream(self, chat_id: int, file_path, speed, playing):
        assistant = await group_assistant(self, chat_id)
        output_path = await self._get_speedup_file(file_path, speed)
        dur = await loop.run_in_executor(None, check_duration, output_path)
        dur = int(dur)
        played, con_seconds = speed_converter(playing[0]["played"], speed)
        duration = seconds_to_min(dur)
        stream = MediaStream(
            output_path,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.SD_480p,
            ffmpeg_parameters=f"-ss {played} -to {duration}",
        ) if playing[0]["streamtype"] == "video" else MediaStream(
            output_path,
            audio_parameters=AudioQuality.HIGH,
            ffmpeg_parameters=f"-ss {played} -to {duration}",
            video_flags=MediaStream.IGNORE,
        )

        if db[chat_id][0]["file"] == file_path:
            await assistant.change_stream(chat_id, stream)
            self._update_db(chat_id, con_seconds, duration, dur, output_path, speed)
        else:
            raise AssistantErr("File path mismatch while speeding up the stream")

    async def _get_speedup_file(self, file_path, speed):
        base = os.path.basename(file_path)
        chatdir = os.path.join(os.getcwd(), "playback", str(speed))
        if not os.path.isdir(chatdir):
            os.makedirs(chatdir)
        output_path = os.path.join(chatdir, base)
        if not os.path.isfile(output_path):
            vs = self._get_video_speed_factor(speed)
            cmd = (
                f"ffmpeg -i {file_path} "
                f"-filter:v setpts={vs}*PTS "
                f"-filter:a atempo={speed} "
                f"{output_path}"
            )
            proc = await asyncio.create_subprocess_shell(
                cmd=cmd,
                stdin=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await proc.communicate()
        return output_path

    def _get_video_speed_factor(self, speed):
        speed_map = {
            "0.5": 2.0,
            "0.75": 1.35,
            "1.5": 0.68,
            "2.0": 0.5,
        }
        return speed_map.get(str(speed), 1.0)

    def _update_db(self, chat_id, con_seconds, duration, dur, output_path, speed):
        db[chat_id][0]["played"] = con_seconds
        db[chat_id][0]["dur"] = duration
        db[chat_id][0]["seconds"] = dur
        db[chat_id][0]["speed_path"] = output_path
        db[chat_id][0]["speed"] = speed

    async def force_stop_stream(self, chat_id: int):
        assistant = await group_assistant(self, chat_id)
        try:
            db.get(chat_id).pop(0)
        except Exception as e:
            LOGGER(__name__).error(f"Error in force stop stream: {e}")
        await remove_active_video_chat(chat_id)
        await remove_active_chat(chat_id)
        try:
            await assistant.leave_group_call(chat_id)
        except Exception as e:
            LOGGER(__name__).error(f"Error leaving group call: {e}")

    async def skip_stream(self, chat_id: int, link: str, video: Union[bool, str] = None):
        assistant = await group_assistant(self, chat_id)
        stream = MediaStream(
            link,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.SD_480p,
        ) if video else MediaStream(
            link,
            audio_parameters=AudioQuality.HIGH,
            video_flags=MediaStream.IGNORE,
        )
        await assistant.change_stream(chat_id, stream)

    async def seek_stream(self, chat_id, file_path, to_seek, duration, mode):
        assistant = await group_assistant(self, chat_id)
        stream = MediaStream(
            file_path,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.SD_480p,
            ffmpeg_parameters=f"-ss {to_seek} -to {duration}",
        ) if mode == "video" else MediaStream(
            file_path,
            audio_parameters=AudioQuality.HIGH,
            ffmpeg_parameters=f"-ss {to_seek} -to {duration}",
            video_flags=MediaStream.IGNORE,
        )
        await assistant.change_stream(chat_id, stream)

    async def stream_call(self, link):
        assistant = await group_assistant(self, config.LOGGER_ID)
        await assistant.join_group_call(config.LOGGER_ID, MediaStream(link))
        await asyncio.sleep(0.2)
        await assistant.leave_group_call(config.LOGGER_ID)

    async def join_call(self, chat_id: int, original_chat_id: int, link, video: Union[bool, str] = None):
        assistant = await group_assistant(self, chat_id)
        language = await get_lang(chat_id)
        _ = get_string(language)
        stream = MediaStream(
            link,
            audio_parameters=AudioQuality.HIGH,
            video_parameters=VideoQuality.SD_480p,
        ) if video else MediaStream(
            link,
            audio_parameters=AudioQuality.HIGH,
            video_flags=MediaStream.IGNORE,
        )
        try:
            await assistant.join_group_call(chat_id, stream)
        except InvalidGroupCall:
            raise AssistantErr(_["call_8"])
        except AlreadyJoinedError:
            raise AssistantErr(_["call_9"])
        except InvalidMTProtoClient:
            raise AssistantErr(_["call_10"])
        except Exception as e:
            if "phone.CreateGroupCall" in str(e):
                raise AssistantErr(_["call_8"])
            LOGGER(__name__).error(f"Error joining call: {e}")

        await add_active_chat(chat_id)
        await music_on(chat_id)
        if video:
            await add_active_video_chat(chat_id)
        if await is_autoend():
            counter[chat_id] = {}
            users = len(await assistant.get_participants(chat_id))
            if users == 1:
                autoend[chat_id] = datetime.now() + timedelta(minutes=1)

    async def change_stream(self, client, chat_id):
        check = db.get(chat_id)
        popped = None
        loop = await get_loop(chat_id)
        try:
            if loop == 0:
                popped = check.pop(0)
            else:
                loop -= 1
                await set_loop(chat_id, loop)
            await auto_clean(popped)
            if not check:
                await _clear_(chat_id)
                return await client.leave
                
                return await client.leave_group_call(chat_id)
        except Exception as e:
            LOGGER(__name__).error(f"Error changing stream: {e}")
            try:
                await _clear_(chat_id)
                return await client.leave_group_call(chat_id)
            except Exception as clear_e:
                LOGGER(__name__).error(f"Error during group call leave after clear: {clear_e}")
                return

        queued = check[0]["file"]
        language = await get_lang(chat_id)
        _ = get_string(language)
        title = (check[0]["title"]).title()
        user = check[0]["by"]
        original_chat_id = check[0]["chat_id"]
        streamtype = check[0]["streamtype"]
        videoid = check[0]["vidid"]
        db[chat_id][0]["played"] = 0

        if exis := (check[0]).get("old_dur"):
            db[chat_id][0]["dur"] = exis
            db[chat_id][0]["seconds"] = check[0]["old_second"]
            db[chat_id][0]["speed_path"] = None
            db[chat_id][0]["speed"] = 1.0

        video = str(streamtype) == "video"

        if "live_" in queued:
            n, link = await YouTube.video(videoid, True)
            if n == 0:
                return await app.send_message(
                    original_chat_id,
                    text=_["call_6"],
                )
            stream = MediaStream(
                link,
                audio_parameters=AudioQuality.HIGH,
                video_parameters=VideoQuality.SD_480p,
            ) if video else MediaStream(
                link,
                audio_parameters=AudioQuality.HIGH,
                video_flags=MediaStream.IGNORE,
            )
            try:
                await client.change_stream(chat_id, stream)
            except Exception:
                return await app.send_message(
                    original_chat_id,
                    text=_["call_6"],
                )
            img = await get_thumb(videoid)
            button = stream_markup(_, chat_id)
            run = await app.send_photo(
                chat_id=original_chat_id,
                photo=img,
                caption=_["stream_1"].format(
                    f"https://t.me/{app.username}?start=info_{videoid}",
                    title[:23],
                    check[0]["dur"],
                    user,
                ),
                reply_markup=InlineKeyboardMarkup(button),
            )
            db[chat_id][0]["mystic"] = run
            db[chat_id][0]["markup"] = "tg"

        elif "vid_" in queued:
            mystic = await app.send_message(original_chat_id, _["call_7"])
            try:
                file_path, direct = await YouTube.download(
                    videoid,
                    mystic,
                    videoid=True,
                    video=str(streamtype) == "video",
                )
            except Exception as e:
                LOGGER(__name__).error(f"Error downloading YouTube video: {e}")
                try:
                    file_path, direct = await YTB.download(
                        videoid,
                        mystic,
                        videoid=True,
                        video=str(streamtype) == "video",
                    )
                except Exception as yt_error:
                    LOGGER(__name__).error(f"Error downloading video using YTB: {yt_error}")
                    return await mystic.edit_text(
                        _["call_6"], disable_web_page_preview=True
                    )

            stream = MediaStream(
                file_path,
                audio_parameters=AudioQuality.HIGH,
                video_parameters=VideoQuality.SD_480p,
            ) if video else MediaStream(
                file_path,
                audio_parameters=AudioQuality.HIGH,
                video_flags=MediaStream.IGNORE,
            )

            try:
                await client.change_stream(chat_id, stream)
            except Exception as stream_error:
                LOGGER(__name__).error(f"Error changing stream: {stream_error}")
                return await app.send_message(
                    original_chat_id,
                    text=_["call_6"],
                )

            img = await get_thumb(videoid)
            button = stream_markup(_, chat_id)
            await mystic.delete()
            run = await app.send_photo(
                chat_id=original_chat_id,
                photo=img,
                caption=_["stream_1"].format(
                    f"https://t.me/{app.username}?start=info_{videoid}",
                    title[:23],
                    check[0]["dur"],
                    user,
                ),
                reply_markup=InlineKeyboardMarkup(button),
            )
            db[chat_id][0]["mystic"] = run
            db[chat_id][0]["markup"] = "stream"

        elif "index_" in queued:
            stream = MediaStream(
                videoid,
                audio_parameters=AudioQuality.HIGH,
                video_parameters=VideoQuality.SD_480p,
            ) if str(streamtype) == "video" else MediaStream(
                videoid,
                audio_parameters=AudioQuality.HIGH,
                video_flags=MediaStream.IGNORE,
            )

            try:
                await client.change_stream(chat_id, stream)
            except Exception as index_error:
                LOGGER(__name__).error(f"Error changing stream for index: {index_error}")
                return await app.send_message(
                    original_chat_id,
                    text=_["call_6"],
                )

            button = stream_markup(_, chat_id)
            run = await app.send_photo(
                chat_id=original_chat_id,
                photo=config.STREAM_IMG_URL,
                caption=_["stream_2"].format(user),
                reply_markup=InlineKeyboardMarkup(button),
            )
            db[chat_id][0]["mystic"] = run
            db[chat_id][0]["markup"] = "tg"

        else:
            stream = MediaStream(
                queued,
                audio_parameters=AudioQuality.HIGH,
                video_parameters=VideoQuality.SD_480p,
            ) if video else MediaStream(
                queued,
                audio_parameters=AudioQuality.HIGH,
                video_flags=MediaStream.IGNORE,
            )

            try:
                await client.change_stream(chat_id, stream)
            except Exception as fallback_error:
                LOGGER(__name__).error(f"Error in fallback stream change: {fallback_error}")
                return await app.send_message(
                    original_chat_id,
                    text=_["call_6"],
                )

            if videoid == "telegram":
                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=(
                        config.TELEGRAM_AUDIO_URL
                        if str(streamtype) == "audio"
                        else config.TELEGRAM_VIDEO_URL
                    ),
                    caption=_["stream_1"].format(
                        config.SUPPORT_CHAT, title[:23], check[0]["dur"], user
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"
            elif videoid == "soundcloud":
                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=config.SOUNCLOUD_IMG_URL,
                    caption=_["stream_1"].format(
                        config.SUPPORT_CHAT, title[:23], check[0]["dur"], user
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"
            else:
                img = await get_thumb(videoid)
                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=img,
                    caption=_["stream_1"].format(
                        f"https://t.me/{app.username}?start=info_{videoid}",
                        title[:23],
                        check[0]["dur"],
                        user,
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "stream"

    async def ping(self):
        pings = []
        for call_instance in self.userbots:
            if call_instance:
                try:
                    ping = await call_instance.ping()
                    pings.append(ping)
                except Exception as e:
                    LOGGER(__name__).error(f"Error pinging call instance: {e}")
        return str(round(sum(pings) / len(pings), 3))

    async def start(self):
        LOGGER(__name__).info("Starting PyTgCalls Client...\n")
        for call_instance in self.userbots:
            if call_instance:
                try:
                    await call_instance.start()
                except Exception as e:
                    LOGGER(__name__).error(f"Error starting call instance: {e}")

    async def decorators(self):
        for call_instance in self.userbots:
            @call_instance.on_kicked()
            @call_instance.on_closed_voice_chat()
            @call_instance.on_left()
            async def stream_services_handler(_, chat_id: int):
                await self.stop_stream(chat_id)

            @call_instance.on_stream_end()
            async def stream_end_handler(client, update: Update):
                if not isinstance(update, StreamAudioEnded):
                    return
                await self.change_stream(client, update.chat_id)


# Instantiate the CallManager class
FALCON = CallManager()

