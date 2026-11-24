''' Loudness normalisation (EBU R128 loudnorm).

Songs from the Subsonic server are streamed, so they are normalised in a single pass.
Songs fetched from Soulseek/YouTube are measured once when they are downloaded (two-pass loudnorm,
linear gain) and the measurement is stored next to the cached file.

Settings (environment / data.env): NORMALIZE_AUDIO (1; 0 = the old ReplayGain-tag-only behaviour),
LOUDNORM_I (target loudness in LUFS, default -16), LOUDNORM_TP (true peak limit in dBTP, default -1.5),
LOUDNORM_LRA (loudness range, default 11).
'''

import asyncio
import json
import logging
import math
import os
import re

from util import env  # noqa: F401  (loads data.env into the environment)

logger = logging.getLogger(__name__)

ENABLED = os.getenv("NORMALIZE_AUDIO", "1").strip().lower() not in ("0", "false", "no", "off")
TARGET_I = float(os.getenv("LOUDNORM_I") or -16)
TARGET_TP = float(os.getenv("LOUDNORM_TP") or -1.5)
TARGET_LRA = float(os.getenv("LOUDNORM_LRA") or 11)
_TARGET = f"I={TARGET_I:g}:TP={TARGET_TP:g}:LRA={TARGET_LRA:g}"


def stream_filter() -> str:
    ''' ffmpeg audio filter for a stream that cannot be measured beforehand (single pass) '''
    return f"loudnorm={_TARGET}" if ENABLED else "volume=replaygain=track"


def file_filter(measured: dict | None) -> str:
    ''' ffmpeg audio filter for a local file; two-pass when its measurement is known '''
    if not ENABLED:
        return "volume=replaygain=track"
    if not measured:
        return f"loudnorm={_TARGET}"
    return (f"loudnorm={_TARGET}:measured_I={measured['i']}:measured_TP={measured['tp']}"
            f":measured_LRA={measured['lra']}:measured_thresh={measured['thresh']}"
            f":offset={measured['offset']}:linear=true")


async def measure(path) -> dict | None:
    ''' First loudnorm pass over a file. Returns the measurement, or None when it failed '''
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-nostats", "-nostdin", "-i", str(path), "-vn",
            "-af", f"loudnorm={_TARGET}:print_format=json", "-f", "null", "-",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, err = await asyncio.wait_for(proc.communicate(), 180)
        blocks = re.findall(r"\{[^{}]*\}", err.decode(errors="replace"))
        data = json.loads(blocks[-1])
        m = {"i": float(data["input_i"]), "tp": float(data["input_tp"]), "lra": float(data["input_lra"]),
             "thresh": float(data["input_thresh"]), "offset": float(data["target_offset"])}
        if not all(math.isfinite(v) for v in m.values()):
            return None  # silent or unmeasurable file
        return m
    except Exception as e:
        logger.warning("loudness measurement of %s failed: %s", path, e)
        return None
