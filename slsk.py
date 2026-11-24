''' Fetches songs that are not in the Subsonic library: Soulseek (sockseek in another container),
then YouTube (yt-dlp in that container). Results are kept in a size-limited cache in this container.

The whole /play text is sent to sockseek as one search string ("-s" song mode, like typing it into the
Soulseek search bar; "Artist - Song" is split into artist and title by sockseek).

The sockseek container is reached with SOCKSEEK_EXEC, e.g.
    docker exec -u 1000:1000 -e HOME=/tmp -i sockseek
(needs the docker CLI and the docker socket in this container). Settings (all optional except the
Soulseek login) are read from the environment / data.env:
    SOCKSEEK_EXEC, SOCKSEEK_ARGS (default "--no-listen"), SLSK_USER, SLSK_PASS,
    SOCKSEEK_TMP_DIR (default /tmp/discodrome, inside the sockseek container),
    SLSK_CACHE_DIR (default cache/songs), SLSK_CACHE_MAX_MB (default 500),
    SLSK_TIMEOUT_S (default 90), SLSK_MAX_STALE_MS (default 30000), YT_TIMEOUT_S (default 120),
    YTDLP_ARGS (extra yt-dlp arguments)
'''

import asyncio
import hashlib
import json
import logging
import os
import shlex
import uuid
from pathlib import Path

from util import env  # noqa: F401  (loads data.env into the environment)
import loudness
from subsonic import Song

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("SLSK_CACHE_DIR", "cache/songs"))
CACHE_MAX_BYTES = int(float(os.getenv("SLSK_CACHE_MAX_MB", "500")) * 1_000_000)
SK_EXEC = shlex.split(os.getenv("SOCKSEEK_EXEC") or "docker exec -i sockseek")
SK_ARGS = shlex.split(os.getenv("SOCKSEEK_ARGS") if os.getenv("SOCKSEEK_ARGS") is not None else "--no-listen")
SLSK_USER = os.getenv("SLSK_USER", "")
SLSK_PASS = os.getenv("SLSK_PASS", "")
REMOTE_TMP = (os.getenv("SOCKSEEK_TMP_DIR") or "/tmp/discodrome").rstrip("/")
SLSK_TIMEOUT = int(os.getenv("SLSK_TIMEOUT_S") or 90)
STALE_MS = os.getenv("SLSK_MAX_STALE_MS") or "30000"
YT_TIMEOUT = int(os.getenv("YT_TIMEOUT_S") or 120)
YTDLP_ARGS = shlex.split(os.getenv("YTDLP_ARGS", ""))
AUDIO_EXT = {".mp3", ".flac", ".m4a", ".ogg", ".opus", ".wav", ".aac", ".wma", ".webm"}
SOURCE_LABEL = {"soulseek": "Soulseek", "youtube": "YouTube"}


def enabled() -> bool:
    ''' True when a Soulseek login is configured '''
    return bool(SLSK_USER and SLSK_PASS)


def clean_query(text: str) -> str:
    ''' The search string sent to sockseek: whitespace collapsed; no "=" (sockseek reads "key=value"
    pairs) and no leading "-" (it would look like an option) '''
    return " ".join((text or "").replace("=", " ").split()).lstrip("- ")


class LocalSong(Song):
    ''' A song that was fetched from Soulseek or YouTube and lives in the local cache '''
    def __init__(self, key: str, query: str, path: Path, source: str, info: dict,
                 measured: dict | None = None, cached: bool = False) -> None:
        label = SOURCE_LABEL.get(source, "Cache")
        super().__init__({"id": f"local:{key}", "title": info.get("title") or query,
                          "artist": info.get("artist") or label, "album": info.get("album") or label,
                          "duration": info.get("duration", 0), "suffix": info.get("codec", ""),
                          "bitRate": info.get("bitRate", 0), "bitDepth": info.get("bitDepth", 0),
                          "samplingRate": info.get("samplingRate", 0), "channelCount": info.get("channelCount", 0)})
        self.path = path
        self.source = source
        self.loudness = measured
        self._origin = label + (", cached" if cached else "")

    @property
    def quality(self) -> str:
        base = super().quality
        return f"{base} · {self._origin}" if base else self._origin


def is_local(song) -> bool:
    return isinstance(song, LocalSong)


# ---- cache ---------------------------------------------------------------------------------
def _key(query: str) -> str:
    return hashlib.sha1(query.casefold().encode()).hexdigest()[:16]


def _meta_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"  # source and loudness measurement of the cached file


def _read_meta(key: str) -> dict:
    try:
        return json.loads(_meta_path(key).read_text())
    except Exception:
        return {}


def _lookup(key: str) -> Path | None:
    for p in CACHE_DIR.glob(f"{key}.*"):
        if p.suffix in AUDIO_EXT and p.is_file():
            return p
    return None


def _in_use() -> set[Path]:
    ''' Cached files that are the current song or in a queue (including queues loaded from disk) '''
    import data  # imported here: data imports player, which imports this module
    paths: set[Path] = set()
    for guild in data._guild_data_instances.values():
        for s in [guild.player.current_song, *guild.player.queue]:
            if is_local(s):
                paths.add(s.path)
    for props in data._guild_property_instances.values():
        for s in props.queue or []:
            if is_local(s):
                paths.add(s.path)
    return paths


def _evict(keep: Path | None = None) -> None:
    ''' Deletes the least recently used files until the cache is within the limit. Files that are
    queued or playing are kept, so the cache can exceed the limit while they alone are larger. '''
    files = [p for p in CACHE_DIR.iterdir() if p.is_file() and p.suffix in AUDIO_EXT]
    total = sum(p.stat().st_size for p in files)
    if total <= CACHE_MAX_BYTES:
        return
    protected = _in_use() | ({keep} if keep else set())
    for p in sorted(files, key=lambda p: p.stat().st_mtime):
        if total <= CACHE_MAX_BYTES:
            break
        if p in protected:
            continue
        total -= p.stat().st_size
        p.unlink(missing_ok=True)
        p.with_suffix(".json").unlink(missing_ok=True)
        logger.debug("cache: evicted %s", p.name)


# ---- container access ----------------------------------------------------------------------
async def _exec(*args: str, stdin: bytes | None = None, timeout: float = 30, stdout=asyncio.subprocess.PIPE):
    proc = await asyncio.create_subprocess_exec(
        *SK_EXEC, *args,
        stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=stdout, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(stdin), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    return proc.returncode, out or b"", err or b""


async def _list_audio(rdir: str) -> list[str]:
    rc, out, _ = await _exec("find", f"{rdir}/out", "-type", "f")
    files = [l for l in out.decode(errors="replace").splitlines() if l]
    return [f for f in files if os.path.splitext(f)[1].lower() in AUDIO_EXT] if rc == 0 else []


async def _pull(remote: str, dest: Path) -> bool:
    ''' Copies a file out of the sockseek container into the cache '''
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    with open(tmp, "wb") as f:
        rc, _, err = await _exec("cat", "--", remote, stdout=f, timeout=120)
    if rc != 0 or tmp.stat().st_size == 0:
        logger.warning("copy from the sockseek container failed: %s", err.decode(errors="replace")[:200])
        tmp.unlink(missing_ok=True)
        return False
    tmp.replace(dest)
    return True


_CODEC_NAMES = {"mp3": "mp3", "flac": "flac", "aac": "aac", "opus": "opus", "vorbis": "ogg", "alac": "alac",
                "wmav2": "wma", "wmapro": "wma"}


async def _probe(path: Path) -> dict:
    ''' Tags and audio properties of a file (ffprobe); an empty dict when it cannot be read '''
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", str(path),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(proc.communicate(), 30)
        j = json.loads(out)
        st = next((s for s in j.get("streams", []) if s.get("codec_type") == "audio"), {})
        fmt = j.get("format", {})
        tags = {k.lower(): v for k, v in {**st.get("tags", {}), **fmt.get("tags", {})}.items()}
        codec = st.get("codec_name", "")
        codec = "wav" if codec.startswith("pcm") else _CODEC_NAMES.get(codec, codec)
        return {"duration": int(float(fmt.get("duration") or st.get("duration") or 0)),
                "codec": codec,
                "bitRate": int(float(st.get("bit_rate") or fmt.get("bit_rate") or 0) / 1000),
                "bitDepth": int(st.get("bits_per_raw_sample") or 0) or int(st.get("bits_per_sample") or 0),
                "samplingRate": int(st.get("sample_rate") or 0),
                "channelCount": int(st.get("channels") or 0),
                "artist": tags.get("artist") or tags.get("album_artist"),
                "title": tags.get("title"), "album": tags.get("album")}
    except Exception:
        return {}


async def _soulseek(query: str, rdir: str) -> str | None:
    rc, _, err = await _exec("mkdir", "-p", f"{rdir}/out")
    if rc != 0:
        raise RuntimeError(f"cannot write to the sockseek container: {err.decode(errors='replace')[:200]}")
    cmd = ["timeout", str(SLSK_TIMEOUT), "sockseek", query, "-s",
           "--output-dir", f"{rdir}/out",
           "--name-format", "dl/{sartist( - )stitle|stitle|slsk-filename}",
           "--index-path", f"{rdir}/index.csv",
           "--max-stale-time", STALE_MS,
           "--user", SLSK_USER, "--pass", SLSK_PASS, *SK_ARGS]
    rc, out, err = await _exec(*cmd, timeout=SLSK_TIMEOUT + 15)
    files = await _list_audio(rdir)
    if not files:
        logger.info("sockseek: nothing for %r (exit %s) %s", query, rc, (err or out).decode(errors="replace")[-200:])
    return files[0] if files else None


async def _youtube(query: str, rdir: str) -> str | None:
    rc, out, err = await _exec(
        "timeout", str(YT_TIMEOUT), "yt-dlp", "-q", "--no-warnings", "--no-playlist", "--no-simulate",
        "-f", "bestaudio/best", "-x", "--match-filter", "duration<=900 & !is_live", *YTDLP_ARGS,
        "-o", f"{rdir}/out/yt/%(title)s [%(id)s].%(ext)s", "--print", "after_move:filepath",
        f"ytsearch1:{query}", timeout=YT_TIMEOUT + 15)
    lines = [l for l in out.decode(errors="replace").splitlines() if l.strip()]
    if rc != 0 or not lines:
        logger.info("yt-dlp: nothing for %r (exit %s) %s", query, rc, err.decode(errors="replace")[-200:])
        return None
    return lines[-1]


# ---- public API ----------------------------------------------------------------------------
_sk_lock = asyncio.Lock()  # one search at a time: one Soulseek login, one listen port
_inflight: dict[str, asyncio.Task] = {}


async def _download(key: str, query: str) -> Path | None:
    async with _sk_lock:
        rdir = f"{REMOTE_TMP}/{uuid.uuid4().hex[:12]}"
        try:
            remote, source = None, "soulseek"
            try:
                remote = await _soulseek(query, rdir)
            except (asyncio.TimeoutError, RuntimeError, OSError) as e:
                logger.warning("Soulseek search failed: %s", e)
            if remote is None:
                remote, source = await _youtube(query, rdir), "youtube"
            if remote is None:
                return None
            dest = CACHE_DIR / f"{key}{os.path.splitext(remote)[1].lower()}"
            if not await _pull(remote, dest):
                return None
            measured = await loudness.measure(dest) if loudness.ENABLED else None
            _meta_path(key).write_text(json.dumps({"source": source, "query": query, "loudness": measured}))
            return dest
        except (asyncio.TimeoutError, OSError) as e:
            logger.warning("fetching %r failed: %s", query, e)
            return None
        finally:
            try:
                await _exec("rm", "-rf", "--", rdir, timeout=30)
            except Exception:
                pass


async def fetch_song(query: str) -> LocalSong | None:
    ''' Returns the song as a local file (cache, Soulseek, then YouTube), or None if nothing was found '''
    query = clean_query(query)
    if not query:
        return None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = _key(query)
    path = _lookup(key)
    cached = path is not None
    if path:
        os.utime(path)  # most recently used
    else:
        task = _inflight.get(key)
        if task is None:  # identical simultaneous requests share one download
            task = asyncio.create_task(_download(key, query))
            _inflight[key] = task
            task.add_done_callback(lambda _t, k=key: _inflight.pop(k, None))
        path = await asyncio.shield(task)
        if path is None:
            return None
        _evict(keep=path)
    meta = _read_meta(key)
    return LocalSong(key, query, path, meta.get("source", "cache"), await _probe(path),
                     meta.get("loudness"), cached)
