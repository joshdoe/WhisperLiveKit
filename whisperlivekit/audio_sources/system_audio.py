import asyncio
import sys
import threading
from typing import Any, AsyncGenerator, Optional


class SystemAudioSource:
    """Capture Windows system audio via a WASAPI loopback device."""

    def __init__(
        self,
        device_index: Optional[int] = None,
        sample_rate: Optional[int] = 16000,
        channels: int = 1,
        frames_per_buffer: int = 1600,
        queue_maxsize: int = 32,
    ) -> None:
        self.device_index = device_index
        self.sample_rate = sample_rate
        self.channels = channels
        self.frames_per_buffer = frames_per_buffer
        self.queue_maxsize = queue_maxsize

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue: Optional[asyncio.Queue[Optional[bytes]]] = None
        self._stop_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._reader_error: Optional[BaseException] = None

        self._pyaudio: Any = None
        self._stream: Any = None
        self._device_info: Optional[dict[str, Any]] = None

    @property
    def device_info(self) -> Optional[dict[str, Any]]:
        return self._device_info

    @staticmethod
    def _load_pyaudio() -> Any:
        if sys.platform != "win32":
            raise RuntimeError("SystemAudioSource currently supports Windows WASAPI loopback only.")

        try:
            import pyaudiowpatch as pyaudio
        except ImportError as exc:
            raise RuntimeError(
                "SystemAudioSource requires the optional PyAudioWPatch dependency on Windows."
            ) from exc

        return pyaudio

    @classmethod
    def list_loopback_devices(cls) -> list[dict[str, Any]]:
        pyaudio = cls._load_pyaudio()
        pa = pyaudio.PyAudio()
        try:
            return [dict(device) for device in pa.get_loopback_device_info_generator()]
        finally:
            pa.terminate()

    def _resolve_device(self, pa: Any) -> dict[str, Any]:
        if self.device_index is None:
            return dict(pa.get_default_wasapi_loopback())

        device_info = dict(pa.get_device_info_by_index(self.device_index))
        if device_info.get("isLoopbackDevice"):
            return device_info

        if hasattr(pa, "get_wasapi_loopback_analogue_by_dict"):
            return dict(pa.get_wasapi_loopback_analogue_by_dict(device_info))

        raise RuntimeError(f"Device index {self.device_index} is not a WASAPI loopback device.")

    def _queue_chunk(self, chunk: Optional[bytes]) -> None:
        if self._queue is None:
            return

        try:
            self._queue.put_nowait(chunk)
        except asyncio.QueueFull:
            if chunk is None:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                self._queue.put_nowait(None)

    def _finish_reader(self, error: Optional[BaseException]) -> None:
        self._reader_error = error
        self._queue_chunk(None)

    def _read_loop(self) -> None:
        error: Optional[BaseException] = None
        try:
            while not self._stop_event.is_set():
                chunk = self._stream.read(
                    self.frames_per_buffer,
                    exception_on_overflow=False,
                )
                if chunk:
                    self._loop.call_soon_threadsafe(self._queue_chunk, bytes(chunk))
        except BaseException as exc:
            error = exc
        finally:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._finish_reader, error)

    async def _close_stream(self) -> None:
        stream = self._stream
        pyaudio = self._pyaudio

        self._stream = None
        self._pyaudio = None
        self._device_info = None

        if stream is not None:
            await asyncio.to_thread(stream.stop_stream)
            await asyncio.to_thread(stream.close)

        if pyaudio is not None:
            await asyncio.to_thread(pyaudio.terminate)

    async def stream_pcm(self) -> AsyncGenerator[bytes, None]:
        if self._reader_thread is not None:
            raise RuntimeError("SystemAudioSource is already streaming.")

        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self._stop_event.clear()
        self._reader_error = None

        pyaudio = self._load_pyaudio()
        pa = pyaudio.PyAudio()
        self._pyaudio = pa
        try:
            self._device_info = self._resolve_device(pa)

            max_input_channels = int(self._device_info.get("maxInputChannels", 0))
            if max_input_channels < 1:
                raise RuntimeError("Selected WASAPI loopback device does not expose input channels.")

            channels = min(self.channels, max_input_channels)
            if channels < 1:
                raise RuntimeError("SystemAudioSource requires at least one input channel.")

            sample_rate = int(self.sample_rate or self._device_info["defaultSampleRate"])
            self._stream = pa.open(
                format=pyaudio.paInt16,
                channels=channels,
                rate=sample_rate,
                input=True,
                frames_per_buffer=self.frames_per_buffer,
                input_device_index=int(self._device_info["index"]),
            )
        except Exception:
            await self._close_stream()
            raise

        self._reader_thread = threading.Thread(
            target=self._read_loop,
            name="whisperlivekit-system-audio",
            daemon=True,
        )
        self._reader_thread.start()

        try:
            while True:
                chunk = await self._queue.get()
                if chunk is None:
                    break
                yield chunk
        finally:
            self._stop_event.set()

            thread = self._reader_thread
            self._reader_thread = None
            if thread is not None:
                await asyncio.to_thread(thread.join, 1.0)

            await self._close_stream()

        if self._reader_error is not None:
            raise RuntimeError("System audio capture stopped unexpectedly.") from self._reader_error
