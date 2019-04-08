import asyncio
import logging
from itertools import chain
from typing import Dict
from urllib.parse import unquote

import h2.config
import h2.connection
import h2.events

from uvicorn.protocols.utils import get_local_addr, get_remote_addr, is_ssl


class Stream:
    def __init__(self, stream_id, app, send):
        self.stream_id = stream_id
        self.asgi_queue = asyncio.Queue()
        self.app = app
        self.asgi_task = None
        self.send = send

    async def asgi_receive(self):
        return await self.asgi_queue.get()

    async def asgi_send(self, message: dict):
        await self.send(self.stream_id, message)

    def startup(self, scope):
        if not self.asgi_task:
            self.asgi_task = asyncio.create_task(
                self.asgi_task(scope, self.asgi_receive, self.asgi_send)
            )
        return self.asgi_task

    def disconnect(self):
        if self.asgi_task:
            self.push({"type": "http.disconnect"})

    def cancel(self):
        if self.asgi_task:
            self.asgi_task.cancel()
            self.asgi_task = None

    def __del__(self):
        if self.asgi_task:
            # TODO: emit a warning
            self.asgi_task.cancel()

    def push(self, message: dict):
        self.asgi_queue.put_nowait(message)


class H2Protocol(asyncio.Protocol):
    def __init__(self, config, server_state, _loop=None):
        if not config.loaded:
            config.load()

        self.config = config
        self.app = config.loaded_app
        self.loop = _loop or asyncio.get_event_loop()
        self.logger = config.logger_instance
        self.access_log = config.access_log and (self.logger.level <= logging.INFO)
        self.conn = h2.connection.H2Connection(
            h2.config.H2Configuration(
                client_side=False, header_encoding=None, logger=None  # = bytes
            )
        )
        self.conn.max_outbound_frame_size = 8192
        self.root_path = config.root_path
        self.limit_concurrency = config.limit_concurrency

        # Shared server state
        self.server_state = server_state
        self.connections = server_state.connections
        self.tasks = server_state.tasks
        self.default_headers = server_state.default_headers

        # Per-connection state
        self.transport = None
        self.server = None
        self.client = None
        self.scheme = None

        # Per-request state
        self.streams: Dict[str, Stream] = {}
        self.flow_control_events: Dict[str, asyncio.Event] = {}

    def connection_made(self, transport):
        self.connections.add(self)

        self.transport = transport
        self.server = get_local_addr(transport)
        self.client = get_remote_addr(transport)
        self.scheme = "https" if is_ssl(transport) else "http"

        if self.logger.level <= logging.DEBUG:
            self.logger.debug("%s - Connected", self.client)

        self.conn.initiate_connection()
        self.drain()

    def connection_lost(self, exc):
        self.connections.discard(self)

        if self.logger.level <= logging.DEBUG:
            self.logger.debug("%s - Disconnected", self.client)
        for stream in self.streams:
            stream.disconnect()

    def eof_received(self):
        pass

    def data_received(self, data):
        for event in self.conn.receive_data(data):
            if isinstance(event, h2.events.RequestReceived):
                self.handle_request_received(event)
            elif isinstance(event, h2.events.StreamEnded):
                self.handle_stream_ended(event)
            elif isinstance(event, h2.events.StreamReset):
                self.handle_stream_reset(event)
            elif isinstance(event, h2.events.DataReceived):
                self.handle_data_received(event)
            elif isinstance(event, h2.events.WindowUpdated):
                self.handle_window_updated(event)
            elif isinstance(event, h2.events.ConnectionTerminated):
                self.handle_connection_terminated(event)
            else:
                ...
        self.drain()

    def drain(self):
        data = self.conn.data_to_send()
        if data:
            self.transport.write(data)

    async def send(self, stream_id, message: dict):
        if message["type"] == "http.response.start":
            headers = chain(
                [(b":status", message["status"])],
                self.default_headers,
                message["headers"],
            )
            self.conn.send_headers(stream_id=stream_id, headers=headers)
        if message["type"] == "http.response.body":
            await self.send_data(stream_id, message["body"])
            if message.get("more_body") is True:
                self.conn.end_stream(stream_id=stream_id)
        self.drain()

    async def send_data(self, stream_id: str, data: bytes):
        """
        Send the data portion of a file. Handles flow control rules.
        """
        start = 0
        while self.transport and not self.transport.is_closing():
            while not self.conn.local_flow_control_window(stream_id):
                await self.wait_for_flow_control(stream_id)

            chunk_size = min(
                self.conn.local_flow_control_window(stream_id),
                self.conn.max_outbound_frame_size,
            )

            chunk = data[start:chunk_size]
            start = start + chunk_size
            keep_reading = len(data) == chunk_size

            self.conn.send_data(stream_id, chunk)
            self.drain()

            if not keep_reading:
                break

    def handle_request_received(self, event: h2.events.RequestReceived):
        headers = tuple(event.headers)
        special = {k: v for k, v in headers if k.startswith(":")}
        path, _, query_string = special[":path"].partition(b"?")

        # Handle 503 responses when 'limit_concurrency' is exceeded.
        if self.limit_concurrency is not None and (
            len(self.connections) >= self.limit_concurrency
            or len(self.tasks) >= self.limit_concurrency
        ):
            app = service_unavailable
            message = "Exceeded concurrency limit."
            self.logger.warning(message)
        else:
            app = self.app

        stream = self.streams[event.stream_id] = Stream(event.stream_id, app, self.send)
        task = stream.startup(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.1"},
                "http_version": "2",
                "method": special[":method"].upper(),
                "scheme": self.scheme,
                "path": unquote(path.decode("ascii")),
                "query_string": query_string,
                "root_path": self.root_path,
                "headers": headers,
                "client": self.client,
                "server": self.server,
            }
        )
        task.add_done_callback(self.tasks.discard)
        self.tasks.add(task)

    def handle_stream_ended(self, event: h2.events.StreamEnded):
        self.streams[event.stream_id].push(
            {"type": "http.response.body", "more_body": False}
        )

    def handle_stream_reset(self, event: h2.events.StreamReset):
        stream = self.streams.pop(event.stream_id, None)
        if stream:
            stream.disconnect()

    def handle_data_received(self, event: h2.events.DataReceived):
        self.streams[event.stream_id].push(
            {"type": "http.request", "body": event.data, "more_body": True}
        )

    def handle_connection_terminated(self, event: h2.events.ConnectionTerminated):
        for stream_id in list(self.streams):
            stream = self.streams.pop(stream_id)
            if stream:
                stream.disconnect()

    def handle_window_updated(self, event: h2.events.WindowUpdated):
        """
        Unblock streams waiting on flow control, if needed.
        """
        stream_id = event.stream_id

        if stream_id:
            blocked_streams = [stream_id]
        else:
            blocked_streams = list(self.flow_control_events.keys())
        for stream_id in blocked_streams:
            event = self.flow_control_events.pop(stream_id, None)
            if event:
                event.set()

    async def wait_for_flow_control(self, stream_id):
        """
        Blocks until the flow control window for a given stream is opened.
        """
        event = self.flow_control_events.setdefault(stream_id, asyncio.Event())
        await event.wait()


async def service_unavailable(scope, receive, send):
    await send(
        {
            "type": "http.response.start",
            "status": 503,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"connection", b"close"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": b"Service Unavailable"})
