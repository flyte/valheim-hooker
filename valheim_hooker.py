import asyncio
from functools import partial
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import click
import httpx
from asyncio_mqtt import Client
from paho.mqtt.client import MQTTMessage

HandlerType = Callable[[Client, MQTTMessage], Coroutine[Any, Any, None]]


SUPPORTED_EVENTS = {
    "pre_bootstrap",
    "post_bootstrap",
    "pre_backup",
    "post_backup",
    "pre_update_check",
    "post_update_check",
    "pre_start",
    "post_start",
    "pre_restart",
    "post_restart",
    "pre_server_run",
    "post_server_run",
    "pre_server_shutdown",
    "post_server_shutdown",
}


DISCO_MSG_HANDLERS = dict(
    post_backup="Server has backed up the world.",
    post_start="Server has been started.",
    post_restart="Server has restarted.",
    post_server_run="Server is no longer running.",
    pre_server_shutdown="Server is shutting down.",
)

ArgsKwargsType = Tuple[List[Any], Dict[str, Any]]


class ValheimHooker:
    def __init__(self, mqtt: Client, discord_webhook: Optional[str] = None):
        self.mqtt = mqtt
        self.discord_webhook = discord_webhook
        self.loop = asyncio.get_event_loop()
        self.event_queues: "Dict[str, asyncio.Queue[str, List[str]]]" = {}

        handler_methods: List[Callable[[None], Coroutine[None, None, None]]] = []

        for event in SUPPORTED_EVENTS:
            method = getattr(self, f"{event}_handler", None)
            if method is None:
                continue
            handler_methods.append(method)
            self.event_queues[event] = asyncio.Queue()

        for event, msg in DISCO_MSG_HANDLERS.items():
            # TODO: Tasks pending completion -@flyte at 09/03/2021, 17:38:30
            # Is this going to have an issue with late binding closures?
            handler_methods.append(partial(self.disco_msg_handler, event, msg))
            if event in self.event_queues:
                raise ValueError(
                    (
                        "Cannot subscribe more than one handler to an event. "
                        f"Remove {event} from DISCO_MSG_HANDLERS."
                    )
                )
            self.event_queues[event] = asyncio.Queue()

        self.tasks: "asyncio.Task[None]" = [
            self.loop.create_task(handler()) for handler in handler_methods
        ]

    async def run(self) -> None:
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def pre_restart_handler(self) -> None:
        queue = self.event_queues["pre_restart"]
        while True:
            await queue.get()
            await self.send_disco("Server is going to restart in ten minutes...")
            await asyncio.sleep(60 * 5)
            await self.send_disco("Server is going to restart in five minutes...")
            await asyncio.sleep(60 * 4)
            await self.send_disco("Server is going to restart in one minute...")
            await asyncio.sleep(60 * 1)
            await self.send_disco("Server is restarting now!")
            await self.send_release("pre_restart")
            queue.task_done()

            # Clear any pending messages since we've just sent the release
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()

    async def disco_msg_handler(self, event: str, msg: str) -> None:
        queue = self.event_queues[event]
        while True:
            await queue.get()
            await self.send_disco(msg)
            await self.send_release(event)
            queue.task_done()

    async def send_disco(self, content: str) -> None:
        if self.discord_webhook is None:
            return
        async with httpx.AsyncClient() as http:
            await http.post(self.discord_webhook, json=dict(content=content))

    async def send_release(self, event: str) -> None:
        await self.mqtt.publish(f"valheim/status/{event}")

    def handle_mqtt_msg(
        self,
        message: MQTTMessage,
        **kwargs: Any,
    ) -> None:
        args: List[str] = message.payload.decode("utf8").split("|")
        event: str = args.pop(0)
        if event not in SUPPORTED_EVENTS:
            return

        event_queue = self.event_queues.get(event)
        if event_queue is not None:
            event_queue.put_nowait((event, args))


async def async_main(
    mqtt_host: str,
    mqtt_port: int,
    mqtt_user: str,
    mqtt_password: str,
    **kwargs: Any,
) -> None:
    async with Client(
        mqtt_host, mqtt_port, username=mqtt_user, password=mqtt_password
    ) as mqtt:
        hooker = ValheimHooker(mqtt, kwargs.get("discord_webhook"))

        async def handle_msgs() -> None:
            async with mqtt.filtered_messages("valheim/status") as messages:
                await mqtt.subscribe("valheim/status")
                async for msg in messages:
                    hooker.handle_mqtt_msg(msg, **kwargs)

        await asyncio.gather(hooker.run(), handle_msgs())


@click.command()
@click.option("-h", "--mqtt-host")
@click.option("-p", "--mqtt-port", type=int, default=1883)
@click.option("-u", "--mqtt-user", type=str, default=None)
@click.option("-P", "--mqtt-password", type=str, default=None)
@click.option("-d", "--discord-webhook", type=str, default=None)
def main(*args: Any, **kwargs: Any) -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main(*args, **kwargs))


if __name__ == "__main__":
    main(auto_envvar_prefix="VALHOOKER")
