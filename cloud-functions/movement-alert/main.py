import asyncio

import json
from typing import Any
import functions_framework
from flask import Request
from flask.typing import ResponseReturnValue
from pydantic_settings import BaseSettings, SettingsConfigDict

from viam.robot.client import RobotClient
from viam.components.board import Board


class Settings(BaseSettings):
    viam_api_key: str = ""
    viam_api_key_id: str = ""
    machine_address: str = ""
    board_name: str = ""
    pin_name: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


async def connect(settings: Settings):
    opts = RobotClient.Options.with_api_key(
        api_key=settings.viam_api_key,
        api_key_id=settings.viam_api_key_id,
    )
    return await RobotClient.at_address(settings.machine_address, opts)


async def blink_led(settings: Settings, data: dict[str, Any]):
    machine = await connect(settings)
    pi = Board.from_robot(machine, settings.board_name)
    pin = await pi.gpio_pin_by_name(settings.pin_name)

    count = 0
    while count < 10:
        await pin.set(True)
        await asyncio.sleep(0.5)

        await pin.set(False)
        await asyncio.sleep(0.5)

        count += 1

    await machine.close()


@functions_framework.http
def alert_movement(request: Request) -> ResponseReturnValue:
    if request.method != "POST":
        return "Ok"

    payload = request.form

    if payload is None:
        return "Ok"

    settings = Settings()

    print("Got the following data:")
    data: dict[str, Any] = json.loads(list(payload.keys())[0])
    print(data)

    asyncio.run(blink_led(settings, data))

    return "Ok"
