import time
import asyncio
from typing import Set

import constants as c
from utils import authenticate, process_streams_subscribers, logger, session
from state_utils import save_state_to_db, load_state_from_db
from classes import Stream


def get_streams(streams) -> Set[Stream]:
    while True:
        try:
            session.headers["auth_token"] = authenticate()
            response = session.get(c.API_BASE_URL)
            if response.status_code == 200:
                data = response.json()
                found_streams = {Stream(name=stream_name) for stream_name in data["StreamNames"]}
                streams_to_add = {stream for stream in found_streams if stream not in streams}
                removed_streams = {stream for stream in streams if stream not in found_streams}
                return streams - removed_streams | streams_to_add
        except Exception as e:
            logger.error(f"Failed to retrieve streams information. Error: {e}. Retrying in 30 seconds")
            time.sleep(30)


def setup(streams) -> Set[Stream]:
    streams_: Set[Stream] = get_streams(streams)
    streams_to_remove = set()

    # update stream shards
    for stream in streams_:
        try:
            session.headers["auth_token"] = authenticate()
            response = session.get(c.API_BASE_URL + stream.name)
            if response.status_code == 200:
                data = response.json()
                stream.update_shards(data)
        except Exception as e:
            logger.error(f"Failed to retrieve stream information for {stream.name}. Error: {e}.")
            streams_to_remove.add(stream)
    return streams_ - streams_to_remove


async def main() -> None:
    streams: Set[Stream] = load_state_from_db()

    while True:
        streams = setup(streams)
        process_streams_subscribers(streams)

        if streams:
            for stream in streams:
                records = stream.get_records()
                for record in records:

                    await stream.send_data_to_subscribers(record)
                print("--------------------") #TODO remove print in pord

        save_state_to_db(streams)
        time.sleep(1) # most probably dont need to wait for a whole sec, but no time to test Kinesis throttling, TODO. 


if __name__ == "__main__":
    asyncio.run(main())