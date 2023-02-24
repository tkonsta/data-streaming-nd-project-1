"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from producers.models.producer import Producer
from producers.models.turnstile_hardware import TurnstileHardware

from producers.topic_config import TOPIC_BASE

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            f"{TOPIC_BASE}.turnstiles",  # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,  # TODO: Uncomment once schema is defined
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
        self.producer.produce(
            topic=self.topic_name,
            key_schema=self.key_schema,
            key={"timestamp": self.time_millis()},
            value_schema=self.value_schema,
            value={
                "station_id": self.station.station_id,
                "station_name": self.station.name,
                "line": self.station.color.name
            }
        )
