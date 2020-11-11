"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker=["kafka://localhost:9092"], store="memory://")
topic = app.topic("com.udacity.project1.stations", value_type=Station)
out_topic = app.topic("com.udacity.project1.stations_clean", partitions=1)
table = app.Table(
   "stations",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def stationevent(station_events):
    async for se in station_events:
        color = None
        if se.red:
            color = 'red'
        elif se.blue:
            color = 'blue'
        elif se.green:
            color = 'green'
        
        if color is None:
            print(f"warning - no color! - {se}")
            continue

        table[se.station_id] = TransformedStation(
            station_id = se.station_id,
            station_name = se.station_name,
            order = se.order,
            line = color
        )
        print(f"{se.station_id} processed")


if __name__ == "__main__":
    app.main()
