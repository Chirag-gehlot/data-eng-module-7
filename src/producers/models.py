from dataclasses import dataclass
import dataclasses
import json
# Without @dataclass, you'd have to write a lot of boilerplate code yourself. Like below
# class Ride:
#     def __init__(self, PULocationID, DOLocationID, trip_distance, total_amount, tpep_pickup_datetime):
#         self.PULocationID = PULocationID
#         self.DOLocationID = DOLocationID
#         self.trip_distance = trip_distance
#         self.total_amount = total_amount
#         self.tpep_pickup_datetime = tpep_pickup_datetime


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )

# This works, but calling dataclasses.asdict() every time is tedious. We can make a serializer that handles dataclasses directly

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)