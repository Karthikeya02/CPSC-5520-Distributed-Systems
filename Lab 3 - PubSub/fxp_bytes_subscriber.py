import struct
from datetime import datetime, timedelta

MICROS_PER_SECOND = 1_000_000


def deserialize_price(b: bytes) -> float:
    """
    Convert 4 bytes to a float (IEEE 754 binary32 little-endian format).
    """
    return struct.unpack('<f', b)[0]


def deserialize_utcdatetime(b: bytes) -> datetime:
    """
    Convert 8 bytes to a datetime object.
    """
    micros = int.from_bytes(b, byteorder='big')
    epoch = datetime(1970, 1, 1)
    return epoch + timedelta(microseconds=micros)


def parse_message(message: bytes) -> list:
    """
    Parse the received message and return a list of quotes.
    Each quote is a dictionary with keys:
    - 'currency1'
    - 'currency2'
    - 'rate'
    - 'timestamp'
    """
    quotes = []
    record_size = 32
    num_records = len(message) // record_size
    for i in range(num_records):
        record = message[i * record_size: (i + 1) * record_size]
        currency1 = record[0:3].decode('ascii')
        currency2 = record[3:6].decode('ascii')
        rate = deserialize_price(record[6:10])
        timestamp = deserialize_utcdatetime(record[10:18])
        quotes.append({
            'currency1': currency1,
            'currency2': currency2,
            'rate': rate,
            'timestamp': timestamp
        })
    return quotes