#!/usr/bin/env python3

import argparse
import pickle
import struct
import sys
import time
import psycopg
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Dict
from datetime import datetime, timezone
import base64

import numpy as np


def read_exact(stream, n: int) -> bytes:
    """Read exactly *n* bytes from *stream*, or return empty on EOF."""
    data = b""
    while len(data) < n:
        chunk = stream.read(n - len(data))
        if not chunk:
            return b""
        data += chunk
    return data


@dataclass
class ParticlePacket:
    """Each packet contains N particles"""
    dict[str, np.array]


class DataReader(ABC):
    """Abstract interface to read data"""

    @abstractmethod
    def read(self) -> Iterable[ParticlePacket]:
        """Read data and return a ParticlePacket iterable"""
        pass


class StdinPickleReader(DataReader):
    """Read an expected stream of pickle data from stdin.

    Packets arrive as length-prefixed pickled blobs on stdin
    (4-byte big-endian length header followed by the pickle
    payload).
    Each packet contains N particles
    """
    def read(self) -> [dict[str, np.array], int]:
        # FIXME to rewrite
        stdin = sys.stdin.buffer

        # Read the 4-byte length prefix
        header = read_exact(stdin, 4)
        if not header:
            return b"", 0

        (length,) = struct.unpack(">I", header)

        # Read the payload
        payload = read_exact(stdin, length)
        if not payload:
            raise ValueError("Empty payload")

        # Deserialize
        return pickle.loads(payload), len(payload) + 4


class DataStorage(ABC):
    """Abstract interface for storage."""

    @abstractmethod
    def setup(self) -> None:
        """storage setup"""
        pass

    @abstractmethod
    def save(self, data: ParticlePacket) -> None:
        """saving data"""
        pass

    @abstractmethod
    def read_by_date_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> Iterable[ParticlePacket]:
        """extract data from storage with timestamp filters"""
        pass

    @abstractmethod
    def close(self) -> None:
        """close every connections"""
        pass


class PostgresStorage(DataStorage):
    """Main storage in a PostgreSQL database"""

    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.conn = None
        self.cursor = None

    def setup(self) -> None:
        self.conn = psycopg.connect(**self.config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS particle (
                scattering TEXT NOT NULL,
                spectral TEXT NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        """)

    def save(self, packets: ParticlePacket) -> None:
        """ numpy data is stored as base64 encoded on the database."""
        if not self.cursor:
            raise RuntimeError("Database is not initialized")

        """ Get the timestamps array in order to count the packets number
            use len() as it is a list
        """
        packet_nbr = len(packets["timestamps"])
        for p in range(packet_nbr):
            self.cursor.execute(
                """INSERT INTO particle (timestamp, scattering, spectral)
                   VALUES (%s, %s, %s)""",
                (
                    datetime.fromtimestamp(packets["timestamps"][p],
                                           tz=timezone.utc).isoformat(),
                    base64.standard_b64encode(packets["scattering"][p]),
                    base64.standard_b64encode(packets["spectral"][p])
                )
            )

    def read_by_date_range(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ) -> Iterable[ParticlePacket]:
        if not self.cursor:
            raise RuntimeError("Database is not initialized")

        print(f"Reading data between {start_date} and {end_date}")
        t00 = time.monotonic()

        self.cursor.execute(
            """
            SELECT timestamp, scattering, spectral
            FROM particle
            WHERE timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp
            """,
            (start_date, end_date)
        )

        res = self.cursor.fetchall()
        if len(res) == 0:
            print("Not data found")
            return False

        ts = []
        scat = []
        spec = []

        for row in res:
            # FIXME use np and ParticlePacket
            # and base64.standard_b64decode scat/spec
            ts.append(row[0])
            scat.append(row[1])
            spec.append(row[2])

        ts = np.hstack(ts)
        packets = {
            "timestamps": ts,
            "scattering": np.vstack(scat),
            "spectral": np.vstack(spec)
        }
        print(f"Found {len(res)} particles")
        print(
            f"Read bandwidth "
            f"{len(ts)/1024/(time.monotonic() - t00):.2f} "
            f"kParticles/s."
        )

        return packets

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class cmd_write:
    """
    Process data
    """

    def __init__(self, streamreader: DataReader, storage: DataStorage):
        self.reader = streamreader
        self.storage = storage

    def process(self) -> None:
        """Reading data"""
        self.storage.setup()
        total_bytes_received = 0
        packets_written = 0

        try:
            while True:
                packet, raw_data_length = self.reader.read()
                if packet == b"":
                    break

                # check save in bulk in db
                self.storage.save(packet)
                packets_written += 1
                total_bytes_received += raw_data_length

            print(
                f"Wrote to storage {packets_written} packets "
                f"({total_bytes_received} bytes).",
                file=sys.stderr,
            )

        finally:
            self.storage.close()


class cmd_read:
    """
    Read data from storage
    """

    def __init__(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            storage: DataStorage
            ):
        self.storage = storage
        self.start = start_time
        self.end = end_time

    def process(self) -> None:
        """Reading data"""
        self.storage.setup()
        try:
            self.storage.read_by_date_range(self.start, self.end)
        finally:
            self.storage.close()


def main():
    # FIXME the password should be considered SECRET
    # NOT PRODUCTION READY
    db_params = {
        "dbname": "storage",
        "password": "Vinyl3Coffin",
        "user": "USER",
        "host": "localhost",
        "port": "5432"
    }

    parser = argparse.ArgumentParser(
        description="New example of data storage system",
    )

    sub = parser.add_subparsers(dest="command")

    # -- write --
    sub.add_parser("write", help="Ingest packets from stdin")

    # -- read --
    p_r = sub.add_parser("read", help="Query by time range")
    p_r.add_argument(
        "--start",
        required=True,
        help="Start timestamp (ISO 8601 format, inclusive)",
    )
    p_r.add_argument(
        "--stop",
        required=True,
        help="Stop timestamp (ISO 8601 format, inclusive)",
    )

    args = parser.parse_args()

    storage = PostgresStorage(db_params)

    if args.command == "write":
        # FIXME test storage capacity at least 1TB free space
        streamreader = StdinPickleReader()
        processor = cmd_write(streamreader, storage)
    elif args.command == "read":
        processor = cmd_read(
            datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc),
            datetime.fromisoformat(args.stop).replace(tzinfo=timezone.utc),
            storage)
    else:
        parser.print_help()
        sys.exit(1)

    try:
        processor.process()
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
