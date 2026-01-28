#!/usr/bin/env python3


from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class."""

    def __init__(self, stream_id: str) -> None:
        """Initialize the stream with an ID."""
        self.stream_id = stream_id
        self.processed_count = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data."""
        pass

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter data based on criteria"""
        if criteria is None:
            return data_batch
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Return stream statistics."""
        return {"stream_id": self.stream_id,
                "processed_count": self.processed_count}


class SensorStream(DataStream):
    """Stream handler for environmental sensor data."""

    def __init__(self, stream_id: str) -> None:
        """Initialize sensor stream."""
        super().__init__(stream_id)
        self.total_temp = 0.0
        self.temp_count = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor data batch."""
        for item in data_batch:
            if isinstance(item, str) and "temp:" in item:
                temp = float(item.split("temp:")[1])
                self.total_temp += temp
                self.temp_count += 1

        self.processed_count += len(data_batch)
        if self.temp_count > 0:
            avg_temp = self.total_temp / self.temp_count
        else:
            avg_temp = 0

        return (f"Sensor analysis: {len(data_batch)} readings processed, "
                f"avg temp: {avg_temp}°C")

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter sensor data based on criteria."""
        if criteria == "high-priority":
            return [item for item in data_batch
                    if isinstance(item, str) and "temp:" in item
                    and float(item.split("temp:")[1]) > 25]
        return data_batch


class TransactionStream(DataStream):
    """Stream handler for financial transaction data."""

    def __init__(self, stream_id: str) -> None:
        """Initialize transaction stream."""
        super().__init__(stream_id)
        self.net_flow = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transaction data batch."""
        for item in data_batch:
            if isinstance(item, str):
                if "buy:" in item:
                    self.net_flow += int(item.split("buy:")[1])
                elif "sell:" in item:
                    self.net_flow -= int(item.split("sell:")[1])

        self.processed_count += len(data_batch)
        sign = "+" if self.net_flow >= 0 else ""

        return (f"Transaction analysis: {len(data_batch)} operations, "
                f"net flow: {sign}{self.net_flow} units")

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter transaction data based on criteria."""
        if criteria == "high-priority":
            return [item for item in data_batch
                    if isinstance(item, str) and ":" in item
                    and int(item.split(":")[1]) > 100]
        return data_batch


class EventStream(DataStream):
    """Stream handler for system events."""

    def __init__(self, stream_id: str) -> None:
        """Initialize event stream."""
        super().__init__(stream_id)
        self.error_count = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process event data batch."""
        for item in data_batch:
            if isinstance(item, str) and item == "error":
                self.error_count += 1

        self.processed_count += len(data_batch)

        return (f"Event analysis: {len(data_batch)} events, "
                f"{self.error_count} error detected")

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter event data based on criteria."""
        if criteria == "high-priority":
            return [item for item in data_batch
                    if isinstance(item, str) and item == "error"]
        return data_batch


class StreamProcessor:
    """Unified processor to handle multiple stream types polymorphically."""

    def __init__(self) -> None:
        """Initialize the stream processor."""
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        """Add a stream to the processor."""
        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        """Process data batches across all streams polymorphically."""
        results = []
        i = 0
        for stream in self.streams:
            try:
                result = stream.process_batch(data_batches[i])
                results.append(result)
                i += 1
            except Exception as e:
                results.append(f"Error: {e}")
                i += 1
        return results


def data_stream() -> None:
    """Demonstrate polymorphic stream processing."""
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    # Initialize and demo individual streams
    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print("Stream ID: SENSOR_001, Type: Environmental Data")
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    print(sensor.process_batch(["temp:22.5", "humidity:65", "pressure:1013"]))

    print("\nInitializing Transaction Stream...")
    transaction = TransactionStream("TRANS_001")
    print("Stream ID: TRANS_001, Type: Financial Data")
    print("Processing transaction batch: [buy:100, sell:150, buy:75]")
    print(transaction.process_batch(["buy:100", "sell:150", "buy:75"]))

    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print("Stream ID: EVENT_001, Type: System Events")
    print("Processing event batch: [login, error, logout]")
    print(event.process_batch(["login", "error", "logout"]))

    # Polymorphic processing demo
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()
    processor.add_stream(SensorStream("SENSOR_002"))
    processor.add_stream(TransactionStream("TRANS_002"))
    processor.add_stream(EventStream("EVENT_002"))

    batches = [
        ["temp:20.0", "humidity:70"],
        ["buy:50", "sell:30", "buy:40", "sell:20"],
        ["login", "warning", "logout"]
    ]

    results = processor.process_all(batches)

    print("\nBatch 1 Results:")
    for result in results:
        print(result)

    print("\nStream filtering active: High-priority data only")

    # Demonstrate filtering
    sensor_filtered = SensorStream("SENSOR_003").filter_data(
        ["temp:26.0", "temp:24.0", "temp:27.0"], "high-priority"
    )
    trans_filtered = TransactionStream("TRANS_003").filter_data(
        ["buy:150", "sell:50"], "high-priority"
    )

    print(f"Filtered results: {len(sensor_filtered)} critical sensor alerts, "
          f"{len(trans_filtered)} large transaction")

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    data_stream()
