#!/usr/bin/env python3


from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class."""

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the data."""
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if data is appropriate."""
        pass

    def format_output(self, result: str) -> str:
        """Format the output string."""
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    """Processor for numeric data."""

    def validate(self, data: Any) -> bool:
        """Validate numeric data."""
        try:
            # List of numbers
            for number in data:
                _ = number + 0
            return True
        except Exception:
            # Single numbers
            try:
                _ = data + 0
                return True
            except Exception:
                return False

    def process(self, data: Any) -> str:
        """Process numeric data."""
        if not self.validate(data):
            raise ValueError("Invalid numeric data")

        # Put data into a list
        numbers = []
        count = 0
        try:
            for number in data:
                numbers += [number]
                count += 1
        except Exception:
            numbers = [data]
            count = 1

        # Calculate sum and average
        total = 0
        for number in numbers:
            total += number
        avg = total / count

        return f"Processed {count} numeric values, sum={total}, avg={avg}"


class TextProcessor(DataProcessor):
    """Processor for text data."""
    def validate(self, data: Any) -> bool:
        """Validate text data."""
        try:
            _ = data + ""
            return True
        except Exception:
            return False

    def process(self, data: Any) -> str:
        """Process text data."""
        if not self.validate(data):
            raise ValueError("Invalid text data")

        char_count = 0
        word_count = 0
        in_word = False
        for char in data:
            if char == ' ' or char == '\t' or char == '\n':
                in_word = False
            elif not in_word:
                word_count += 1
                in_word = True
            char_count += 1

        return f"Processed text: {char_count} characters, {word_count} words"


class LogProcessor(DataProcessor):
    """Processor for log entry."""

    def validate(self, data: Any) -> bool:
        """Validate log entry."""
        try:
            _ = data + ""
            if (data[:5] == "ERROR" or data[:7] == "WARNING"
                    or data[:4] == "INFO"):
                return True
            return False
        except Exception:
            return False

    def process(self, data: Any) -> str:
        """Process log entry."""
        if not self.validate(data):
            raise ValueError("Invalid log entry")

        if data[:5] == "ERROR":
            log_text = f"[ALERT] ERROR level detected:{data[6:]}"
        elif data[:7] == "WARNING":
            log_text = f"[WARNING] WARNING level detected:{data[8:]}"
        else:
            log_text = f"[INFO] INFO level detected:{data[5:]}"

        return f"{log_text}"


def stream_processor() -> None:
    """Demonstrate polymorphic data processing."""
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")

    # Numeric Processor
    print("\nInitializing Numeric Processor...")
    numeric = NumericProcessor()
    print("Processing data: [1, 2, 3, 4, 5]")
    try:
        result = numeric.process([1, 2, 3, 4, 5])
        print("Validation: Numeric data verified")
        print(f"{numeric.format_output(result)}")
    except Exception as e:
        print(f"Error: {e}")

    # Text Processor
    print("\nInitializing Text Processor...")
    text = TextProcessor()
    print('Processing data: "Hello Nexus World"')
    try:
        result = text.process("Hello Nexus World")
        print("Validation: Text data verified")
        print(f"{text.format_output(result)}")
    except Exception as e:
        print(f"Error: {e}")

    # Log Processor
    print("\nInitializing Log Processor...")
    log = LogProcessor()
    print('Processing data: "ERROR: Connection timeout"')
    try:
        result = log.process("ERROR: Connection timeout")
        print("Validation: Log entry verified")
        print(f"{log.format_output(result)}")
    except Exception as e:
        print(f"Error: {e}")

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    print(f"Result 1: {NumericProcessor().process([1, 2, 3])}")
    print(f"Result 2: {TextProcessor().process('Hello World!')}")
    print(f"Result 3: {LogProcessor().process('INFO: System ready')}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    stream_processor()
