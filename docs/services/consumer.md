# Consumer Service

> TODO: Pulsar consumer & frame processing

## Overview

The consumer receives frame events from Pulsar using a Key_Shared subscription, ensuring ordering per device while allowing horizontal scaling.

## Key Concepts

- **Key_Shared Subscription**: Messages with same key (deviceId) go to same consumer
- **Frame Accumulation**: Frames are buffered until segment threshold
- **Segment Triggers**: By frame count or time threshold

## Configuration

See [Configuration](../configuration.md#pulsar-pulsar_)

## Code Reference

- `src/stream_processor/consumer/pulsar_consumer.py`
