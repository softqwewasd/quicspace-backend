# Exoquic Live Streaming

A reactive HLS streaming application using Kafka and Spring Boot.

## Overview

This application provides a robust and scalable solution for handling HLS (HTTP Live Streaming) content with Kafka as a message broker. It's designed to work with SRS (Simple RTMP Server) HTTP callbacks to handle live streaming events and segments.

## Features

- Reactive programming with Spring WebFlux and Reactor
- Kafka integration for segment storage and retrieval
- HTTP API for SRS callbacks
- Dynamic HLS playlist generation
- Zero-copy ByteBuffer operations for high performance

## Requirements

- Java 17 or higher
- Apache Kafka (running on localhost:9092 by default)
- SRS (Simple RTMP Server) configured to call the HTTP callbacks

## Configuration

The application configuration is in `src/main/resources/application.properties`. Key settings include:

```properties
# Server configuration
server.port=8080

# Custom application properties
app.http-server.port=8000
app.kafka.bootstrap-servers=localhost:9092
app.kafka.consumer-group=hls-segment-consumer
app.kafka.segment-topic=stream-segments
app.kafka.metadata-topic=stream-metadata
```

## Building

```bash
mvn clean package
```

## Running

```bash
mvn spring-boot:run
```

Or with the JAR file:

```bash
java -jar target/exoquic-live-streaming-0.0.1-SNAPSHOT.jar
```

## HTTP Endpoints

### SRS Callbacks

- POST `/api/stream/start` - Called when a stream starts
- POST `/api/stream/end` - Called when a stream ends
- POST `/api/stream/hls` - Called when a new HLS segment is created
- POST `/api/stream/notify` - Called when HLS playlist is updated

### HLS Endpoints

- GET `/hls/{streamId}.m3u8` - Get the HLS playlist for a stream
- GET `/hls/{streamId}/segment/{timestamp}` - Get a specific HLS segment

## Architecture

The application uses a reactive architecture with the following components:

- `KafkaStreamingApplication` - Main Spring Boot application
- `HlsSegment` - Model class for HLS segments
- `KafkaConfig` - Kafka configuration
- `HlsKafkaService` - Service to handle SRS hooks and store/retrieve HLS segments
- `HttpServerConfig` - HTTP server for SRS hooks and HLS content
- `StreamingMetrics` - Metrics tracking utility
