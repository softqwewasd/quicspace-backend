package com.exoquic.streaming.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exoquic.streaming.model.HlsSegment;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Service to handle SRS HTTP hooks and store/retrieve HLS segments from Kafka
 */
@Component
public class HlsKafkaService {
    private final KafkaSender<String, ByteBuffer> kafkaSender;
    private final ReceiverOptions<String, ByteBuffer> receiverOptions;
    private final StreamingMetrics metrics;
    
    // Keep track of latest segments per stream
    private final Map<String, List<HlsSegment>> liveSegments = new ConcurrentHashMap<>();
    private static final int MAX_SEGMENTS_PER_STREAM = 6; // Keep last 6 segments for live streaming
    
    // Keep track of stream metadata
    private final Map<String, Map<String, Object>> streamMetadata = new ConcurrentHashMap<>();
    
    // Kafka topics
    private final String segmentTopic;
    private final String metadataTopic;
    
    public HlsKafkaService(KafkaSender<String, ByteBuffer> kafkaSender, 
                         ReceiverOptions<String, ByteBuffer> receiverOptions,
                         StreamingMetrics metrics,
                         @Value("${app.kafka.segment-topic}") String segmentTopic,
                         @Value("${app.kafka.metadata-topic}") String metadataTopic) {
        this.kafkaSender = kafkaSender;
        this.receiverOptions = receiverOptions;
        this.metrics = metrics;
        this.segmentTopic = segmentTopic;
        this.metadataTopic = metadataTopic;
        
        // Initialize consumer for segments
        consumeSegments();
    }
    
    /**
     * Consume segments from Kafka for live streaming
     */
    private void consumeSegments() {
        ReceiverOptions<String, ByteBuffer> options = receiverOptions.subscription(Collections.singleton(segmentTopic));
        
        KafkaReceiver.create(options)
            .receive()
            .publishOn(Schedulers.boundedElastic())
            .subscribe(record -> {
                try {
                    String streamId = new String(record.headers().lastHeader("streamId").value());
                    String segmentPath = new String(record.headers().lastHeader("segmentPath").value());
                    long timestamp = record.timestamp();
                    long duration = Long.parseLong(new String(record.headers().lastHeader("duration").value()));
                    
                    // Create segment instance with metadata but keep the buffer as is for zero-copy
                    HlsSegment segment = new HlsSegment(
                        streamId,
                        timestamp,
                        segmentPath,
                        duration,
                        record.value(),
                        record.offset()
                    );
                    
                    // Track metrics
                    metrics.trackSegmentReceived(record.value());
                    
                    // Update live segments
                    liveSegments.compute(streamId, (key, segments) -> {
                        List<HlsSegment> updatedSegments = segments == null ? 
                            new ArrayList<>() : new ArrayList<>(segments);
                        updatedSegments.add(segment);
                        
                        // Keep only the latest segments
                        if (updatedSegments.size() > MAX_SEGMENTS_PER_STREAM) {
                            updatedSegments = updatedSegments.subList(
                                updatedSegments.size() - MAX_SEGMENTS_PER_STREAM,
                                updatedSegments.size()
                            );
                        }
                        return updatedSegments;
                    });
                    
                    // Acknowledge record
                    record.receiverOffset().acknowledge();
                } catch (Exception e) {
                    System.err.println("Error processing segment: " + e.getMessage());
                    e.printStackTrace();
                }
            });
    }
    
    /**
     * Extract timestamp from HLS segment filename
     */
    private long extractTimestampFromFilename(String fileName) {
        try {
            // Try to extract timestamp from filename format: streamname-timestamp.ts
            String[] parts = fileName.split("-");
            if (parts.length >= 2) {
                String timestampStr = parts[parts.length - 1].replaceAll("\\.ts$", "");
                return Long.parseLong(timestampStr);
            }
        } catch (Exception e) {
            // Ignore parsing errors
        }
        return System.currentTimeMillis();
    }
    
    /**
     * Handler for stream start event
     */
    public Mono<Void> handleStreamStart(Map<String, Object> streamInfo) {
        final String streamId = String.valueOf(streamInfo.get("stream"));
        final long startTime = System.currentTimeMillis();
        
        final Map<String, Object> metadata = new HashMap<>(streamInfo);
        metadata.put("startTime", startTime);
        metadata.put("action", "start");
        
        // Store metadata
        streamMetadata.put(streamId, metadata);
        
        // Send metadata to Kafka
        final String metadataJson = mapToJson(metadata);
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(
            metadataTopic, 
            streamId, 
            ByteBuffer.wrap(metadataJson.getBytes())
        );
        
        return kafkaSender.send(Mono.just(SenderRecord.create(record, streamId)))
            .then();
    }
    
    /**
     * Handler for stream end event
     */
    public Mono<Void> handleStreamEnd(Map<String, Object> streamInfo) {
        final String streamId = String.valueOf(streamInfo.get("stream"));
        final long endTime = System.currentTimeMillis();
        
        final Map<String, Object> metadata = new HashMap<>(streamInfo);
        metadata.put("endTime", endTime);
        metadata.put("action", "end");
        
        // Update metadata
        if (streamMetadata.containsKey(streamId)) {
            Map<String, Object> existingMetadata = streamMetadata.get(streamId);
            existingMetadata.put("endTime", endTime);
            existingMetadata.put("action", "end");
        } else {
            streamMetadata.put(streamId, metadata);
        }
        
        // Send metadata to Kafka
        String metadataJson = mapToJson(metadata);
        ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(
            metadataTopic, 
            streamId, 
            ByteBuffer.wrap(metadataJson.getBytes())
        );
        
        return kafkaSender.send(Mono.just(SenderRecord.create(record, streamId)))
            .then();
    }
    
    /**
     * Handler for HLS segment creation
     */
    public Mono<Void> handleHlsSegment(Map<String, Object> segmentInfo) {
        final String streamId = String.valueOf(segmentInfo.get("stream"));
        final String filePath = String.valueOf(segmentInfo.get("file"));
        // Get duration with fallback to "4" and handle potential numeric values
        Object durationObj = segmentInfo.getOrDefault("duration", "4");
        final long duration;
        if (durationObj instanceof Number) {
            duration = ((Number) durationObj).longValue() * 1000; // Convert to ms
        } else {
            duration = Long.parseLong(String.valueOf(durationObj)) * 1000; // Convert to ms
        }
        
        // Extract timestamp from filename
        final String fileName = Paths.get(filePath).getFileName().toString();
        final long timestamp = extractTimestampFromFilename(fileName);
        
        // Read file as direct ByteBuffer for zero-copy
        return readFileToDirectBuffer(filePath)
            .flatMap(buffer -> {
                // Create Kafka record headers with metadata
                RecordHeaders headers = new RecordHeaders();
                headers.add(new RecordHeader("streamId", streamId.getBytes()));
                headers.add(new RecordHeader("segmentPath", fileName.getBytes()));
                headers.add(new RecordHeader("duration", String.valueOf(duration).getBytes()));
                
                // Create producer record
                ProducerRecord<String, ByteBuffer> record = new ProducerRecord<>(
                    segmentTopic,
                    null,
                    timestamp,
                    streamId,
                    buffer,
                    headers
                );
                
                // Send to Kafka
                return kafkaSender.send(Mono.just(SenderRecord.create(record, null)))
                    .then();
            });
    }
    
    /**
     * Handler for HLS playlist notification
     */
    public Mono<Void> handleHlsNotify(Map<String, Object> notifyInfo) {
        // For now, just log the notification
        System.out.println("HLS Playlist updated: " + mapToJson(notifyInfo));
        return Mono.empty();
    }
    
    /**
     * Read file to direct ByteBuffer for zero-copy
     */
    private Mono<ByteBuffer> readFileToDirectBuffer(String filePath) {
        return Mono.fromCallable(() -> {
            Path path = Paths.get(filePath);
            long fileSize = Files.size(path);
            ByteBuffer buffer = ByteBuffer.allocateDirect((int) fileSize);
            
            try (AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)) {
                final AtomicLong position = new AtomicLong(0);
                final AtomicLong remaining = new AtomicLong(fileSize);
                
                // Read the entire file
                while (remaining.get() > 0) {
                    int bytesRead = channel.read(buffer, position.get()).get();
                    if (bytesRead <= 0) break;
                    position.addAndGet(bytesRead);
                    remaining.addAndGet(-bytesRead);
                }
                
                buffer.flip();
                return buffer;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    /**
     * Generate live m3u8 playlist for a stream
     */
    public Mono<String> generatePlaylist(String streamId, Long startTimeMs) {
        List<HlsSegment> segments = liveSegments.get(streamId);
        if (segments == null || segments.isEmpty()) {
            return Mono.just("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n");
        }
        
        // Generate playlist
        StringBuilder playlist = new StringBuilder();
        playlist.append("#EXTM3U\n");
        playlist.append("#EXT-X-VERSION:3\n");
        playlist.append("#EXT-X-TARGETDURATION:6\n"); // Slightly higher than segment duration
        
        // Calculate media sequence from first segment
        long mediaSequence = segments.get(0).getOffset();
        playlist.append("#EXT-X-MEDIA-SEQUENCE:" + mediaSequence + "\n");
        
        // Add segments
        for (HlsSegment segment : segments) {
            double durationSec = segment.getDuration() / 1000.0;
            playlist.append(String.format("#EXTINF:%.3f,\n", durationSec));
            playlist.append(String.format("/hls/%s/segment/%d\n", streamId, segment.getTimestamp()));
        }
        
        return Mono.just(playlist.toString());
    }
    
    /**
     * Get segment data for a stream and timestamp
     */
    public Mono<ByteBuffer> getSegmentData(String streamId, long timestamp) {
        List<HlsSegment> segments = liveSegments.get(streamId);
        if (segments == null || segments.isEmpty()) {
            return Mono.empty();
        }
        
        // Find segment with matching timestamp
        Optional<HlsSegment> segment = segments.stream()
            .filter(s -> s.getTimestamp() == timestamp)
            .findFirst();
            
        if (segment.isEmpty()) {
            return Mono.empty();
        }
        
        // Track metrics
        metrics.trackSegmentServed(segment.get().getData());
        
        // Return segment data (already a direct ByteBuffer)
        return Mono.just(segment.get().getData().duplicate());
    }
    
    /**
     * Helper method to convert map to JSON
     */
    private String mapToJson(Map<String, Object> map) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            System.err.println("Error converting map to JSON: " + e.getMessage());
            // Fallback to simple implementation if Jackson fails
            return map.entrySet().stream()
                .map(entry -> "\"" + entry.getKey() + "\":" + formatJsonValue(entry.getValue()))
                .collect(Collectors.joining(",", "{", "}"));
        }
    }
    
    /**
     * Helper method to format JSON values based on their type
     */
    private String formatJsonValue(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        } else {
            return "\"" + value.toString().replace("\"", "\\\"") + "\"";
        }
    }
}
