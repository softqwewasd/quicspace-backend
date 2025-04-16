package com.exoquic.streaming.model;

import java.nio.ByteBuffer;

/**
 * Model class for HLS segments with metadata
 */
public class HlsSegment {
    private final String streamId;
    private final long timestamp;
    private final String segmentPath;
    private final long duration;
    private final ByteBuffer data;
    private final long offset;
    
    public HlsSegment(String streamId, long timestamp, String segmentPath, long duration, ByteBuffer data, long offset) {
        this.streamId = streamId;
        this.timestamp = timestamp;
        this.segmentPath = segmentPath;
        this.duration = duration;
        this.data = data;
        this.offset = offset;
    }
    
    public String getStreamId() {
        return streamId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getSegmentPath() {
        return segmentPath;
    }
    
    public long getDuration() {
        return duration;
    }
    
    public ByteBuffer getData() {
        return data;
    }
    
    public long getOffset() {
        return offset;
    }
}
