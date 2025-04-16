package com.exoquic.streaming.service;

import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Additional utility to add logging for better tracking
 */
@Component
public class StreamingMetrics {
    private long segmentsReceived = 0;
    private long segmentsServed = 0;
    private long bytesReceived = 0;
    private long bytesServed = 0;
    
    public void trackSegmentReceived(ByteBuffer data) {
        segmentsReceived++;
        bytesReceived += data.remaining();
    }
    
    public void trackSegmentServed(ByteBuffer data) {
        segmentsServed++;
        bytesServed += data.remaining();
    }
    
    public Map<String, Long> getMetrics() {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("segmentsReceived", segmentsReceived);
        metrics.put("segmentsServed", segmentsServed);
        metrics.put("bytesReceived", bytesReceived);
        metrics.put("bytesServed", bytesServed);
        return metrics;
    }
}
