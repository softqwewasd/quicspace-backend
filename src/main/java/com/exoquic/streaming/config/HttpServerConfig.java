package com.exoquic.streaming.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.exoquic.streaming.service.HlsKafkaService;

import io.netty.handler.codec.http.HttpHeaderNames;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * HTTP server to expose SRS hooks and HLS content
 */
@Component
public class HttpServerConfig {
    private final HlsKafkaService hlsKafkaService;
    private DisposableServer server;
    
    @Value("${app.http-server.port}")
    private int serverPort;
    
    public HttpServerConfig(HlsKafkaService hlsKafkaService) {
        this.hlsKafkaService = hlsKafkaService;
        
        // Start HTTP server
        startServer();
    }
    
    private void startServer() {
        server = HttpServer.create()
            .port(8153)
            .route(routes -> {
                // Add CORS support
                routes.options("/**", (request, response) -> {
                    response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                    response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE, OPTIONS");
                    response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, Authorization, X-Start-Time");
                    response.header(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, "3600");
                    return response.send().then();
                });
                
                // SRS Hook endpoints
                routes.post("/api/stream/start", (request, response) -> 
                    request.receive()
                        .aggregate()
                        .asString(StandardCharsets.UTF_8)
                        .map(this::parseJsonData)
                        .flatMap(hlsKafkaService::handleStreamStart)
                        .then(Mono.defer(() -> {
                            addCorsHeaders(response);
                            response.header(HttpHeaderNames.CONTENT_TYPE, "application/json");
                            return response.sendString(Mono.just("{\"code\":0}")).then();
                        }))
                );
                
                routes.post("/api/stream/end", (request, response) -> 
                    request.receive()
                        .aggregate()
                        .asString(StandardCharsets.UTF_8)
                        .map(this::parseJsonData)
                        .flatMap(hlsKafkaService::handleStreamEnd)
                        .then(Mono.defer(() -> {
                            addCorsHeaders(response);
                            response.header(HttpHeaderNames.CONTENT_TYPE, "application/json");
                            return response.sendString(Mono.just("{\"code\":0}")).then();
                        }))
                );
                
                routes.post("/api/stream/hls", (request, response) -> 
                    request.receive()
                        .aggregate()
                        .asString(StandardCharsets.UTF_8)
                        .map(this::parseJsonData)
                        .flatMap(hlsKafkaService::handleHlsSegment)
                        .then(Mono.defer(() -> {
                            addCorsHeaders(response);
                            response.header(HttpHeaderNames.CONTENT_TYPE, "application/json");
                            return response.sendString(Mono.just("{\"code\":0}")).then();
                        }))
                );
                
                routes.post("/api/stream/hls/notify", (request, response) -> 
                    request.receive()
                        .aggregate()
                        .asString(StandardCharsets.UTF_8)
                        .map(this::parseJsonData)
                        .flatMap(hlsKafkaService::handleHlsNotify)
                        .then(Mono.defer(() -> {
                            addCorsHeaders(response);
                            response.header(HttpHeaderNames.CONTENT_TYPE, "application/json");
                            return response.sendString(Mono.just("{\"code\":0}")).then();
                        }))
                );
                
                // HLS endpoints
                routes.get("/hls/{streamId}.m3u8", (request, response) -> {
                    String streamId = request.param("streamId");
                    String startTimeStr = request.requestHeaders().get("X-Start-Time");
                    Long startTime = startTimeStr != null ? Long.parseLong(startTimeStr) : null;
                    
                    addCorsHeaders(response);
                    return response
                        .header(HttpHeaderNames.CONTENT_TYPE, "application/vnd.apple.mpegurl")
                        .header(HttpHeaderNames.CACHE_CONTROL, "no-cache")
                        .sendString(hlsKafkaService.generatePlaylist(streamId, startTime));
                });
                
                routes.get("/hls/{streamId}/segment/{timestamp}", (request, response) -> {
                    String streamId = request.param("streamId");
                    
                    try {
                        long timestamp = Long.parseLong(request.param("timestamp"));
                        
                        return hlsKafkaService.getSegmentData(streamId, timestamp)
                            .flatMap(buffer -> {
                                addCorsHeaders(response);
                                response.header(HttpHeaderNames.CONTENT_TYPE, "video/MP2T");
                                response.header(HttpHeaderNames.CACHE_CONTROL, "public, max-age=31536000");
                                return Mono.defer(() -> 
                                    response.send(Mono.just(Unpooled.wrappedBuffer(buffer))).then()
                                );
                            })
                            .switchIfEmpty(Mono.defer(() -> {
                                addCorsHeaders(response);
                                return response.status(404).sendString(Mono.just("Segment not found")).then();
                            }));
                    } catch (NumberFormatException e) {
                        addCorsHeaders(response);
                        return response.status(400).sendString(Mono.just("Invalid timestamp format"));
                    }
                });
            })
            .bindNow();
        
        System.out.println("HTTP server started on port " + server.port());
    }
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Helper method to add CORS headers to responses
     */
    private void addCorsHeaders(reactor.netty.http.server.HttpServerResponse response) {
        response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE, OPTIONS");
        response.header(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, Authorization, X-Start-Time");
    }
    
    /**
     * Parse JSON data from string using Jackson
     */
    private Map<String, Object> parseJsonData(String json) {
        Map<String, Object> data = new HashMap<>();
        if (json != null && !json.isEmpty()) {
            try {
                data = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
            }
        }
        return data;
    }
}
