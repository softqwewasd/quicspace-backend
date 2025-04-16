package com.exoquic.streaming.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.nio.ByteBuffer;
import java.util.Properties;

@Configuration
public class KafkaConfig {
    
    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${app.kafka.consumer-group}")
    private String consumerGroup;
    
    @Bean
    public KafkaSender<String, ByteBuffer> kafkaSender() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "hls-segment-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        // Use larger buffer sizes for better throughput with video segments
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072); // 128KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // 5ms batch delay
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Use LZ4 compression which is fast
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
        
        SenderOptions<String, ByteBuffer> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }
    
    @Bean
    public ReceiverOptions<String, ByteBuffer> kafkaReceiverOptions() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "hls-segment-consumer-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        // Set fetch sizes appropriately for video segments
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536); // 64KB
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800); // 50MB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760); // 10MB
        
        return ReceiverOptions.<String, ByteBuffer>create(props);
    }
}
