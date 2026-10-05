package ru.yandex.practicum.telemetry.collector.configuration;

import jakarta.annotation.PreDestroy;
import ru.yandex.practicum.telemetry.collector.utils.AvroSerializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Properties;

@Configuration
public class KafkaConfig {
    private Producer<String, SpecificRecordBase> producer;

    @Bean
    public Producer<String, SpecificRecordBase> kafkaProducer(
            @Value("${collector.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
        this.producer = new KafkaProducer<>(properties);
        return this.producer;
    }

    @PreDestroy
    public void shutdown() {
        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        }
    }
}
