package ru.yandex.practicum.telemetry.analyzer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.deserializer.HubEventDeserializer;
import ru.yandex.practicum.telemetry.analyzer.deserializer.SnapshotDeserializer;

import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer(KafkaConfig kafkaConfig) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getSnapshotConsumerGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SnapshotDeserializer.class);
        // повторная обработка снапшотов нежелательна - фиксируем смещения вручную после обработки
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer(KafkaConfig kafkaConfig) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getHubEventConsumerGroupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventDeserializer.class);
        // поток низкоинтенсивный, повторная обработка не критична - авто-коммит допустим
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        return new KafkaConsumer<>(properties);
    }
}
