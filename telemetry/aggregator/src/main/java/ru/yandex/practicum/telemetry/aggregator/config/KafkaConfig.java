package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaConfig {

    private String bootstrapServers;
    private String consumerGroupId;
    private Topics topics;

    @Getter
    @Setter
    public static class Topics {
        private String sensors;
        private String snapshots;
    }
}
