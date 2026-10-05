package ru.yandex.practicum.telemetry.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "analyzer.kafka")
public class KafkaConfig {

    private String bootstrapServers;
    private String snapshotConsumerGroupId;
    private String hubEventConsumerGroupId;
    private Topics topics;

    @Getter
    @Setter
    public static class Topics {
        private String snapshots;
        private String hubs;
    }
}
