package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Slf4j
@Service
public class KafkaEventProducer {

    private final Producer<String, SpecificRecordBase> producer;
    private final String sensorsTopic;
    private final String hubsTopic;

    public KafkaEventProducer(Producer<String, SpecificRecordBase> producer,
                               @Value("${collector.kafka.topics.sensors}") String sensorsTopic,
                               @Value("${collector.kafka.topics.hubs}") String hubsTopic) {
        this.producer = producer;
        this.sensorsTopic = sensorsTopic;
        this.hubsTopic = hubsTopic;
    }

    public void sendSensorEvent(SensorEventAvro event) {
        send(sensorsTopic, event.getHubId(), event);
    }

    public void sendHubEvent(HubEventAvro event) {
        send(hubsTopic, event.getHubId(), event);
    }

    private void send(String topic, String key, SpecificRecordBase value) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие в топик {}: {}", topic, exception.getMessage(), exception);
            } else {
                log.debug("Событие отправлено в топик {}, partition {}, offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
}
