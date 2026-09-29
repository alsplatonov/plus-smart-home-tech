package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;

//Асинхронная отправка Avro-событий в Kafka.
//Ключ сообщения — идентификатор хаба: события одного хаба попадают в одну партицию и сохраняют порядок

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

    public void sendSensorEvent(String hubId, Instant timestamp, SpecificRecordBase value) {
        send(sensorsTopic, hubId, timestamp, value);
    }

    public void sendHubEvent(String hubId, Instant timestamp, SpecificRecordBase value) {
        send(hubsTopic, hubId, timestamp, value);
    }

    private void send(String topic, String key, Instant timestamp, SpecificRecordBase value) {
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие в топик {}", topic, exception);
            }
        });
    }
}
