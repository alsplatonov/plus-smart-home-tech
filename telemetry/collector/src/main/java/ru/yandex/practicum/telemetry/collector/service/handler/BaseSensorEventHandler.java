package ru.yandex.practicum.telemetry.collector.service.handler;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import java.time.Instant;

public abstract class BaseSensorEventHandler implements SensorEventHandler {

    private final KafkaEventProducer producer;

    protected BaseSensorEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract SpecificRecordBase mapToAvroPayload(SensorEventProto event);

    @Override
    public void handle(SensorEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException("Неизвестный тип события датчика: " + event.getPayloadCase());
        }

        SensorEventAvro payload = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos()))
                .setPayload(mapToAvroPayload(event))
                .build();

        producer.sendSensorEvent(payload);
    }
}
