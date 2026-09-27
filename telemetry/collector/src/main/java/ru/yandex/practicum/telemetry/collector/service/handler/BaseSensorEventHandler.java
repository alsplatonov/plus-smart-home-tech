package ru.yandex.practicum.telemetry.collector.service.handler;

import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;

@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends SensorEvent> implements SensorEventHandler {
    private final KafkaEventProducer producer;

    protected abstract SpecificRecordBase toPayload(T event);

    @Override
    @SuppressWarnings("unchecked")
    public void handle(SensorEvent event) {
        T typed = (T) event;
        SensorEventAvro avro = SensorEventAvro.newBuilder()
                .setId(typed.getId())
                .setHubId(typed.getHubId())
                .setTimestamp(typed.getTimestamp())
                .setPayload(toPayload(typed))
                .build();
        producer.sendSensorEvent(typed.getHubId(), typed.getTimestamp(), avro);
    }
}
