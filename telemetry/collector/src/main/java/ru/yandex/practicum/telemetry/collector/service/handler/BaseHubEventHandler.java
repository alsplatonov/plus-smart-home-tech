package ru.yandex.practicum.telemetry.collector.service.handler;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import java.time.Instant;

public abstract class BaseHubEventHandler implements HubEventHandler {

    private final KafkaEventProducer producer;

    protected BaseHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    protected abstract SpecificRecordBase mapToAvroPayload(HubEventProto event);

    @Override
    public void handle(HubEventProto event) {
        if (event.getPayloadCase() != getMessageType()) {
            throw new IllegalArgumentException("Неизвестный тип события хаба: " + event.getPayloadCase());
        }

        HubEventAvro payload = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos()))
                .setPayload(mapToAvroPayload(event))
                .build();

        producer.sendHubEvent(payload);
    }
}
