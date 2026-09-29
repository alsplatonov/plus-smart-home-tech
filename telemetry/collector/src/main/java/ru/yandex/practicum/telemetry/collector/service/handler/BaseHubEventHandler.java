package ru.yandex.practicum.telemetry.collector.service.handler;

import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;

@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T extends HubEvent> implements HubEventHandler {
    private final KafkaEventProducer producer;

    protected abstract SpecificRecordBase toPayload(T event);

    @Override
    @SuppressWarnings("unchecked")
    public void handle(HubEvent event) {
        T typed = (T) event;
        HubEventAvro avro = HubEventAvro.newBuilder()
                .setHubId(typed.getHubId())
                .setTimestamp(typed.getTimestamp())
                .setPayload(toPayload(typed))
                .build();
        producer.sendHubEvent(typed.getHubId(), typed.getTimestamp(), avro);
    }
}
