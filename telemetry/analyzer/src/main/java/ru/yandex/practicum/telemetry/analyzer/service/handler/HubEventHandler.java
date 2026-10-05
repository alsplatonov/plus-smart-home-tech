package ru.yandex.practicum.telemetry.analyzer.service.handler;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {

    Class<? extends SpecificRecordBase> getPayloadType();

    void handle(HubEventAvro event);
}
