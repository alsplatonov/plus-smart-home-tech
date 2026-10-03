package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.BaseHubEventHandler;

@Component
public class DeviceRemovedEventHandler extends BaseHubEventHandler {

    public DeviceRemovedEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvroPayload(HubEventProto event) {
        DeviceRemovedEventProto proto = event.getDeviceRemoved();
        return DeviceRemovedEventAvro.newBuilder()
                .setId(proto.getId())
                .build();
    }
}
