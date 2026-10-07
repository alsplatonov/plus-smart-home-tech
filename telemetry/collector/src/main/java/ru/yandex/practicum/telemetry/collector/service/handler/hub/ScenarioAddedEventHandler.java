package ru.yandex.practicum.telemetry.collector.service.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.BaseHubEventHandler;

import java.util.List;

@Component
public class ScenarioAddedEventHandler extends BaseHubEventHandler {

    public ScenarioAddedEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvroPayload(HubEventProto event) {
        ScenarioAddedEventProto proto = event.getScenarioAdded();

        List<ScenarioConditionAvro> conditions = proto.getConditionList().stream()
                .map(this::mapCondition)
                .toList();

        List<DeviceActionAvro> actions = proto.getActionList().stream()
                .map(this::mapAction)
                .toList();

        return ScenarioAddedEventAvro.newBuilder()
                .setName(proto.getName())
                .setConditions(conditions)
                .setActions(actions)
                .build();
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto proto) {
        Object value = switch (proto.getValueCase()) {
            case BOOL_VALUE -> proto.getBoolValue();
            case INT_VALUE -> proto.getIntValue();
            case VALUE_NOT_SET -> null;
        };

        return ScenarioConditionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(ConditionTypeAvro.valueOf(proto.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(proto.getOperation().name()))
                .setValue(value)
                .build();
    }

    private DeviceActionAvro mapAction(DeviceActionProto proto) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(proto.getSensorId())
                .setType(ActionTypeAvro.valueOf(proto.getType().name()))
                .setValue(proto.hasValue() ? proto.getValue() : null)
                .build();
    }
}
