package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.telemetry.analyzer.model.Action;

import java.time.Instant;

/**
 * Клиент для отправки запросов на выполнение действий сценария в HubRouter по gRPC.
 */
@Slf4j
@Service
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterStub;

    public void sendAction(String hubId, String scenarioName, String sensorId, Action action) {
        DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                .setSensorId(sensorId)
                .setType(ActionTypeProto.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            actionBuilder.setValue(action.getValue());
        }

        Instant now = Instant.now();
        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(actionBuilder.build())
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();

        try {
            hubRouterStub.handleDeviceAction(request);
            log.info("Отправлено действие {} по датчику {} (сценарий '{}', хаб {})",
                    action.getType(), sensorId, scenarioName, hubId);
        } catch (Exception e) {
            log.error("Не удалось отправить действие {} по датчику {} (сценарий '{}', хаб {})",
                    action.getType(), sensorId, scenarioName, hubId, e);
        }
    }
}
