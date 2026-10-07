package ru.yandex.practicum.telemetry.analyzer.service.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return DeviceAddedEventAvro.class;
    }

    @Override
    public void handle(HubEventAvro event) {
        DeviceAddedEventAvro payload = (DeviceAddedEventAvro) event.getPayload();

        if (sensorRepository.existsById(payload.getId())) {
            log.debug("Датчик {} уже зарегистрирован, игнорируем повторное событие", payload.getId());
            return;
        }

        Sensor sensor = Sensor.builder()
                .id(payload.getId())
                .hubId(event.getHubId())
                .build();

        sensorRepository.save(sensor);
        log.info("Зарегистрирован датчик {} в хабе {}", payload.getId(), event.getHubId());
    }
}
