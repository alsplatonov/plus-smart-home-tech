package ru.yandex.practicum.telemetry.analyzer.service.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return DeviceRemovedEventAvro.class;
    }

    @Override
    public void handle(HubEventAvro event) {
        DeviceRemovedEventAvro payload = (DeviceRemovedEventAvro) event.getPayload();

        if (sensorRepository.findByIdAndHubId(payload.getId(), event.getHubId()).isEmpty()) {
            log.debug("Датчик {} уже удалён из хаба {}, игнорируем событие", payload.getId(), event.getHubId());
            return;
        }

        // удаляем связи сценариев, ссылающиеся на удаляемый датчик,
        // чтобы не нарушить ограничения внешних ключей
        scenarioConditionRepository.deleteBySensorId(payload.getId());
        scenarioActionRepository.deleteBySensorId(payload.getId());

        sensorRepository.deleteByIdAndHubId(payload.getId(), event.getHubId());
        log.info("Удалён датчик {} из хаба {}", payload.getId(), event.getHubId());
    }
}
