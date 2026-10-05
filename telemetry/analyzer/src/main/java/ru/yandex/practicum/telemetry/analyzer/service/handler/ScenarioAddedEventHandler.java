package ru.yandex.practicum.telemetry.analyzer.service.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.ActionType;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioAddedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();
        String hubId = event.getHubId();

        Scenario scenario = scenarioRepository.findByHubIdAndName(hubId, payload.getName())
                .orElseGet(() -> scenarioRepository.save(
                        Scenario.builder().hubId(hubId).name(payload.getName()).build()));

        // если сценарий уже существовал - удаляем старые условия/действия и пересоздаём их заново
        clearScenarioLinks(scenario.getId());

        for (ScenarioConditionAvro conditionAvro : payload.getConditions()) {
            String sensorId = conditionAvro.getSensorId();
            if (sensorRepository.findByIdAndHubId(sensorId, hubId).isEmpty()) {
                log.warn("Датчик {} не зарегистрирован в хабе {}, условие сценария '{}' пропущено",
                        sensorId, hubId, payload.getName());
                continue;
            }

            Condition condition = conditionRepository.save(Condition.builder()
                    .type(ConditionType.valueOf(conditionAvro.getType().name()))
                    .operation(ConditionOperation.valueOf(conditionAvro.getOperation().name()))
                    .value(extractIntValue(conditionAvro.getValue()))
                    .build());

            scenarioConditionRepository.save(ScenarioCondition.builder()
                    .scenarioId(scenario.getId())
                    .sensorId(sensorId)
                    .conditionId(condition.getId())
                    .build());
        }

        for (DeviceActionAvro actionAvro : payload.getActions()) {
            String sensorId = actionAvro.getSensorId();
            if (sensorRepository.findByIdAndHubId(sensorId, hubId).isEmpty()) {
                log.warn("Датчик {} не зарегистрирован в хабе {}, действие сценария '{}' пропущено",
                        sensorId, hubId, payload.getName());
                continue;
            }

            Action action = actionRepository.save(Action.builder()
                    .type(ActionType.valueOf(actionAvro.getType().name()))
                    .value(extractIntValue(actionAvro.getValue()))
                    .build());

            scenarioActionRepository.save(ScenarioAction.builder()
                    .scenarioId(scenario.getId())
                    .sensorId(sensorId)
                    .actionId(action.getId())
                    .build());
        }

        log.info("Сценарий '{}' хаба {} сохранён", payload.getName(), hubId);
    }

    private void clearScenarioLinks(Long scenarioId) {
        List<ScenarioCondition> oldConditions = scenarioConditionRepository.findByScenarioId(scenarioId);
        List<ScenarioAction> oldActions = scenarioActionRepository.findByScenarioId(scenarioId);

        scenarioConditionRepository.deleteByScenarioId(scenarioId);
        scenarioActionRepository.deleteByScenarioId(scenarioId);

        oldConditions.forEach(sc -> conditionRepository.deleteById(sc.getConditionId()));
        oldActions.forEach(sa -> actionRepository.deleteById(sa.getActionId()));
    }

    private Integer extractIntValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean bool) {
            return bool ? 1 : 0;
        }
        return ((Number) value).intValue();
    }
}
