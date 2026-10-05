package ru.yandex.practicum.telemetry.analyzer.service.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;

    @Override
    public Class<? extends SpecificRecordBase> getPayloadType() {
        return ScenarioRemovedEventAvro.class;
    }

    @Override
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioRemovedEventAvro payload = (ScenarioRemovedEventAvro) event.getPayload();

        scenarioRepository.findByHubIdAndName(event.getHubId(), payload.getName()).ifPresentOrElse(scenario -> {
            List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenarioId(scenario.getId());
            List<ScenarioAction> actions = scenarioActionRepository.findByScenarioId(scenario.getId());

            scenarioConditionRepository.deleteByScenarioId(scenario.getId());
            scenarioActionRepository.deleteByScenarioId(scenario.getId());

            conditions.forEach(sc -> conditionRepository.deleteById(sc.getConditionId()));
            actions.forEach(sa -> actionRepository.deleteById(sa.getActionId()));

            scenarioRepository.delete(scenario);
            log.info("Сценарий '{}' хаба {} удалён", payload.getName(), event.getHubId());
        }, () -> log.debug("Сценарий '{}' хаба {} уже отсутствует, игнорируем событие",
                payload.getName(), event.getHubId()));
    }
}
