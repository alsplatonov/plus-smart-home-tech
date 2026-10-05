package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Обрабатывает снэпшоты показаний датчиков (топик telemetry.snapshots.v1):
 * для каждого снэпшота проверяет все сценарии хаба и, если условия сценария
 * выполнены, отправляет связанные с ним действия в HubRouter.
 * Запускается в основном потоке приложения через {@link #start()} (см. {@code Analyzer.main}).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final KafkaConfig kafkaConfig;
    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final HubRouterClient hubRouterClient;

    public void start() {
        try {
            consumer.subscribe(List.of(kafkaConfig.getTopics().getSnapshots()));

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        handleSnapshot(record.value());
                    } catch (Exception e) {
                        log.error("Ошибка обработки снэпшота хаба {}", record.value().getHubId(), e);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки снэпшотов", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер снэпшотов");
                consumer.close();
            }
        }
    }

    private void handleSnapshot(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        for (Scenario scenario : scenarios) {
            if (isSatisfied(scenario, sensorsState)) {
                executeActions(scenario, hubId);
            }
        }
    }

    private boolean isSatisfied(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        List<ScenarioCondition> links = scenarioConditionRepository.findByScenarioId(scenario.getId());
        if (links.isEmpty()) {
            return false;
        }

        for (ScenarioCondition link : links) {
            SensorStateAvro state = sensorsState.get(link.getSensorId());
            if (state == null) {
                return false;
            }

            Condition condition = conditionRepository.findById(link.getConditionId()).orElse(null);
            if (condition == null) {
                return false;
            }

            SpecificRecordBase data = (SpecificRecordBase) state.getData();
            Integer actualValue = SensorValueExtractor.extract(condition.getType(), data);
            if (actualValue == null || !matches(condition.getOperation(), actualValue, condition.getValue())) {
                return false;
            }
        }

        return true;
    }

    private boolean matches(ConditionOperation operation, int actualValue, Integer conditionValue) {
        if (conditionValue == null) {
            return false;
        }
        return switch (operation) {
            case EQUALS -> actualValue == conditionValue;
            case GREATER_THAN -> actualValue > conditionValue;
            case LOWER_THAN -> actualValue < conditionValue;
        };
    }

    private void executeActions(Scenario scenario, String hubId) {
        List<ScenarioAction> links = scenarioActionRepository.findByScenarioId(scenario.getId());
        for (ScenarioAction link : links) {
            Action action = actionRepository.findById(link.getActionId()).orElse(null);
            if (action == null) {
                continue;
            }
            hubRouterClient.sendAction(hubId, scenario.getName(), link.getSensorId(), action);
        }
    }
}
