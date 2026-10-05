package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioActionId;

import java.util.List;

public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    List<ScenarioAction> findByScenarioId(Long scenarioId);

    void deleteByScenarioId(Long scenarioId);

    void deleteBySensorId(String sensorId);
}
