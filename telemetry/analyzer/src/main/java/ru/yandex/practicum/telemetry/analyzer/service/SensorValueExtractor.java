package ru.yandex.practicum.telemetry.analyzer.service;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.analyzer.model.ConditionType;

/**
 * Достаёт из конкретного показания датчика (союз типов SensorStateAvro.data)
 * числовое значение, с которым можно сравнивать опорное значение условия сценария.
 */
public final class SensorValueExtractor {

    private SensorValueExtractor() {
    }

    public static Integer extract(ConditionType type, SpecificRecordBase data) {
        return switch (type) {
            case MOTION -> data instanceof MotionSensorAvro motion ? (motion.getMotion() ? 1 : 0) : null;
            case LUMINOSITY -> data instanceof LightSensorAvro light ? light.getLuminosity() : null;
            case SWITCH -> data instanceof SwitchSensorAvro state ? (state.getState() ? 1 : 0) : null;
            case TEMPERATURE -> extractTemperature(data);
            case CO2LEVEL -> data instanceof ClimateSensorAvro climate ? climate.getCo2Level() : null;
            case HUMIDITY -> data instanceof ClimateSensorAvro climate ? climate.getHumidity() : null;
        };
    }

    private static Integer extractTemperature(SpecificRecordBase data) {
        if (data instanceof ClimateSensorAvro climate) {
            return climate.getTemperatureC();
        }
        if (data instanceof TemperatureSensorAvro temperature) {
            return temperature.getTemperatureC();
        }
        return null;
    }
}
