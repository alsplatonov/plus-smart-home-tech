package ru.yandex.practicum.telemetry.collector.service.handler;

import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEventType;

public interface SensorEventHandler {
    SensorEventType getType();

    void handle(SensorEvent event);
}
