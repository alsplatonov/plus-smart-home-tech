package ru.yandex.practicum.telemetry.collector.service.handler.sensor;

import ru.yandex.practicum.telemetry.collector.model.TemperatureSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.BaseSensorEventHandler;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

@Component
public class TemperatureSensorEventHandler extends BaseSensorEventHandler<TemperatureSensorEvent> {

    public TemperatureSensorEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    protected SpecificRecordBase toPayload(TemperatureSensorEvent e) {
        return TemperatureSensorAvro.newBuilder()
                .setId(e.getId())
                .setHubId(e.getHubId())
                .setTimestamp(e.getTimestamp())
                .setTemperatureC(e.getTemperatureC())
                .setTemperatureF(e.getTemperatureF())
                .build();
    }
}
