package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.KafkaConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Класс AggregationStarter, ответственный за запуск агрегации данных.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final Producer<String, SpecificRecordBase> producer;
    private final KafkaConfig kafkaConfig;

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    // оффсеты успешно обработанных записей - коммитим только их,
    // чтобы не подтвердить сообщение, snapshot которого не удалось записать в Kafka
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    /**
     * Метод для начала процесса агрегации данных.
     * Подписывается на топики для получения событий от датчиков,
     * формирует снимок их состояния и записывает в кафку.
     */
    public void start() {
        try {
            consumer.subscribe(List.of(kafkaConfig.getTopics().getSensors()));

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            poll:
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    try {
                        SensorEventAvro event = record.value();
                        log.debug("Получено событие датчика: {}", event);

                        Optional<SensorsSnapshotAvro> snapshotOpt = updateState(event);
                        if (snapshotOpt.isPresent()) {
                            SensorsSnapshotAvro snapshot = snapshotOpt.get();
                            ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(
                                    kafkaConfig.getTopics().getSnapshots(), snapshot.getHubId(), snapshot);
                            // ждём подтверждения записи в Kafka синхронно - иначе offset исходного
                            // sensor-event может быть зафиксирован раньше, чем snapshot реально доехал
                            producer.send(producerRecord).get();
                        }

                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    } catch (ExecutionException | InterruptedException e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        log.error("Не удалось записать снапшот в Kafka, offset записи {}-{} не будет зафиксирован. "
                                        + "Останавливаем consumer - при следующем запуске обработка продолжится "
                                        + "с последнего зафиксированного offset",
                                record.topic(), record.offset(), e);

                        break poll;
                    }
                }

                if (!currentOffsets.isEmpty()) {
                    consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                        if (exception != null) {
                            log.error("Не удалось асинхронно зафиксировать оффсеты {}", offsets, exception);
                        }
                    });
                }
            }

        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер и продюсер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {

            try {
                // Перед тем, как закрыть продюсер и консьюмер, нужно убедиться,
                // что все сообщения, лежащие в буффере, отправлены, и
                // зафиксировать оффсеты только успешно обработанных сообщений
                producer.flush();
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(event.getHubId(), hubId ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build());

        SensorStateAvro oldState = snapshot.getSensorsState().get(event.getId());
        if (oldState != null) {
            if (oldState.getTimestamp().isAfter(event.getTimestamp())
                    || oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(event.getId(), newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}
