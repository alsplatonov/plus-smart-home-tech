package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;
import ru.yandex.practicum.telemetry.analyzer.service.handler.HubEventHandler;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Обрабатывает события добавления/удаления устройств и сценариев хаба (топик telemetry.hubs.v1).
 * Запускается в отдельном потоке (см. {@code Analyzer.main}).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final KafkaConfig kafkaConfig;
    private final List<HubEventHandler> handlerList;

    // оффсеты успешно обработанных событий - коммитим только их, чтобы не подтвердить
    // событие, которое не удалось сохранить в БД (устройство/сценарий)
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    @Override
    public void run() {
        Map<Class<?>, HubEventHandler> handlers = handlerList.stream()
                .collect(Collectors.toMap(HubEventHandler::getPayloadType, Function.identity()));

        try {
            consumer.subscribe(List.of(kafkaConfig.getTopics().getHubs()));

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    try {
                        HubEventHandler handler = handlers.get(event.getPayload().getClass());
                        if (handler == null) {
                            log.warn("Не найден обработчик для события хаба {}: {}",
                                    event.getHubId(), event.getPayload().getClass());
                        } else {
                            handler.handle(event);
                        }

                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    } catch (Exception e) {
                        log.error("Ошибка обработки события хаба {}, offset записи {}-{} не будет зафиксирован",
                                event.getHubId(), record.topic(), record.offset(), e);
                        // не продолжаем обработку остатка батча - оффсет неподтверждённой записи
                        // и всех последующих в этом цикле poll() не попадёт в currentOffsets
                        break;
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
            // игнорируем - закрываем консьюмер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            try {
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
            } finally {
                log.info("Закрываем консьюмер событий хаба");
                consumer.close();
            }
        }
    }
}
