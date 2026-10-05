package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;
import ru.yandex.practicum.telemetry.analyzer.service.handler.HubEventHandler;

import java.time.Duration;
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
                            continue;
                        }
                        handler.handle(event);
                    } catch (Exception e) {
                        log.error("Ошибка обработки события хаба {}", event.getHubId(), e);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            // игнорируем - закрываем консьюмер в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер событий хаба");
                consumer.close();
            }
        }
    }
}
