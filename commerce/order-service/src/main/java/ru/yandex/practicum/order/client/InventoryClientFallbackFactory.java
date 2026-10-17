package ru.yandex.practicum.order.client;

import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.client.dto.StockOperationRequest;
import ru.yandex.practicum.order.client.dto.StockOperationResponse;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;

/**
 Резервная реализация InventoryClient. Бизнес-отказы (нет складской записи,
 недостаточно остатков) пропускаются как есть. Остальные причины (таймаут,
 5xx, открытый Circuit Breaker) оборачиваются в
 InventoryServiceUnavailableException.
 */
@Component
public class InventoryClientFallbackFactory implements FallbackFactory<InventoryClient> {

    private static final Logger log = LoggerFactory.getLogger(InventoryClientFallbackFactory.class);

    @Override
    public InventoryClient create(Throwable cause) {
        return new InventoryClient() {

            @Override
            public StockOperationResponse reserve(StockOperationRequest request) {
                if (cause instanceof FeignException.NotFound notFound) {
                    throw notFound;
                }
                if (cause instanceof FeignException.Conflict conflict) {
                    throw conflict;
                }
                log.warn("inventory-service недоступен при резервировании товара id={}", request.productId(), cause);
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }

            @Override
            public StockOperationResponse release(StockOperationRequest request) {
                log.warn("inventory-service недоступен при снятии резерва товара id={}", request.productId(), cause);
                throw new InventoryServiceUnavailableException(request.productId(), cause);
            }
        };
    }
}
