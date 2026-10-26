package ru.yandex.practicum.order.client;

import feign.FeignException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;

/**
 Резервная реализация ProductClient. Бизнес-отказ (товар не найден)
 пропускается как есть — это не техническая деградация. Любая другая причина
 (таймаут, 5xx, Circuit Breaker в состоянии OPEN) оборачивается в
 ProductServiceUnavailableException
 */
@Component
public class ProductClientFallbackFactory implements FallbackFactory<ProductClient> {

    private static final Logger log = LoggerFactory.getLogger(ProductClientFallbackFactory.class);

    @Override
    public ProductClient create(Throwable cause) {
        return productId -> {
            if (cause instanceof FeignException.NotFound notFound) {
                throw notFound;
            }
            log.warn("product-service недоступен при запросе товара id={}", productId, cause);
            throw new ProductServiceUnavailableException(productId, cause);
        };
    }
}
