package ru.yandex.practicum.order.exception;

/**
 * Техническая недоступность product-service (таймаут, 5xx, Circuit
 * Breaker в состоянии OPEN). В отличие от бизнес-отказов (товар не найден, товар снят с
 * продажи), эта ошибка не означает, что товар недоступен для заказа — просто
 * сейчас нельзя получить о нём данные.
 */
public class ProductServiceUnavailableException extends RuntimeException {

    private final Long productId;

    public ProductServiceUnavailableException(Long productId, Throwable cause) {
        super("product-service недоступен при запросе товара id=" + productId, cause);
        this.productId = productId;
    }

    public Long getProductId() {
        return productId;
    }
}
