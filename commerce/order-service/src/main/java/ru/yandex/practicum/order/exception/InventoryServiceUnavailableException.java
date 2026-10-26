package ru.yandex.practicum.order.exception;

/**
 * Техническая недоступность inventory-service (таймаут, 5xx, Circuit
 * Breaker в состоянии OPEN). В отличие от бизнес-отказов (нет складской записи, недостаточно
 * остатков), эта ошибка означает, что сейчас нельзя подтвердить ни
 * резервирование, ни снятие резерва.
 */
public class InventoryServiceUnavailableException extends RuntimeException {

    private final Long productId;

    public InventoryServiceUnavailableException(Long productId, Throwable cause) {
        super("inventory-service недоступен для товара id=" + productId, cause);
        this.productId = productId;
    }

    public Long getProductId() {
        return productId;
    }
}
