package ru.yandex.practicum.order.exception;

/**
 * Ожидаемая ошибка сценария оформления заказа (товар не найден, товар снят
 * с продажи, недостаточно остатков и т.п.).
 */
public class OrderProcessingException extends RuntimeException {

    public OrderProcessingException(String message) {
        super(message);
    }
}
