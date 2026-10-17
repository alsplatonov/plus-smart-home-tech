package ru.yandex.practicum.order.service.result;

/**
 * Результат обращения к соседнему сервису (product-service, inventory-service)
 * в сценарии оформления заказа.
 */
public sealed interface ServiceCallResult<T> {

    record Success<T>(T value) implements ServiceCallResult<T> { }

    record Failure<T>(String message) implements ServiceCallResult<T> { }

    record Degraded<T>(String reason) implements ServiceCallResult<T> { }
}
