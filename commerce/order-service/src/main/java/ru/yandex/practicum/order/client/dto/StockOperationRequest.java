package ru.yandex.practicum.order.client.dto;

public record StockOperationRequest(

        Long productId,

        Integer quantity
) {
}
