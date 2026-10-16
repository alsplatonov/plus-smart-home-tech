package ru.yandex.practicum.order.client.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StockOperationResponse(

        boolean success,

        Integer availableQuantity,

        String message
) {
}
