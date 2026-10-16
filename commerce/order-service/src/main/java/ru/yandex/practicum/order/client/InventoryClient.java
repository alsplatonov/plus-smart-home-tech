package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.order.client.dto.StockOperationRequest;
import ru.yandex.practicum.order.client.dto.StockOperationResponse;

@FeignClient(name = "inventory-service")
public interface InventoryClient {

    @PostMapping("/api/inventory/reserve")
    StockOperationResponse reserve(@RequestBody StockOperationRequest request);

    @PostMapping("/api/inventory/release")
    StockOperationResponse release(@RequestBody StockOperationRequest request);
}
