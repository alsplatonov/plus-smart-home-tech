package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.client.dto.ProductResponse;
import ru.yandex.practicum.order.client.dto.StockOperationRequest;
import ru.yandex.practicum.order.client.dto.StockOperationResponse;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.InventoryServiceUnavailableException;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.exception.ProductServiceUnavailableException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;
import ru.yandex.practicum.order.service.result.ServiceCallResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String STATUS_CONFIRMED = "CONFIRMED";
    private static final String STATUS_PENDING_CONFIRMATION = "PENDING_CONFIRMATION";
    private static final String PENDING_STATUS_DETAILS =
            "Заказ принят, но требует ручной проверки: часть данных временно недоступна "
                    + "из-за технической недоступности соседних сервисов";

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final ProductClient productClient;
    private final InventoryClient inventoryClient;

    /**
     Сценарий оформления заказа: получить товарные данные из product-service,
     зарезервировать остатки в inventory-service и только затем сохранить заказ.
     Удалённые вызовы выполняются ДО открытия DB-транзакции сохранения заказа.
     Вызовы к соседним сервисам защищены Circuit Breaker (Resilience4j) и
     различают три исхода: успех, бизнес-отказ
     (останавливает сценарий — заказ не создаётся) и техническая деградация
     (заказ всё равно сохраняется, но в статусе PENDING_CONFIRMATION).
     */
    public OrderDto createOrder(CreateOrderRequest request) {
        Map<Long, Integer> quantityByProduct = aggregateQuantityByProduct(request.items());

        Map<Long, ProductResponse> productsById = new LinkedHashMap<>();
        Set<Long> degradedProductIds = new LinkedHashSet<>();
        for (Long productId : quantityByProduct.keySet()) {
            switch (fetchProduct(productId)) {
                case ServiceCallResult.Success<ProductResponse> success ->
                        productsById.put(productId, success.value());
                case ServiceCallResult.Failure<ProductResponse> failure ->
                        throw new OrderProcessingException(failure.message());
                case ServiceCallResult.Degraded<ProductResponse> degraded -> {
                    log.warn("Товар id={} недоступен для проверки: {}", productId, degraded.reason());
                    degradedProductIds.add(productId);
                }
            }
        }

        List<Long> reservedProductIds = new ArrayList<>();
        boolean inventoryDegraded = false;
        try {
            for (Map.Entry<Long, Integer> entry : quantityByProduct.entrySet()) {
                Long productId = entry.getKey();
                switch (reserveStock(productId, entry.getValue())) {
                    case ServiceCallResult.Success<StockOperationResponse> success ->
                            reservedProductIds.add(productId);
                    case ServiceCallResult.Failure<StockOperationResponse> failure ->
                            throw new OrderProcessingException(failure.message());
                    case ServiceCallResult.Degraded<StockOperationResponse> degraded -> {
                        log.warn("Резервирование товара id={} недоступно: {}", productId, degraded.reason());
                        inventoryDegraded = true;
                    }
                }
            }
        } catch (RuntimeException ex) {
            releaseReservations(reservedProductIds, quantityByProduct);
            throw ex;
        }

        boolean degraded = !degradedProductIds.isEmpty() || inventoryDegraded;
        try {
            return persistOrder(request, productsById, degradedProductIds, degraded);
        } catch (RuntimeException ex) {
            log.error("Не удалось сохранить заказ после резервирования, снимаем резервы", ex);
            releaseReservations(reservedProductIds, quantityByProduct);
            throw new OrderProcessingException("Не удалось сохранить заказ, попробуйте повторить попытку позже");
        }
    }

    @Transactional(readOnly = true)
    public OrderDto getOrderById(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
        return orderMapper.toDto(order);
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(orderMapper::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getOrdersByEmail(String email) {
        return orderRepository.findByCustomerEmail(email).stream()
                .map(orderMapper::toDto)
                .toList();
    }

    private Map<Long, Integer> aggregateQuantityByProduct(List<OrderItemRequest> items) {
        Map<Long, Integer> quantityByProduct = new LinkedHashMap<>();
        for (OrderItemRequest item : items) {
            quantityByProduct.merge(item.productId(), item.quantity(), Integer::sum);
        }
        return quantityByProduct;
    }

    /**
     Запрашивает товар в product-service. NotFound и active=false — бизнес-отказ,
     ProductServiceUnavailableException (брошенный fallback-фабрикой при
     таймауте, 5xx или открытом Circuit Breaker) — техническая деградация.
     */
    private ServiceCallResult<ProductResponse> fetchProduct(Long productId) {
        ProductResponse product;
        try {
            product = productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            return new ServiceCallResult.Failure<>("Товар с id=" + productId + " не найден");
        } catch (ProductServiceUnavailableException e) {
            return new ServiceCallResult.Degraded<>("Каталог временно недоступен");
        } catch (FeignException e) {
            log.error("Ошибка при обращении к product-service за товаром id={}", productId, e);
            return new ServiceCallResult.Degraded<>("Каталог временно недоступен");
        }

        if (product.active() == null || !product.active()) {
            return new ServiceCallResult.Failure<>("Товар с id=" + productId + " снят с продажи");
        }
        return new ServiceCallResult.Success<>(product);
    }

    /**
     Резервирует остатки в inventory-service. NotFound и Conflict (товара
     недостаточно) — бизнес-отказ, InventoryServiceUnavailableException —
     техническая деградация.
     */
    private ServiceCallResult<StockOperationResponse> reserveStock(Long productId, Integer quantity) {
        try {
            StockOperationResponse response =
                    inventoryClient.reserve(new StockOperationRequest(productId, quantity));
            return new ServiceCallResult.Success<>(response);
        } catch (FeignException.NotFound e) {
            return new ServiceCallResult.Failure<>("Складская запись для товара с id=" + productId + " не найдена");
        } catch (FeignException.Conflict e) {
            return new ServiceCallResult.Failure<>("Товара с id=" + productId + " недостаточно на складе");
        } catch (InventoryServiceUnavailableException e) {
            return new ServiceCallResult.Degraded<>("Склад временно недоступен");
        } catch (FeignException e) {
            log.error("Ошибка при обращении к inventory-service для резервирования товара id={}", productId, e);
            return new ServiceCallResult.Degraded<>("Склад временно недоступен");
        }
    }

    /**
     Снятие резерва — компенсация лучшего усилия: делаем одну попытку на
     каждый ранее зарезервированный товар и логируем неудачу, не подменяя
     исходную ошибку сценария. Склад может быть недоступен и в момент
     компенсации — в учебном проекте это сознательно не исправляется отдельным
     механизмом восстановления (повторные попытки, очередь, outbox).
     */
    private void releaseReservations(List<Long> reservedProductIds, Map<Long, Integer> quantityByProduct) {
        for (Long productId : reservedProductIds) {
            try {
                inventoryClient.release(new StockOperationRequest(productId, quantityByProduct.get(productId)));
            } catch (RuntimeException ex) {
                log.error("Не удалось снять резерв для товара с id={} в рамках компенсации", productId, ex);
            }
        }
    }
    
    private OrderDto persistOrder(
            CreateOrderRequest request,
            Map<Long, ProductResponse> productsById,
            Set<Long> degradedProductIds,
            boolean degraded
    ) {
        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status(degraded ? STATUS_PENDING_CONFIRMATION : STATUS_CONFIRMED)
                .statusDetails(degraded ? PENDING_STATUS_DETAILS : null)
                .createdAt(LocalDateTime.now())
                .build();

        BigDecimal totalPrice = BigDecimal.ZERO;
        for (OrderItemRequest itemRequest : request.items()) {
            Long productId = itemRequest.productId();
            OrderItem.OrderItemBuilder itemBuilder = OrderItem.builder()
                    .order(order)
                    .productId(productId)
                    .quantity(itemRequest.quantity());

            if (degradedProductIds.contains(productId)) {
                itemBuilder.productName("Товар #" + productId + " (ожидает проверки)")
                        .price(BigDecimal.ZERO);
            } else {
                ProductResponse product = productsById.get(productId);
                itemBuilder.productName(product.name())
                        .price(product.price());
                totalPrice = totalPrice.add(product.price().multiply(BigDecimal.valueOf(itemRequest.quantity())));
            }
            order.getItems().add(itemBuilder.build());
        }
        order.setTotalPrice(totalPrice);

        Order saved = orderRepository.save(order);
        return orderMapper.toDto(saved);
    }
}
