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
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String STATUS_CONFIRMED = "CONFIRMED";

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final ProductClient productClient;
    private final InventoryClient inventoryClient;

    /**
     * Сценарий оформления заказа: получить товарные данные из product-service,
     * зарезервировать остатки в inventory-service и только затем сохранить заказ.
     * Удалённые вызовы выполняются ДО открытия DB-транзакции сохранения заказа.
     */
    public OrderDto createOrder(CreateOrderRequest request) {
        Map<Long, Integer> quantityByProduct = aggregateQuantityByProduct(request.items());

        Map<Long, ProductResponse> productsById = new LinkedHashMap<>();
        for (Long productId : quantityByProduct.keySet()) {
            productsById.put(productId, fetchActiveProduct(productId));
        }

        List<Long> reservedProductIds = new ArrayList<>();
        try {
            for (Map.Entry<Long, Integer> entry : quantityByProduct.entrySet()) {
                reserveStock(entry.getKey(), entry.getValue());
                reservedProductIds.add(entry.getKey());
            }
        } catch (RuntimeException ex) {
            releaseReservations(reservedProductIds, quantityByProduct);
            throw ex;
        }

        try {
            return persistOrder(request, productsById);
        } catch (RuntimeException ex) {
            log.error("Не удалось сохранить заказ после успешного резервирования, снимаем резервы", ex);
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

    private ProductResponse fetchActiveProduct(Long productId) {
        ProductResponse product;
        try {
            product = productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Товар с id=" + productId + " не найден");
        } catch (FeignException e) {
            log.error("Ошибка при обращении к product-service за товаром id={}", productId, e);
            throw new OrderProcessingException("Не удалось получить данные о товаре, попробуйте повторить попытку позже");
        }

        if (product.active() == null || !product.active()) {
            throw new OrderProcessingException("Товар с id=" + productId + " снят с продажи");
        }
        return product;
    }

    private void reserveStock(Long productId, Integer quantity) {
        try {
            inventoryClient.reserve(new StockOperationRequest(productId, quantity));
        } catch (FeignException.NotFound e) {
            throw new OrderProcessingException("Складская запись для товара с id=" + productId + " не найдена");
        } catch (FeignException.Conflict e) {
            throw new OrderProcessingException("Товара с id=" + productId + " недостаточно на складе");
        } catch (FeignException e) {
            log.error("Ошибка при обращении к inventory-service для резервирования товара id={}", productId, e);
            throw new OrderProcessingException("Не удалось зарезервировать товар, попробуйте повторить попытку позже");
        }
    }

    private void releaseReservations(List<Long> reservedProductIds, Map<Long, Integer> quantityByProduct) {
        for (Long productId : reservedProductIds) {
            try {
                inventoryClient.release(new StockOperationRequest(productId, quantityByProduct.get(productId)));
            } catch (FeignException e) {
                // Снятие резерва — компенсация лучшего усилия: не подменяем исходную ошибку сценария.
                log.error("Не удалось снять резерв для товара с id={} в рамках компенсации", productId, e);
            }
        }
    }

    private OrderDto persistOrder(CreateOrderRequest request, Map<Long, ProductResponse> productsById) {
        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status(STATUS_CONFIRMED)
                .createdAt(LocalDateTime.now())
                .build();

        BigDecimal totalPrice = BigDecimal.ZERO;
        for (OrderItemRequest itemRequest : request.items()) {
            ProductResponse product = productsById.get(itemRequest.productId());
            OrderItem item = OrderItem.builder()
                    .order(order)
                    .productId(itemRequest.productId())
                    .productName(product.name())
                    .quantity(itemRequest.quantity())
                    .price(product.price())
                    .build();
            order.getItems().add(item);
            totalPrice = totalPrice.add(product.price().multiply(BigDecimal.valueOf(itemRequest.quantity())));
        }
        order.setTotalPrice(totalPrice);

        Order saved = orderRepository.save(order);
        return orderMapper.toDto(saved);
    }
}
