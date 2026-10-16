package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryItem;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.mapper.InventoryMapper;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final InventoryMapper inventoryMapper;

    @Transactional
    public InventoryDto createInventory(UpdateInventoryRequest request) {
        if (inventoryRepository.findByProductId(request.productId()).isPresent()) {
            throw new IllegalArgumentException(
                    "Складская запись для productId=" + request.productId() + " уже существует");
        }
        InventoryItem item = InventoryItem.builder()
                .productId(request.productId())
                .quantity(request.quantity())
                .reservedQuantity(0)
                .build();
        InventoryItem saved = inventoryRepository.save(item);
        return inventoryMapper.toDto(saved);
    }

    @Transactional
    public InventoryDto updateInventory(UpdateInventoryRequest request) {
        InventoryItem item = findByProductIdOrThrow(request.productId());
        item.setQuantity(request.quantity());
        InventoryItem saved = inventoryRepository.save(item);
        return inventoryMapper.toDto(saved);
    }

    @Transactional(readOnly = true)
    public InventoryDto getByProductId(Long productId) {
        return inventoryMapper.toDto(findByProductIdOrThrow(productId));
    }

    @Transactional(readOnly = true)
    public List<InventoryDto> getAllInventory() {
        return inventoryRepository.findAll().stream()
                .map(inventoryMapper::toDto)
                .toList();
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        InventoryItem item = findByProductIdOrThrow(request.productId());

        int available = item.getAvailableQuantity();
        if (available < request.quantity()) {
            throw new InsufficientStockException(
                    "Недостаточно товара на складе: доступно " + available + ", запрошено " + request.quantity());
        }

        item.setReservedQuantity(item.getReservedQuantity() + request.quantity());
        InventoryItem saved = inventoryRepository.save(item);

        return new ReserveResponse(true, saved.getAvailableQuantity(), "Резервирование выполнено успешно");
    }

    @Transactional
    public ReserveResponse release(ReserveRequest request) {
        InventoryItem item = findByProductIdOrThrow(request.productId());

        if (item.getReservedQuantity() < request.quantity()) {
            throw new IllegalArgumentException(
                    "Невозможно снять резерв: зарезервировано " + item.getReservedQuantity()
                            + ", запрошено к снятию " + request.quantity());
        }

        item.setReservedQuantity(item.getReservedQuantity() - request.quantity());
        InventoryItem saved = inventoryRepository.save(item);

        return new ReserveResponse(true, saved.getAvailableQuantity(), "Резерв успешно снят");
    }

    private InventoryItem findByProductIdOrThrow(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException("Складская запись для productId=" + productId + " не найдена"));
    }
}
