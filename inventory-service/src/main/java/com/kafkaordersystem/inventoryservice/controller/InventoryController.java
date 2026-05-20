package com.kafkaordersystem.inventoryservice.controller;

import com.kafkaordersystem.inventoryservice.model.InventoryView;
import com.kafkaordersystem.inventoryservice.model.OrderResult;
import com.kafkaordersystem.inventoryservice.store.InventoryStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryStore inventory;

    @GetMapping("/inventory/{itemId}")
    public ResponseEntity<InventoryView> getInventory(@PathVariable String itemId) {
        if (!inventory.contains(itemId)) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new InventoryView(itemId, inventory.getStock(itemId)));
    }

    /**
     * Returns the processing outcome for an order. Responds {@code 404} until the
     * consumer has processed the order, so a recently accepted order (HTTP 202
     * from order-api) may not have a result yet.
     */
    @GetMapping("/orders/{orderId}/status")
    public ResponseEntity<OrderResult> getOrderStatus(@PathVariable String orderId) {
        return inventory.findOrderResult(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
