package com.kafkaordersystem.inventoryservice.model;

public record InventoryView(
        String itemId,
        int availableStock
) {
}
