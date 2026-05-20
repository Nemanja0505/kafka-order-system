package com.kafkaordersystem.inventoryservice.model;

import java.time.Instant;
import java.util.UUID;

public record OrderResult(
        String orderId,
        String itemId,
        int quantity,
        OrderStatus status,
        String reason,
        Instant processedAt,
        UUID correlationId
) {
}
