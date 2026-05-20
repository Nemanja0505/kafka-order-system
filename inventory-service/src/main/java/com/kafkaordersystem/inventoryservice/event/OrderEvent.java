package com.kafkaordersystem.inventoryservice.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Inventory-service's own copy of the order event contract.
 *
 * <p>The consumer deserializes the {@code orders} topic into this local type
 * ({@code spring.json.use.type.headers: false}), so it is intentionally independent
 * of order-api's {@code OrderEvent}. Field names must stay in sync with the producer.
 */
public record OrderEvent(
        String orderId,
        String itemId,
        int quantity,
        Instant timestamp,
        UUID correlationId
) {
}
