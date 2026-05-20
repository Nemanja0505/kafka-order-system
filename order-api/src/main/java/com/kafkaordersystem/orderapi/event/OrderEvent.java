package com.kafkaordersystem.orderapi.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Wire contract for events published to the {@code orders} Kafka topic.
 *
 * <p>inventory-service keeps its own copy of this record and deserializes
 * messages into that type, so any field change here must be mirrored there.
 */
public record OrderEvent(
        String orderId,
        String itemId,
        int quantity,
        Instant timestamp,
        UUID correlationId
) {
}
