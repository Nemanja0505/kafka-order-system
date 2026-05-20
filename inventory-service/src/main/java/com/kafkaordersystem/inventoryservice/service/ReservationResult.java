package com.kafkaordersystem.inventoryservice.service;

import java.util.UUID;

/**
 * Result of processing one order event, returned by {@link ReservationService#process}
 * for every outcome the consumer can acknowledge (approved, rejected, duplicate).
 */
public record ReservationResult(
        ReservationStatus status,
        String reason,
        String orderId,
        UUID correlationId
) {
}
