package com.kafkaordersystem.inventoryservice.service;

/**
 * Outcome of {@link ReservationService#process}.
 *
 * <p>{@code APPROVED}, {@code REJECTED} and {@code DUPLICATE} are returned to the
 * caller — they are normal outcomes the consumer can acknowledge. {@code INVALID}
 * marks an event that cannot be processed at all (malformed payload, invalid
 * quantity, unknown item); the service surfaces those by throwing
 * {@link IllegalArgumentException} so the Kafka error handler routes them to the
 * dead-letter topic instead of acknowledging them.
 */
public enum ReservationStatus {
    APPROVED,
    REJECTED,
    DUPLICATE,
    INVALID
}
