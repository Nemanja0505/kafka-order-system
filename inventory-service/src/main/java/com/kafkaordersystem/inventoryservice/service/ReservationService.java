package com.kafkaordersystem.inventoryservice.service;

import java.time.Instant;

import com.kafkaordersystem.inventoryservice.event.OrderEvent;
import com.kafkaordersystem.inventoryservice.model.OrderResult;
import com.kafkaordersystem.inventoryservice.model.OrderStatus;
import com.kafkaordersystem.inventoryservice.store.InventoryStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Service layer for reserving stock against incoming order events.
 *
 * <p>The Kafka consumer ({@code OrderEventConsumer}) owns transport concerns only and
 * delegates here. This class owns the order-processing flow: validate the event,
 * claim its ID for idempotency, reserve stock, and persist the result.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReservationService {

    /**
     * Test-only item ID that simulates a transient (retryable) failure, used to
     * exercise the retry/dead-letter path. Checked before the idempotency claim so
     * each redelivery is genuinely retried instead of being skipped as a duplicate.
     */
    private static final String RETRY_TRIGGER_ITEM_ID = "item-retry";

    private final InventoryStore inventory;

    /**
     * Processes one order event: validates it, applies idempotent deduplication,
     * reserves stock, and stores the result for later lookup.
     *
     * @return a {@link ReservationResult} for an acknowledgeable outcome
     *         ({@code APPROVED}, {@code REJECTED} or {@code DUPLICATE})
     * @throws IllegalArgumentException for a non-retryable failure — a malformed
     *         event, an invalid quantity, or an unknown item; the caller lets these
     *         propagate so the error handler routes them to the dead-letter topic
     * @throws RuntimeException for the simulated transient (retryable) failure
     */
    public ReservationResult process(OrderEvent event) {
        validate(event);

        if (RETRY_TRIGGER_ITEM_ID.equals(event.itemId())) {
            throw new RuntimeException("Simulated transient failure for orderId=" + event.orderId());
        }

        // Idempotency: claim the order ID atomically before doing any work. Kafka
        // delivers at-least-once, so the same orderId can arrive more than once
        // (a lost offset commit, or two partition threads under concurrency > 1).
        // Only the first caller wins the claim, so reserve() never double-counts.
        if (!inventory.claimOrder(event.orderId())) {
            log.info("Duplicate order ignored: {}", event.orderId());
            return new ReservationResult(ReservationStatus.DUPLICATE, "already processed",
                    event.orderId(), event.correlationId());
        }

        if (!inventory.contains(event.itemId())) {
            inventory.saveResult(new OrderResult(
                    event.orderId(), event.itemId(), event.quantity(),
                    OrderStatus.UNKNOWN, "Unknown item " + event.itemId(),
                    Instant.now(), event.correlationId()));
            throw new IllegalArgumentException(
                    "Unknown item orderId=" + event.orderId() + " itemId=" + event.itemId());
        }

        boolean reserved = inventory.reserve(event.itemId(), event.quantity());
        OrderStatus status = reserved ? OrderStatus.APPROVED : OrderStatus.REJECTED;
        String reason = reserved
                ? "Reserved " + event.quantity() + " of " + event.itemId()
                : "Insufficient stock for " + event.itemId();
        inventory.saveResult(new OrderResult(
                event.orderId(), event.itemId(), event.quantity(),
                status, reason, Instant.now(), event.correlationId()));

        return new ReservationResult(
                reserved ? ReservationStatus.APPROVED : ReservationStatus.REJECTED,
                reason, event.orderId(), event.correlationId());
    }

    /**
     * Validates a freshly consumed event. The Kafka topic — not the validated HTTP
     * API — is the trust boundary, so the consumer must not assume a well-formed
     * payload. A quantity below 1 is especially dangerous: a negative value would
     * make {@code reserve()} compute {@code current - quantity} and silently
     * inflate stock.
     *
     * @throws IllegalArgumentException if the event is malformed or the quantity is invalid
     */
    private void validate(OrderEvent event) {
        if (event == null || event.orderId() == null || event.itemId() == null) {
            throw new IllegalArgumentException("Malformed order event: " + event);
        }
        if (event.quantity() < 1) {
            throw new IllegalArgumentException(
                    "Invalid quantity " + event.quantity() + " for orderId=" + event.orderId()
                            + " (must be >= 1)");
        }
    }
}
