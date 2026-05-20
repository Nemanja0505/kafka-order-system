package com.kafkaordersystem.inventoryservice.consumer;

import java.nio.charset.StandardCharsets;

import com.kafkaordersystem.inventoryservice.event.OrderEvent;
import com.kafkaordersystem.inventoryservice.service.ReservationResult;
import com.kafkaordersystem.inventoryservice.service.ReservationService;
import com.kafkaordersystem.inventoryservice.service.ReservationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/**
 * Kafka transport layer for the {@code orders} topic.
 *
 * <p>This class owns transport concerns only — correlation-ID MDC setup, offset
 * acknowledgement, and re-throwing failures so the container's error handler can
 * retry or dead-letter them. All validation, idempotency and reservation logic
 * lives in {@link ReservationService}.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ReservationService reservationService;

    /**
     * {@code concurrency = "3"} runs one listener thread per partition of the
     * 3-partition {@code orders} topic. Safe under concurrency because idempotency
     * uses an atomic claim ({@code InventoryStore.claimOrder}) and stock
     * reservation is an atomic compare-and-decrement.
     */
    @KafkaListener(topics = "orders", groupId = "inventory-service-group", concurrency = "3")
    public void handle(OrderEvent event,
                       @Header(name = "X-Correlation-Id", required = false) byte[] correlationIdHeader,
                       Acknowledgment ack) {
        try {
            if (correlationIdHeader != null) {
                MDC.put("correlationId", new String(correlationIdHeader, StandardCharsets.UTF_8));
            }

            ReservationResult result = reservationService.process(event);
            ack.acknowledge();
            log.info("Order {} {} - {}", result.orderId(), result.status(), result.reason());
        } catch (IllegalArgumentException e) {
            // Non-retryable: the error handler routes this straight to the DLQ.
            log.warn("Order event rejected as {} (non-retryable): {}",
                    ReservationStatus.INVALID, e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            // Retryable: the error handler retries with backoff, then dead-letters.
            log.warn("Order processing failed (retryable): {}", e.getMessage());
            throw e;
        } finally {
            MDC.clear();
        }
    }
}
