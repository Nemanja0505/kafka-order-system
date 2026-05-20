package com.kafkaordersystem.orderapi.dto;

/**
 * Body of the {@code 202 Accepted} response for {@code POST /orders}. Returns
 * the {@code correlationId} so the caller can trace the order through the system
 * and correlate a later {@code GET /orders/{orderId}/status} call.
 */
public record OrderResponse(
        String orderId,
        String correlationId,
        String status
) {
}
