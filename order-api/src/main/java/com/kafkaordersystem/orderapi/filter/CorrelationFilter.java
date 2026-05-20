package com.kafkaordersystem.orderapi.filter;

import java.io.IOException;
import java.util.UUID;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Establishes the correlation ID at the HTTP boundary, so every log line for a
 * request - the controller's first one included - carries the same ID, and the
 * caller gets that ID back to correlate later calls (e.g. GET /orders/{id}/status).
 *
 * <p>An inbound {@code X-Correlation-Id} header is honoured when it is a valid
 * UUID; anything else (absent, blank, or non-UUID) yields a freshly generated
 * one. The ID stays a UUID end-to-end - it also populates
 * {@code OrderEvent.correlationId} - so a non-UUID inbound value cannot be
 * propagated and is replaced.
 */
@Slf4j
@Component
public class CorrelationFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String correlationId = resolveCorrelationId(request.getHeader(CORRELATION_ID_HEADER));
        try {
            MDC.put(CORRELATION_ID_MDC_KEY, correlationId);
            // Set on the response before the chain runs, while it is still uncommitted.
            response.setHeader(CORRELATION_ID_HEADER, correlationId);
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(CORRELATION_ID_MDC_KEY);
        }
    }

    private String resolveCorrelationId(String inboundHeader) {
        if (inboundHeader != null && !inboundHeader.isBlank()) {
            try {
                return UUID.fromString(inboundHeader.trim()).toString();
            } catch (IllegalArgumentException e) {
                log.warn("Ignoring non-UUID X-Correlation-Id header '{}' - generating a new ID",
                        inboundHeader);
            }
        }
        return UUID.randomUUID().toString();
    }
}
