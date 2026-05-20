package com.kafkaordersystem.orderapi.controller;

import com.kafkaordersystem.orderapi.dto.OrderRequest;
import com.kafkaordersystem.orderapi.dto.OrderResponse;
import com.kafkaordersystem.orderapi.filter.CorrelationFilter;
import com.kafkaordersystem.orderapi.producer.OrderEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody OrderRequest request) {
        // CorrelationFilter has already put the ID in MDC for the whole request.
        String correlationId = MDC.get(CorrelationFilter.CORRELATION_ID_MDC_KEY);
        log.info("Received order: {}", request);
        orderEventProducer.publishOrderEvent(request, correlationId);

        OrderResponse response = new OrderResponse(request.orderId(), correlationId, "ACCEPTED");
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

}
