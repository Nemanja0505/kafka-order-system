package com.kafkaordersystem.orderapi.producer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.kafkaordersystem.orderapi.dto.OrderRequest;
import com.kafkaordersystem.orderapi.event.OrderEvent;
import com.kafkaordersystem.orderapi.exception.OrderPublishingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventProducer {

    private static final String ORDERS_TOPIC = "orders";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    /**
     * Publishes the order event to the {@code orders} topic and blocks up to
     * 5 seconds for the broker to acknowledge it.
     *
     * @throws OrderPublishingException if the broker is unavailable, the send
     *         times out, or the publish otherwise fails — {@code GlobalExceptionHandler}
     *         surfaces this to the caller as HTTP 503
     */
    public void publishOrderEvent(OrderRequest request, String correlationId) {
        OrderEvent event = new OrderEvent(
                request.orderId(),
                request.itemId(),
                request.quantity(),
                Instant.now(),
                UUID.fromString(correlationId)
        );

        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>(ORDERS_TOPIC, event.orderId(), event);
        record.headers().add("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8));

        try {
            kafkaTemplate.send(record)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            MDC.put("correlationId", correlationId);
                            try {
                                log.info("Published order event orderId={} correlationId={} topic={} partition={} offset={}",
                                        event.orderId(),
                                        event.correlationId(),
                                        result.getRecordMetadata().topic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } finally {
                                MDC.clear();
                            }
                        }
                    })
                    .get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new OrderPublishingException(
                    "Failed to publish order event for orderId=" + event.orderId(),
                    event.orderId(), e.getCause());
        } catch (TimeoutException e) {
            throw new OrderPublishingException(
                    "Timed out publishing order event for orderId=" + event.orderId(),
                    event.orderId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublishingException(
                    "Interrupted while publishing order event for orderId=" + event.orderId(),
                    event.orderId(), e);
        } catch (RuntimeException e) {
            throw new OrderPublishingException(
                    "Failed to publish order event for orderId=" + event.orderId(),
                    event.orderId(), e);
        }
    }

}
