package com.kafkaordersystem.inventoryservice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class DeadLetterConsumer {

    @KafkaListener(
            topics = "orders-dead-letter",
            groupId = "inventory-service-dlq-group",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
    )
    public void handle(
            @Payload(required = false) String rawEvent,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionClass,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            Acknowledgment ack
    ) {
        log.error("DEAD LETTER received - reason: {} - {} | raw payload: {}",
                exceptionClass, exceptionMessage, rawEvent);
        ack.acknowledge();
    }
}
