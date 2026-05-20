package com.kafkaordersystem.orderapi.exception;

public class OrderPublishingException extends RuntimeException {

    private final String orderId;

    public OrderPublishingException(String message, String orderId, Throwable cause) {
        super(message, cause);
        this.orderId = orderId;
    }

    public String getOrderId() {
        return orderId;
    }

}
