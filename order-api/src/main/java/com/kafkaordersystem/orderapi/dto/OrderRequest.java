package com.kafkaordersystem.orderapi.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record OrderRequest(

        @NotBlank
        String orderId,

        @NotBlank
        String itemId,

        @Min(1)
        int quantity

) {
}
