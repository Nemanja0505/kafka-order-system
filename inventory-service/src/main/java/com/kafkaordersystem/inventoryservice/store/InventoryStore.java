package com.kafkaordersystem.inventoryservice.store;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.kafkaordersystem.inventoryservice.model.OrderResult;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * In-memory store for stock levels, order results, and processed-order claims.
 *
 * <p>Kept as a plain data structure: the order-processing flow — validation,
 * idempotency decisions, reservation outcome — lives in the service layer.
 * {@code reserve()} stays here because its compare-and-decrement must run
 * atomically with the underlying map update.
 */
@Component
public class InventoryStore {

    private final Map<String, Integer> stock = new ConcurrentHashMap<>();
    private final Map<String, OrderResult> orderResults = new ConcurrentHashMap<>();

    /**
     * Order IDs already claimed for processing — the consumer-side idempotency set.
     *
     * <p>Idempotency is session-scoped — a service restart clears this set. Durable
     * deduplication requires an external store (Redis, DB). This is noted in the README.
     */
    private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();

    @PostConstruct
    void seed() {
        stock.put("item-1", 10);
        stock.put("item-2", 5);
        stock.put("item-3", 0); // 0 units intentionally — exercises the REJECTED path
    }

    /**
     * Reserves {@code quantity} units of an item, decrementing stock only when
     * the current level can cover the request.
     *
     * @return {@code true} if the stock was reserved; {@code false} if the item
     *         is unknown or does not have enough stock
     */
    public boolean reserve(String itemId, int quantity) {
        boolean[] reserved = {false};
        stock.computeIfPresent(itemId, (key, current) -> {
            if (current >= quantity) {
                reserved[0] = true;
                return current - quantity;
            }
            return current;
        });
        return reserved[0];
    }

    public int getStock(String itemId) {
        return stock.getOrDefault(itemId, 0);
    }

    public boolean contains(String itemId) {
        return stock.containsKey(itemId);
    }

    public void saveResult(OrderResult result) {
        orderResults.put(result.orderId(), result);
    }

    public Optional<OrderResult> findOrderResult(String orderId) {
        return Optional.ofNullable(orderResults.get(orderId));
    }

    /**
     * Atomically claims an order ID for processing. The first call for a given
     * {@code orderId} wins the claim; every later call is a duplicate the caller
     * should skip without error.
     *
     * <p>{@code Set.add} on a {@link ConcurrentHashMap} key set is a single atomic
     * operation, so the claim stays race-free when the listener runs with
     * {@code concurrency > 1}. Claims are session-scoped — a service restart
     * clears them.
     *
     * @return {@code true} the first time {@code orderId} is seen; {@code false}
     *         on every later call (a duplicate)
     */
    public boolean claimOrder(String orderId) {
        return processedOrderIds.add(orderId);
    }
}
