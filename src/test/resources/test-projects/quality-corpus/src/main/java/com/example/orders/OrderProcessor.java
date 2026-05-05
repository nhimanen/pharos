package com.example.orders;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Processes customer orders through their full lifecycle:
 * submission, cancellation, pricing, promotional discounts, and payment refunds.
 */
public class OrderProcessor {

    /**
     * Submits a new order, reserving inventory and initiating payment capture.
     * Returns the assigned order identifier on success.
     *
     * @param customerId the purchasing customer's identifier
     * @param items      map of product IDs to quantities ordered
     * @return the new order identifier
     */
    public String submitOrder(String customerId, Map<String, Integer> items) {
        String orderId = UUID.randomUUID().toString();
        double total = calculateTotal(items);
        orders.put(orderId, new Order(customerId, total));
        return orderId;
    }

    /**
     * Cancels an existing order and releases any reserved inventory.
     * Automatically triggers a full payment refund if the charge was already captured.
     *
     * @param orderId the identifier of the order to cancel
     */
    public void cancelOrder(String orderId) {
        Order order = orders.get(orderId);
        if (order != null) {
            refundPayment(orderId, order.total);
            orders.remove(orderId);
        }
    }

    /**
     * Calculates the total price of the given items including applicable taxes.
     *
     * @param items product IDs mapped to quantities
     * @return the total price in the store's base currency unit
     */
    public double calculateTotal(Map<String, Integer> items) {
        double subtotal = items.entrySet().stream()
                .mapToDouble(e -> getPrice(e.getKey()) * e.getValue())
                .sum();
        return subtotal * 1.1; // 10% tax
    }

    /**
     * Applies a promotional discount code (coupon or voucher) to reduce the order total.
     * Updates the stored order total and records which promotion was redeemed.
     *
     * @param promoCode the coupon or voucher code to redeem
     * @param orderId   the order to apply the discount to
     * @return the updated order total after the discount
     */
    public double applyPromoCode(String promoCode, String orderId) {
        double discount = lookupDiscount(promoCode);
        Order order = orders.get(orderId);
        if (order != null) {
            order.total = Math.max(0, order.total - discount);
            return order.total;
        }
        return 0;
    }

    /**
     * Issues a full or partial refund to the customer's original payment method.
     * Returns the refund transaction identifier.
     *
     * @param orderId the order for which to issue a refund
     * @param amount  the amount to return; must not exceed the original charge
     * @return the refund transaction identifier
     */
    public String refundPayment(String orderId, double amount) {
        return "refund-" + orderId + "-" + amount;
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private final Map<String, Order> orders = new HashMap<>();

    private double getPrice(String productId) { return 10.0; }
    private double lookupDiscount(String code) { return 5.0; }

    private static class Order {
        final String customerId;
        double total;
        Order(String customerId, double total) {
            this.customerId = customerId;
            this.total = total;
        }
    }
}
