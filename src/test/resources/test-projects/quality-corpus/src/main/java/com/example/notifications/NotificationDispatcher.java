package com.example.notifications;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Routes outbound notifications across email, mobile push, and broadcast channels.
 */
public class NotificationDispatcher {

    /**
     * Sends an email notification to a single recipient address.
     *
     * @param recipient the destination email address
     * @param subject   the email subject line
     * @param body      the email body content (plain text or HTML)
     */
    public void sendEmailNotification(String recipient, String subject, String body) {
        // deliver via SMTP relay
        System.out.printf("[email] To: %s | Subject: %s%n", recipient, subject);
    }

    /**
     * Delivers a push notification to a registered mobile device.
     *
     * @param deviceToken the FCM or APNs device registration token
     * @param message     the notification text to display on the device
     */
    public void sendPushNotification(String deviceToken, String message) {
        System.out.printf("[push] device=%s msg=%s%n", deviceToken, message);
    }

    /**
     * Schedules a notification for delivery at a future UTC time.
     * Returns a scheduling identifier that can be used to cancel delivery before it occurs.
     *
     * @param recipient the notification recipient address
     * @param message   the content to deliver
     * @param when      the UTC datetime at which to dispatch the notification
     * @return the scheduled notification identifier
     */
    public String scheduleNotification(String recipient, String message, LocalDateTime when) {
        String id = UUID.randomUUID().toString();
        scheduled.put(id, new ScheduledItem(recipient, message, when));
        return id;
    }

    /**
     * Cancels a previously scheduled notification before it is dispatched.
     * Has no effect if the notification has already been sent.
     *
     * @param notificationId the identifier returned by {@link #scheduleNotification}
     */
    public void cancelScheduledNotification(String notificationId) {
        scheduled.remove(notificationId);
    }

    /**
     * Broadcasts the same message to all members of a named subscriber group.
     * Useful for product announcements, system alerts, or bulk communications
     * where all subscribers should receive the same content simultaneously.
     *
     * @param groupId the name of the subscriber group
     * @param message the content to send to every group member
     */
    public void broadcastToGroup(String groupId, String message) {
        List<String> members = subscriberGroups.getOrDefault(groupId, List.of());
        for (String addr : members) {
            sendEmailNotification(addr, "Broadcast from " + groupId, message);
        }
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private final Map<String, ScheduledItem> scheduled = new HashMap<>();
    private final Map<String, List<String>>  subscriberGroups = new HashMap<>();

    private record ScheduledItem(String recipient, String message, LocalDateTime when) {}
}
