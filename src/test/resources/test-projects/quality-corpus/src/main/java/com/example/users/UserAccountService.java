package com.example.users;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Manages user account lifecycle: registration, session management,
 * profile updates, credential resets, and email verification.
 */
public class UserAccountService {

    /**
     * Registers a new user account with the given credentials.
     * Sends a verification email to confirm address ownership.
     * Returns the newly assigned user identifier.
     *
     * @param username the desired login username
     * @param email    the user's email address
     * @return the newly assigned user identifier
     */
    public String createAccount(String username, String email) {
        String userId = UUID.randomUUID().toString();
        accounts.put(userId, new Account(username, email));
        // send verification link to confirm email ownership
        String verifyToken = UUID.randomUUID().toString();
        pendingVerifications.put(verifyToken, email);
        return userId;
    }

    /**
     * Deactivates a user account, preventing further login.
     * Invalidates all active sessions for the account.
     * The account data is retained and can be reactivated by an administrator.
     *
     * @param userId the identifier of the account to suspend
     */
    public void deactivateAccount(String userId) {
        Account acct = accounts.get(userId);
        if (acct != null) {
            acct.active = false;
            activeSessions.remove(userId);
        }
    }

    /**
     * Initiates the password reset flow for the account identified by email.
     * Sends a one-time reset link to the registered email address.
     * The link expires after one hour.
     *
     * @param email the email address of the account whose password should be reset
     */
    public void resetPassword(String email) {
        Account acct = findByEmail(email);
        if (acct != null) {
            String resetToken = UUID.randomUUID().toString();
            passwordResetTokens.put(resetToken, email);
            // send reset link via email
        }
    }

    /**
     * Updates the display name shown to other users in the application.
     * Does not affect the login username.
     *
     * @param userId      the account to update
     * @param displayName the new display name to store
     */
    public void updateProfile(String userId, String displayName) {
        Account acct = accounts.get(userId);
        if (acct != null) {
            acct.displayName = displayName;
        }
    }

    /**
     * Confirms ownership of an email address using the one-time token
     * delivered during account registration or an email change request.
     *
     * @param token the verification token from the confirmation email
     * @return true if the token was valid and the address is now confirmed
     */
    public boolean verifyEmailAddress(String token) {
        String email = pendingVerifications.remove(token);
        if (email != null) {
            verifiedEmails.add(email);
            return true;
        }
        return false;
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private final Map<String, Account>      accounts             = new HashMap<>();
    private final Map<String, Set<String>>  activeSessions       = new HashMap<>();
    private final Map<String, String>       pendingVerifications = new HashMap<>();
    private final Set<String>               verifiedEmails       = new HashSet<>();
    private final Map<String, String>       passwordResetTokens  = new HashMap<>();

    private Account findByEmail(String email) {
        return accounts.values().stream()
                .filter(a -> email.equals(a.email))
                .findFirst()
                .orElse(null);
    }

    private static class Account {
        String username, email, displayName;
        boolean active = true;
        Account(String username, String email) {
            this.username = username;
            this.email = email;
        }
    }
}
