package com.example.auth;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manages authentication tokens for session lifecycle control.
 * Handles generation, validation, and revocation of JWT-style access tokens.
 */
public class TokenManager {

    private final Set<String> denyList = new HashSet<>();

    /**
     * Generates a signed access token for the given user identity.
     * The token encodes expiry time and user role claims and is signed with HMAC-SHA256.
     *
     * @param userId the subject identifier
     * @param role   the user's permission role
     * @return a signed JWT access token string
     */
    public String generateAccessToken(String userId, String role) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("sub", userId);
        claims.put("role", role);
        claims.put("exp", System.currentTimeMillis() + 3_600_000);
        return sign(claims);
    }

    /**
     * Validates the signature and expiry of the given token.
     * Returns true if the token is well-formed, unexpired, and the signature verifies.
     *
     * @param token the JWT token to check
     * @return true if the credential is valid and not expired
     */
    public boolean validateToken(String token) {
        if (token == null || token.isBlank()) return false;
        if (denyList.contains(token)) return false;
        return verifySignature(token) && !isExpired(token);
    }

    /**
     * Revokes an access token by adding it to the deny-list.
     * After revocation {@link #validateToken} will return false for this token.
     *
     * @param token the token to invalidate
     */
    public void revokeToken(String token) {
        denyList.add(token);
    }

    /**
     * Issues a new access token using a valid refresh token.
     * Extends the session without requiring the user to log in again.
     * Throws if the refresh token is invalid or expired.
     *
     * @param refreshToken a long-lived refresh credential
     * @return a new short-lived access token
     * @throws IllegalArgumentException if the refresh token is invalid or expired
     */
    public String refreshExpiredToken(String refreshToken) {
        if (!validateToken(refreshToken)) {
            throw new IllegalArgumentException("Invalid or expired refresh token");
        }
        String userId = extractClaim(refreshToken, "sub");
        String role   = extractClaim(refreshToken, "role");
        return generateAccessToken(userId, role);
    }

    /**
     * Extracts the claims payload from a token without verifying the signature.
     * Use only for non-security-sensitive introspection such as logging or diagnostics.
     *
     * @param token the JWT token to decode
     * @return a map of claim name to string value
     */
    public Map<String, String> extractClaims(String token) {
        return parseClaims(token);
    }

    // ── stubs ─────────────────────────────────────────────────────────────────

    private String sign(Map<String, Object> claims) { return "tok." + claims.hashCode(); }
    private boolean verifySignature(String t)        { return t.startsWith("tok."); }
    private boolean isExpired(String t)              { return false; }
    private String extractClaim(String t, String k)  { return k + "-value"; }
    private Map<String, String> parseClaims(String t){ return Map.of(); }
}
