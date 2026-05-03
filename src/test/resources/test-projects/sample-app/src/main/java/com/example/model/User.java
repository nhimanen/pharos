package com.example.model;

/**
 * Represents a user in the system with a name and email address.
 */
public class User {

    private final String name;
    private final String email;

    /**
     * Creates a new User.
     *
     * @param name  the user's display name
     * @param email the user's email address
     */
    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }

    /**
     * Returns the user's display name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the user's email address.
     */
    public String getEmail() {
        return email;
    }

    /**
     * Checks whether this user has a valid email address (non-null and contains @).
     */
    public boolean hasValidEmail() {
        return email != null && email.contains("@");
    }

    @Override
    public String toString() {
        return "User{name='" + name + "', email='" + email + "'}";
    }
}
