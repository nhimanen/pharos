package com.example.service;

import com.example.model.User;

/**
 * Service for generating personalized greeting and farewell messages.
 */
public class GreetingService {

    /**
     * Generates a greeting message for the given user.
     *
     * @param user the user to greet
     * @return a personalized greeting string
     */
    public String greet(User user) {
        return "Hello, " + user.getName() + "!";
    }

    /**
     * Generates a farewell message for the given user.
     *
     * @param user the user to bid farewell
     * @return a personalized farewell string
     */
    public String farewell(User user) {
        return "Goodbye, " + user.getName() + "!";
    }

    /**
     * Generates a formal greeting using the user's email domain.
     */
    public String formalGreet(User user) {
        String email = user.getEmail();
        String domain = email != null && email.contains("@")
                ? email.substring(email.indexOf('@') + 1)
                : "unknown";
        return "Dear " + user.getName() + " from " + domain + ",";
    }
}
