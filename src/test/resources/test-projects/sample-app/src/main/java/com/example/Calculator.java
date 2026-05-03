package com.example;

/**
 * Provides basic arithmetic operations.
 * Useful for performing mathematical calculations.
 */
public class Calculator {

    /**
     * Adds two integers and returns the sum.
     *
     * @param a the first operand
     * @param b the second operand
     * @return the sum of a and b
     */
    public int add(int a, int b) {
        return a + b;
    }

    /**
     * Subtracts b from a.
     *
     * @param a the minuend
     * @param b the subtrahend
     * @return the difference
     */
    public int subtract(int a, int b) {
        return a - b;
    }

    /**
     * Multiplies two integers.
     */
    public int multiply(int a, int b) {
        return a * b;
    }

    /**
     * Divides a by b. Throws IllegalArgumentException if b is zero.
     *
     * @param a the dividend
     * @param b the divisor (must not be zero)
     * @return the quotient
     * @throws IllegalArgumentException if b is zero
     */
    public double divide(double a, double b) {
        if (b == 0) {
            throw new IllegalArgumentException("Division by zero is not allowed");
        }
        return a / b;
    }

    /**
     * Computes the absolute value of an integer.
     */
    public int abs(int value) {
        return value < 0 ? -value : value;
    }
}
