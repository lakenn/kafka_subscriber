package com.github.lamkenn.kafka.tutorial1.exception;

public class ConsumerException extends RuntimeException {
    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable ex) {
        super(message, ex);
    }
}