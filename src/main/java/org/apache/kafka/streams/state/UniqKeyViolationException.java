package org.apache.kafka.streams.state;

public class UniqKeyViolationException extends RuntimeException {
    public UniqKeyViolationException(String message) {
        super(message);
    }
}
