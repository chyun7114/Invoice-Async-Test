package com.seoulmilk.receipt.exception;

public class ReviewRequiredException extends RuntimeException {
    public ReviewRequiredException(String message) {
        super(message);
    }

    public ReviewRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
