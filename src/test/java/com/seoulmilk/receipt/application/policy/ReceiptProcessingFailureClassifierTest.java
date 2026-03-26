package com.seoulmilk.receipt.application.policy;

import com.seoulmilk.core.exception.DomainException;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.exception.ReviewRequiredException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;

import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReceiptProcessingFailureClassifierTest {
    private final ReceiptProcessingFailureClassifier classifier = new ReceiptProcessingFailureClassifier();

    @Test
    void classifyBusinessFailure() {
        ProcessingFailureType failureType = classifier.classify(new DomainException("business failure"));
        assertEquals(ProcessingFailureType.BUSINESS, failureType);
    }

    @Test
    void classifyReviewRequiredFailure() {
        ProcessingFailureType failureType = classifier.classify(new ReviewRequiredException("manual review"));
        assertEquals(ProcessingFailureType.REVIEW_REQUIRED, failureType);
    }

    @Test
    void classifyTransientFailureByCause() {
        RuntimeException ex = new RuntimeException(new SocketTimeoutException("timeout"));
        ProcessingFailureType failureType = classifier.classify(ex);
        assertEquals(ProcessingFailureType.TRANSIENT, failureType);
    }

    @Test
    void classifyTransientFailureBySpringException() {
        ProcessingFailureType failureType = classifier.classify(new QueryTimeoutException("db timeout", null));
        assertEquals(ProcessingFailureType.TRANSIENT, failureType);
    }
}
