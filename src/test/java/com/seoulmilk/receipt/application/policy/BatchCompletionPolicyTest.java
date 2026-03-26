package com.seoulmilk.receipt.application.policy;

import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchCompletionPolicyTest {
    private final BatchCompletionPolicy policy = new BatchCompletionPolicy();

    @Test
    void completedWhenFinalizedCountMatchesTotal() {
        boolean completed = policy.isCompleted(10, 7, 2, 1, 0);
        assertTrue(completed);
    }

    @Test
    void notCompletedWhenFinalizedCountIsLessThanTotal() {
        boolean completed = policy.isCompleted(10, 6, 2, 1, 0);
        assertFalse(completed);
    }

    @Test
    void statusCompletedWithoutFailures() {
        BatchProcessStatus status = policy.resolveStatus(0, 0, 0);
        assertEquals(BatchProcessStatus.COMPLETED, status);
    }

    @Test
    void statusCompletedWithFailures() {
        BatchProcessStatus status = policy.resolveStatus(1, 0, 0);
        assertEquals(BatchProcessStatus.COMPLETED_WITH_FAILURES, status);
    }
}
