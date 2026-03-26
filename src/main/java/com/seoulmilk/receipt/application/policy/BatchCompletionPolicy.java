package com.seoulmilk.receipt.application.policy;

import com.seoulmilk.receipt.domain.value.BatchProcessStatus;
import org.springframework.stereotype.Component;

@Component
public class BatchCompletionPolicy {

    public boolean isCompleted(int totalCount, long successCount, long failedCount, long skippedDuplicateCount, long reviewRequiredCount) {
        long finalized = successCount + failedCount + skippedDuplicateCount + reviewRequiredCount;
        return totalCount > 0 && finalized >= totalCount;
    }

    public BatchProcessStatus resolveStatus(long failedCount, long skippedDuplicateCount, long reviewRequiredCount) {
        if (failedCount > 0 || skippedDuplicateCount > 0 || reviewRequiredCount > 0) {
            return BatchProcessStatus.COMPLETED_WITH_FAILURES;
        }
        return BatchProcessStatus.COMPLETED;
    }
}
