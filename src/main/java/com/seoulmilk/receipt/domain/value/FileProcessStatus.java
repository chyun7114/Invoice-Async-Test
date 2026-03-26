package com.seoulmilk.receipt.domain.value;

public enum FileProcessStatus {
    SUCCESS,
    FAILED,
    RETRYING,
    DLQ,
    REVIEW_REQUIRED,
    SKIPPED_DUPLICATE
}
