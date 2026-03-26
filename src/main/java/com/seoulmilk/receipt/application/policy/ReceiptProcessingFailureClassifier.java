package com.seoulmilk.receipt.application.policy;

import com.seoulmilk.core.exception.DomainException;
import com.seoulmilk.receipt.domain.value.ProcessingFailureType;
import com.seoulmilk.receipt.exception.ReviewRequiredException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLTransientException;
import java.util.concurrent.TimeoutException;

@Component
public class ReceiptProcessingFailureClassifier {

    public ProcessingFailureType classify(Throwable throwable) {
        if (throwable == null) {
            return ProcessingFailureType.BUSINESS;
        }

        if (hasCause(throwable, ReviewRequiredException.class)) {
            return ProcessingFailureType.REVIEW_REQUIRED;
        }

        if (hasCause(throwable, DomainException.class) || hasCause(throwable, IllegalArgumentException.class)) {
            return ProcessingFailureType.BUSINESS;
        }

        if (isTransientFailure(throwable)) {
            return ProcessingFailureType.TRANSIENT;
        }

        return ProcessingFailureType.BUSINESS;
    }

    private boolean isTransientFailure(Throwable throwable) {
        return hasCause(throwable, TimeoutException.class)
                || hasCause(throwable, SocketTimeoutException.class)
                || hasCause(throwable, ConnectException.class)
                || hasCause(throwable, IOException.class)
                || hasCause(throwable, SQLTransientException.class)
                || hasCause(throwable, TransientDataAccessException.class);
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable cursor = throwable;
        while (cursor != null) {
            if (type.isInstance(cursor)) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }
}
