package com.seoulmilk.invoice.infrastructure.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.ThreadLocalRandom;

@Getter
@Setter
@ConfigurationProperties(prefix = "invoice.mock")
public class InvoiceMockProperties {
    @Deprecated(forRemoval = false)
    private int ocrDelayMs = 1500;
    private int ocrDelayMinMs = 1500;
    private int ocrDelayMaxMs = 3000;

    @Deprecated(forRemoval = false)
    private int validationDelayMs = 1000;
    private int validationDelayMinMs = 1000;
    private int validationDelayMaxMs = 2000;

    private int defaultAutoFileCount = 5;

    @Deprecated(forRemoval = false)
    private int minAutoFileCount = 5;
    @Deprecated(forRemoval = false)
    private int maxAutoFileCount = 5;
    private double randomFailRate = 0.025;
    private double transientFailRatio = 0.70;
    private double reviewRequiredFailRatio = 0.20;
    private double businessFailRatio = 0.10;
    private long defaultEmpPk = 1L;
    private boolean allowMockEmpFallback = false;

    public int nextOcrDelayMs() {
        return nextDelay(ocrDelayMinMs, ocrDelayMaxMs, ocrDelayMs);
    }

    public int nextValidationDelayMs() {
        return nextDelay(validationDelayMinMs, validationDelayMaxMs, validationDelayMs);
    }

    private int nextDelay(int min, int max, int fallback) {
        int safeMin = Math.max(0, min);
        int safeMax = Math.max(safeMin, max);
        if (safeMin == safeMax) {
            return safeMin;
        }
        if (safeMin == 0 && safeMax == 0 && fallback > 0) {
            return fallback;
        }
        return ThreadLocalRandom.current().nextInt(safeMin, safeMax + 1);
    }

    public double normalizedTransientFailRatio() {
        return normalizeRatioPart(transientFailRatio);
    }

    public double normalizedReviewRequiredFailRatio() {
        return normalizeRatioPart(reviewRequiredFailRatio);
    }

    public double normalizedBusinessFailRatio() {
        return normalizeRatioPart(businessFailRatio);
    }

    private double normalizeRatioPart(double target) {
        double t = Math.max(0.0, transientFailRatio);
        double r = Math.max(0.0, reviewRequiredFailRatio);
        double b = Math.max(0.0, businessFailRatio);
        double sum = t + r + b;
        if (sum == 0.0) {
            return 0.0;
        }
        return Math.max(0.0, target) / sum;
    }
}
