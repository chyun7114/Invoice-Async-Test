package com.seoulmilk.invoice.infrastructure.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "invoice.mock")
public class InvoiceMockProperties {
    private int ocrDelayMs = 1500;
    private int validationDelayMs = 1000;
    private int minAutoFileCount = 1;
    private int maxAutoFileCount = 50;
}
