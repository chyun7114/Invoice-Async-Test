package com.seoulmilk.invoice.application.usecase;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;

public interface InvoiceProcessUseCase {
    Boolean processInvoices(CustomUserDetails customUserDetails, List<MultipartFile> files);
}
