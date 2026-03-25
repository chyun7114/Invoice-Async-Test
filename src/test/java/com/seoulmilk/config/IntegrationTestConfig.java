package com.seoulmilk.config;

import com.seoulmilk.core.application.FileStorageService;
import io.minio.MinioClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
public abstract class IntegrationTestConfig {

    @MockBean
    private MinioClient minioClient;

    @MockBean
    private FileStorageService fileStorageService;
}
