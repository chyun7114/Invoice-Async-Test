package com.seoulmilk.config;

import com.seoulmilk.core.application.FileStorageService;
import com.seoulmilk.invoice.application.OpenFeignService;
import io.minio.MinioClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import static org.mockito.Mockito.mock;

/**
 * 테스트 환경에서 외부 의존성(OpenFeign, MinIO 등)을 Mock으로 대체합니다.
 * 실제 OCR/국세청/MinIO 서버 등에 연결할 필요 없이 테스트가 독립적으로 실행됩니다.
 */
@TestConfiguration
@Profile("test")
public class TestMockBeanConfig {

    @Bean
    @Primary
    public OpenFeignService mockOpenFeignService() {
        return mock(OpenFeignService.class);
    }

    @Bean
    @Primary
    public MinioClient mockMinioClient() {
        return mock(MinioClient.class);
    }

    @Bean
    @Primary
    public FileStorageService mockFileStorageService() {
        return mock(FileStorageService.class);
    }
}
