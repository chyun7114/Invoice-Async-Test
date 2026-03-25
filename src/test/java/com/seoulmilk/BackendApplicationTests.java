package com.seoulmilk;

import com.seoulmilk.core.application.FileStorageService;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
		partitions = 1,
		brokerProperties = {
				"listeners=PLAINTEXT://localhost:0",
				"port=0"
		}
)
class BackendApplicationTests {

	@MockBean
	private MinioClient minioClient;

	@MockBean
	private FileStorageService fileStorageService;

	@Test
	void contextLoads() {
	}

}
