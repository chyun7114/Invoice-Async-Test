package com.seoulmilk.core.infrastructure;

import com.seoulmilk.core.application.FileStorageService;
import com.seoulmilk.core.configuration.minio.MinioProperties;
import com.seoulmilk.core.exception.error.GlobalErrorCode;
import io.minio.*;
import io.minio.http.Method;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Log4j2
public class FileStorageServiceImpl implements FileStorageService {

    private final MinioClient minioClient;
    private final MinioProperties minioProperties;

    @Value("${minio.bucket}")
    private String bucketName;

    @Value("${minio.expiry}")
    private int expiry;

    @PostConstruct
    void initBucket() throws Exception {
        log.info("[MinIO-Init] endpoint={}, bucket={}, expiryDays={}",
                minioProperties.getEndpoint(),
                bucketName,
                expiry);

        try {
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
                log.info("[MinIO-Init] bucket does not exist. creating bucket={}", bucketName);
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("[MinIO-Init] bucket created. bucket={}", bucketName);
            } else {
                log.info("[MinIO-Init] bucket exists. bucket={}", bucketName);
            }
        } catch (Exception e) {
            log.error("[MinIO-Init] failed. endpoint={}, bucket={}, message={}",
                    minioProperties.getEndpoint(),
                    bucketName,
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    @Override
    public String uploadFile(MultipartFile file) throws Exception {
        if (file == null || file.isEmpty()) {
            throw GlobalErrorCode.INVALID_FILE.toException();
        }

        String objectName = UUID.randomUUID() + "_" + file.getOriginalFilename();
        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .stream(file.getInputStream(), file.getSize(), -1)
                        .contentType(file.getContentType())
                        .build()
        );

        return getPresignedUrl(objectName);
    }

    private String getPresignedUrl(String objectName) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(expiry, TimeUnit.DAYS)
                        .build()
        );
    }
}
