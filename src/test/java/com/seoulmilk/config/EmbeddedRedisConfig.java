package com.seoulmilk.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import redis.embedded.RedisServer;

/**
 * 테스트 환경에서 실제 Redis 서버 없이 동작하기 위한 Embedded Redis 설정.
 * test 프로파일이 활성화될 때만 자동으로 Redis를 6379 포트에 띄우고,
 * 테스트 종료 시 자동으로 내려줍니다.
 */
@Configuration
@Profile("test")
public class EmbeddedRedisConfig {

    private RedisServer redisServer;

    @PostConstruct
    public void start() throws Exception {
        try {
            redisServer = RedisServer.builder()
                    .port(6379)
                    .setting("maxmemory 128M")
                    .build();
            redisServer.start();
        } catch (Exception e) {
            // 이미 포트가 사용중이면 무시 (이전 테스트에서 떠있을 수 있음)
        }
    }

    @PreDestroy
    public void stop() {
        if (redisServer != null) {
            redisServer.stop();
        }
    }
}
