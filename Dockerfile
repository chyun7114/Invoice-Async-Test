# 1. 빌드 환경
FROM eclipse-temurin:21-jdk-jammy AS builder
WORKDIR /app

# 소스 코드 전체 복사
COPY . .

# 윈도우에서 넘어온 gradlew의 CRLF 문제 해결(sed) 및 실행 권한 부여
RUN sed -i 's/\r$//' ./gradlew && chmod +x ./gradlew

# 테스트 생략하고 빌드 진행
RUN ./gradlew clean build -x test

# 2. 실제 실행 환경
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# 빌드 스테이지에서 생성된 백엔드 jar 파일 복사
COPY --from=builder /app/build/libs/*SNAPSHOT.jar app.jar

# 타임존 설정 및 환경변수 주입하여 실행 (-Duser.timezone=Asia/Seoul)
ENTRYPOINT ["java", "-jar", "-Duser.timezone=Asia/Seoul", "-Dspring.profiles.active=local", "app.jar"]