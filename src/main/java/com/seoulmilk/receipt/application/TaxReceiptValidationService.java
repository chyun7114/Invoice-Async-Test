package com.seoulmilk.receipt.application;

import com.seoulmilk.core.infrastructure.security.CustomUserDetails;
import com.seoulmilk.emp.domain.entity.Emp;
import com.seoulmilk.emp.domain.repository.EmpRepository;
import com.seoulmilk.emp.exception.EmpErrorCode;
import com.seoulmilk.receipt.domain.InValidReceiptRepository;
import com.seoulmilk.receipt.domain.ValidReceiptRepository;
import com.seoulmilk.receipt.domain.entity.InValidReceipt;
import com.seoulmilk.receipt.dto.request.OcrValidationRequest;
import com.seoulmilk.receipt.dto.request.TaxReceiptValidationRequest;
import com.seoulmilk.receipt.exception.ReceiptErrorCode;
import com.seoulmilk.receipt.infrastructure.factory.ReceiptFactory;
import com.seoulmilk.receipt.infrastructure.factory.TaxReceiptValidationRequestFactory;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.entity.InValidReceiptJpaEntity;
import com.seoulmilk.receipt.infrastructure.persistence.jpa.repository.InValidJpaReceiptRepository;
import com.seoulmilk.receipt.infrastructure.persistence.mapper.InValidReceiptMapper;
import com.seoulmilk.receipt.infrastructure.service.ReceiptCacheService;
import com.seoulmilk.receipt.presentation.dto.request.ValidationRequest;
import com.seoulmilk.receipt.presentation.dto.response.AdditionalAuthResponse;
import com.seoulmilk.receipt.presentation.dto.response.TaxReceiptValidationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@RequiredArgsConstructor
@Log4j2
public class TaxReceiptValidationService {
    //    private final TaxReceiptValidationProvider taxReceiptValidationProvider;
    private final EmpRepository empRepository;
    private final ReceiptCacheService receiptCacheService;
    private final InValidReceiptRepository invalidReceiptRepository;
    private final ValidReceiptRepository validReceiptRepository;
    private final RedisTemplate redisTemplate;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}", concurrency = "3")
    public void listen(List<OcrValidationRequest> ocrValidationRequestList) {
        if (ocrValidationRequestList == null || ocrValidationRequestList.isEmpty()) return;

        // 1. Idempotency (멱등성 방어) 필터링
        // Kafka 의 재전송(At-least-once)으로 인해 완벽히 동일한 이벤트가 2~3번 들어올 위험이 있습니다.
        // 첫 번째 파일의 고유 URL(내부에 UUID 존재)을 추출해 고유 식별 키로 사용합니다.
        String idempotencyKey = "idempotency:ocr_event:" + ocrValidationRequestList.getFirst().fileUrl();
        
        // setIfAbsent: 키가 존재하지 않으면 Redis에 저장(true 반환), 이미 존재하면 실패(false 반환)
        Boolean isFirstReceived = redisTemplate.opsForValue().setIfAbsent(idempotencyKey, "DONE", java.time.Duration.ofMinutes(10));
        
        if (Boolean.FALSE.equals(isFirstReceived)) {
            log.warn("[Consumer-Idempotency] 🚨 이미 처리 중이거나 완료된 중복 이벤트입니다! 안전하게 무시합니다. Key: {}", idempotencyKey);
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("[Consumer] Kafka 이벤트 수신 및 중복 체크 통과: 총 {} 장의 OCR 파일", ocrValidationRequestList.size());

        Long pk = ocrValidationRequestList.getFirst().empPk();
        log.info("[Consumer] 현재 처리 중인 사용자 PK - {}", pk);

        log.info("[Consumer] 국세청 검증 서버 연동 중...");
        
        // 국세청 검증 지연시간(1~3초) 시뮬레이션
        simulateDelay(1000, 3000);
        
        // 여기에 원래 들어갔어야 할 Redis 적재 및 DB 저장 로직이 성공적으로 완료되었다고 가정
        String transactionId = UUID.randomUUID().toString();
        log.info("[Consumer] 가상 검증 성공! 할당된 Transaction ID: {}", transactionId);
        
        long endTime = System.currentTimeMillis();
        log.info("[Consumer] 비동기 데이터 처리 최종 완료. 소요시간: {}ms", (endTime - startTime));
    }

    private void simulateDelay(int minMs, int maxMs) {
        try {
            int delay = new java.util.Random().nextInt(maxMs - minMs + 1) + minMs;
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
//    private AdditionalAuthResponse requestAdditionalAuthentication(List<TaxReceiptValidationRequest> taxReceiptValidationRequests) {
//        return taxReceiptValidationProvider.requestAdditionalAuthentication(taxReceiptValidationRequests);
//    }
//
//    private Emp getEmployee(Long empPk) {
//        return empRepository.findById(empPk)
//                .orElseThrow(EmpErrorCode.NOT_EXIST_EMPLOYEE::toException);
//    }
//
//    private TaxReceiptValidationRequest createTaxReceiptValidationRequest(
//            Emp emp, OcrValidationRequest ocrValidationRequest, String uuid
//    ) {
//        return TaxReceiptValidationRequestFactory.create(emp, ocrValidationRequest, uuid);
//    }
//
//    private TaxReceiptValidationRequest createTaxReceiptValidationRequest(
//            Emp emp, ValidationRequest validationRequest, String uuid
//    ) {
//        return TaxReceiptValidationRequestFactory.create(emp, validationRequest, uuid);
//    }
//
//    public Boolean retrieveValidatedTaxReceiptsWithTransactionId(CustomUserDetails customUserDetails){
//        log.info("[retrieveValidatedTaxReceiptsWithTransactionId] 현재 사용자 pk - {}", customUserDetails.getId());
//        Emp emp = getEmployee(customUserDetails.getId());
//        log.info("[retrieveValidatedTaxReceiptsWithTransactionId] 현재 사용자 정보 - {}", emp.getName());
//
//        String transactionCacheKey = "transactionId:" + customUserDetails.getId();
//        String transactionId = receiptCacheService.getTransactionIdInRedis(transactionCacheKey);
//        log.info("[retrieveValidatedTaxReceiptsWithTransactionId] 현재 트랜잭션 id - {}", transactionId);
//
//        String dataCacheKey = "requestData:" + customUserDetails.getId();
//        List<OcrValidationRequest> requestsData = receiptCacheService.getOcrValidationRequestDataInRedis(dataCacheKey);
//
//        List<TaxReceiptValidationResponse> responses = taxReceiptValidationProvider.retrieveValidatedTaxReceipts(transactionId);
//        Collections.reverse(responses);
//
//        Boolean success = saveRecieptData(emp, requestsData, responses);
//
//        return success;
//    }
//
//    private Boolean saveRecieptData(Emp emp, List<OcrValidationRequest> requestsData, List<TaxReceiptValidationResponse> responses){
//        Boolean flag = true;
//        for(int i = 0; i < responses.size(); i++) {
//            OcrValidationRequest ocrValidationRequest = requestsData.get(i);
//            if(responses.get(i).resAuthenticity().equals("1")){
//                validReceiptRepository.save(ReceiptFactory.validReceiptCreate(emp, ocrValidationRequest));
//            }else if(responses.get(i).resAuthenticity().equals("0")){
//                flag = false;
//                InValidReceipt inValidReceipt = ReceiptFactory.inValidReceiptCreate(emp, ocrValidationRequest);
//                invalidReceiptRepository.save(inValidReceipt);
//            }
//        }
//
//        return flag;
//    }
//
//    // 파일 업로드 하지 않고 5개 정보로 요청을 보낸경우
//    public String requestAdditionalAuthentication(
//            CustomUserDetails customUserDetails,
//            List<ValidationRequest> requests
//    ) {
//        Emp emp = getEmployee(customUserDetails.getId());
//
//        List<TaxReceiptValidationRequest> taxReceiptValidationRequestList = new ArrayList<>();
//        String uuid = UUID.randomUUID().toString();
//        for (ValidationRequest validationRequest : requests) {
//            TaxReceiptValidationRequest taxReceiptValidationRequest =
//                    createTaxReceiptValidationRequest(emp, validationRequest, uuid);
//            taxReceiptValidationRequestList.add(taxReceiptValidationRequest);
//        }
//        log.info("현재 요청 보내는 데이터 - {}", taxReceiptValidationRequestList);
//
//        AdditionalAuthResponse additionalAuthResponse =
//            taxReceiptValidationProvider.requestAdditionalAuthentication(taxReceiptValidationRequestList);
//        String key = "taxReceiptValidationRequestList" + customUserDetails.getId();
//        redisTemplate.opsForValue().set(key, taxReceiptValidationRequestList);
//
//        return additionalAuthResponse.jti();
//    }
//
//    public Boolean retrieveValidatedTaxReceipts(
//            CustomUserDetails customUserDetails, List<Long> receiptPks, String transactionId
//    ) {
//        List<TaxReceiptValidationResponse> responses =
//                taxReceiptValidationProvider.retrieveValidatedTaxReceipts(transactionId);
//
//        Collections.reverse(responses);
//
//        Boolean success = updateInvalidReceiptData(getEmployee(customUserDetails.getId()), receiptPks, responses);
//
//        return success;
//    }
//
//    public Boolean updateInvalidReceiptData(Emp emp, List<Long> receiptPks, List<TaxReceiptValidationResponse> responses){
//        Boolean flag = true;
//        String key = "taxReceiptValidationRequestList" + emp.getId();
//        List<TaxReceiptValidationRequest> taxReceiptValidationRequestList =
//                (List<TaxReceiptValidationRequest>) redisTemplate.opsForValue().getAndDelete(key);
//
//        for(int i = 0; i < responses.size(); i++) {
//            if (i >= receiptPks.size()) {
//                throw new IllegalStateException("receiptPks와 responses의 개수가 일치하지 않습니다.");
//            }
//            Long pk = receiptPks.get(i);
//            if(responses.get(i).resAuthenticity().equals("1")){
//                InValidReceipt inValidReceipt = invalidReceiptRepository.findById(pk)
//                        .orElseThrow(ReceiptErrorCode.NOT_EXIST_RECEIPT::toException);
//                invalidReceiptRepository.deleteById(pk);
//                validReceiptRepository.save(ReceiptFactory.validReceiptCreate(
//                        emp, inValidReceipt, taxReceiptValidationRequestList.get(i)
//                ));
//            }else if(responses.get(i).resAuthenticity().equals("0")){
//                flag = false;
//            }
//        }
//
//        return flag;
//    }
}
