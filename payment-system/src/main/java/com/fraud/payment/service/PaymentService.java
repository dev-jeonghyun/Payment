package com.fraud.payment.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async; // 1. 임포트 추가
import com.fasterxml.jackson.databind.ObjectMapper; 
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper(); // JSON 변환기

    @Async("paymentTaskExecutor") // 2. 비동기 스레드 풀 지정
    public void sendPaymentRequest(String orderId, Long userId, Double timeDiff,
                                   Integer locDiff, Integer isNight, Integer isHighAmount) {
        try {
            // 1. FastAPI가 기대하는 형식으로 Map 구성
            Map<String, Object> messageMap = new HashMap<>();
            messageMap.put("orderId", orderId);
            messageMap.put("userId", userId);
            messageMap.put("time_diff", timeDiff);
            messageMap.put("loc_diff", locDiff);
            messageMap.put("is_night", isNight);
            messageMap.put("is_high_amount", isHighAmount);

            // 2. 객체를 JSON 문자열로 변환
            String message = objectMapper.writeValueAsString(messageMap);

            // 3. Kafka 전송
            kafkaTemplate.send("payment-topic", message);
            System.out.println("[Kafka] 결제 데이터 전송: " + orderId);

        } catch (Exception e) {
            System.err.println("JSON 변환 에러: " + e.getMessage());
        }
    }
}