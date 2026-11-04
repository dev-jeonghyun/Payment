package com.fraud.payment.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class PaymentConsumer {

    // 1. payment-topic 전용 그룹
    @KafkaListener(topics = "payment-topic", groupId = "payment-group")
    public void consume(ConsumerRecord<String, String> record) {
        System.out.println("========================================");
        System.out.println(" [Kafka Consumer] 결제 요청 수신 성공!");
        System.out.println(" 받은 데이터(Payload): " + record.value());
        System.out.println("========================================");
    }

    // 2. detection-result-topic 전용 그룹 (groupId를 다르게 분리!)
    @KafkaListener(topics = "detection-result-topic", groupId = "detection-result-group")
    public void consumeDetectionResult(ConsumerRecord<String, String> record) {
        try {
            String payload = record.value();
            log.info("🎯 [AI 탐지 결과 최종 수신 성공!] {}", payload);
        } catch (Exception e) {
            log.error("❌ 탐지 결과 처리 중 에러 발생", e);
        }
    }
}