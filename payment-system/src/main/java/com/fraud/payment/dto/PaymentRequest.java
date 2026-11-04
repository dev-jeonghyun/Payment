package com.fraud.payment.dto; // 패키지 경로는 프로젝트 구조에 맞게 확인해주세요!

public record PaymentRequest(
    String orderId,
    Long userId,
    Double timeDiff,
    Integer locDiff,
    Integer isNight,
    Integer isHighAmount
) {}