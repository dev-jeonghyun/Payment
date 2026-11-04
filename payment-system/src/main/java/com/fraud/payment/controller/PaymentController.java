package com.fraud.payment.controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import com.fraud.payment.dto.PaymentRequest;
import com.fraud.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api") // 공통 주소 지정
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/payments")
    public String pay(@RequestBody PaymentRequest request) {
        paymentService.sendPaymentRequest(
            request.orderId(), 
            request.userId(), 
            request.timeDiff(), 
            request.locDiff(), 
            request.isNight(), 
            request.isHighAmount()
        );
        return "결제 요청이 카프카로 전송되었습니다: " + request.orderId();
    }
}