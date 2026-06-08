package kr.magicbox.ssegateway.adapter.in.kafka;

import kr.magicbox.ssegateway.adapter.in.kafka.event.PurchaseTokenIssuedKafkaEvent;
import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.redis.RedisPubSubAdapter;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PurchaseTokenKafkaListener {

    private final RedisPubSubAdapter redisPubSubAdapter;

    @KafkaListener(topics = "sse.purchase-token-issued", groupId = "sse-gateway-service")
    public void handlePurchaseTokenIssued(ConsumerRecord<String, PurchaseTokenIssuedKafkaEvent> record) {
        PurchaseTokenIssuedKafkaEvent event = record.value();

        log.debug("purchase token 발급 이벤트 수신 releaseId={} userId={}", event.releaseId(), event.userId());

        SseNotificationResponse payload = SseNotificationResponse.builder()
                .purchaseToken(event.purchaseToken())
                .build();

        redisPubSubAdapter.publishNotification(UserId.of(event.userId()), payload).subscribe();
    }
}
