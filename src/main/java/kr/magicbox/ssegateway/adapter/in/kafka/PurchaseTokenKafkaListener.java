package kr.magicbox.ssegateway.adapter.in.kafka;

import kr.magicbox.ssegateway.adapter.in.kafka.annotation.Idempotent;
import kr.magicbox.ssegateway.adapter.in.kafka.event.PurchaseTokenIssuedKafkaEvent;
import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.persistence.repository.SseGatewayInboxRepository;
import kr.magicbox.ssegateway.adapter.out.redis.RedisStreamAdapter;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PurchaseTokenKafkaListener {

    private final RedisStreamAdapter redisStreamAdapter;
    private final SseGatewayInboxRepository sseGatewayInboxRepository;

    @Idempotent
    @RetryableTopic
    @KafkaListener(topics = "sse.purchase-token-issued", groupId = "sse-gateway-service")
    public void handlePurchaseTokenIssued(ConsumerRecord<String, PurchaseTokenIssuedKafkaEvent> record) {
        PurchaseTokenIssuedKafkaEvent event = record.value();
        log.debug("purchase token 발급 이벤트 수신 releaseId={} userId={}", event.releaseId(), event.userId());

        SseNotificationResponse payload = SseNotificationResponse.builder()
                .purchaseToken(event.purchaseToken())
                .build();

        redisStreamAdapter.appendNotification(UserId.of(event.userId()), payload).subscribe();
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, ?> consumerRecord) {
        log.error("[Inbox] DLT 전환. topic={}, partition={}, offset={}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        sseGatewayInboxRepository.findByTopicAndPartitionAndOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset())
                .flatMap(inbox -> sseGatewayInboxRepository.save(inbox.markDeadLettered()))
                .subscribe();
    }
}
