package kr.magicbox.ssegateway.adapter.in.kafka;

import kr.magicbox.ssegateway.adapter.in.kafka.annotation.Idempotent;
import kr.magicbox.ssegateway.adapter.in.kafka.event.UserLoggedOutKafkaEvent;
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
public class LogoutKafkaListener {

    private final RedisStreamAdapter redisStreamAdapter;
    private final SseGatewayInboxRepository sseGatewayInboxRepository;

    @Idempotent
    @RetryableTopic
    @KafkaListener(topics = "outbox.event.user-logged-out", groupId = "sse-gateway-service")
    public void handleLogout(ConsumerRecord<String, UserLoggedOutKafkaEvent> record) {
        UserLoggedOutKafkaEvent event = record.value();
        log.debug("로그아웃 이벤트 수신, Redis Stream logout 적재 userId={}", event.userId());
        redisStreamAdapter.appendLogout(UserId.of(event.userId())).subscribe();
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, ?> consumerRecord) {
        log.error("[Inbox] DLT 전환. topic={}, partition={}, offset={}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        sseGatewayInboxRepository.findByTopicAndPartitionAndOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset())
                .flatMap(inbox -> sseGatewayInboxRepository.save(inbox.markDeadLettered()))
                .subscribe();
    }
}
