package kr.magicbox.ssegateway.adapter.in.kafka;

import tools.jackson.databind.ObjectMapper;
import kr.magicbox.ssegateway.adapter.in.kafka.event.UserLoggedOutKafkaEvent;
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
public class LogoutKafkaListener {

    private final RedisPubSubAdapter redisPubSubAdapter;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "outbox.event.user-logged-out", groupId = "sse-gateway-service")
    public void handleLogout(ConsumerRecord<String, String> record) {
        UserLoggedOutKafkaEvent event;
        try {
            event = objectMapper.readValue(record.value(), UserLoggedOutKafkaEvent.class);
        } catch (Exception e) {
            log.error("logout 이벤트 역직렬화 실패: {}", record.value(), e);
            return;
        }

        log.debug("로그아웃 이벤트 수신, Redis logout 발행 userId={}", event.userId());
        redisPubSubAdapter.publishLogout(UserId.of(event.userId())).subscribe();
    }
}
