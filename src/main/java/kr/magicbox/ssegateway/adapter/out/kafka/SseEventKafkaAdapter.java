package kr.magicbox.ssegateway.adapter.out.kafka;

import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SseEventKafkaAdapter {

    private static final String CONNECTED_TOPIC = "sse.connected";
    private static final String DISCONNECTED_TOPIC = "sse.disconnected";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void publishConnected(UserId userId) {
        kafkaTemplate.send(CONNECTED_TOPIC, String.valueOf(userId.value()), "");
        log.debug("sse.connected 발행 userId={}", userId.value());
    }

    public void publishDisconnected(UserId userId) {
        kafkaTemplate.send(DISCONNECTED_TOPIC, String.valueOf(userId.value()), "");
        log.debug("sse.disconnected 발행 userId={}", userId.value());
    }
}
