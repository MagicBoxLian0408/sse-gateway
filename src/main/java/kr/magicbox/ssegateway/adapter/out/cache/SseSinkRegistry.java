package kr.magicbox.ssegateway.adapter.out.cache;

import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * userId를 키로 SSE Sink를 관리하는 인메모리 레지스트리.
 * Redis Pub/Sub 메시지를 수신한 인스턴스에서 해당 userId의 Sink가 있을 때만 emit한다.
 */
@Slf4j
@Component
public class SseSinkRegistry {

    private final Map<Long, Sinks.One<SseNotificationResponse>> sinks = new ConcurrentHashMap<>();

    public Sinks.One<SseNotificationResponse> register(UserId userId) {
        Sinks.One<SseNotificationResponse> sink = Sinks.one();
        sinks.put(userId.value(), sink);
        log.info("[SINK-REGISTRY] Sink 등록 userId={} totalSinks={}", userId.value(), sinks.size());
        return sink;
    }

    public void emit(UserId userId, SseNotificationResponse payload) {
        Sinks.One<SseNotificationResponse> sink = sinks.remove(userId.value());
        if (sink != null) {
            Sinks.EmitResult result = sink.tryEmitValue(payload);
            log.info("[SINK-REGISTRY] emit 완료 userId={} result={}", userId.value(), result);
        } else {
            log.warn("[SINK-REGISTRY] emit 실패 - Sink 없음 userId={}", userId.value());
        }
    }

    public void remove(UserId userId) {
        Sinks.One<SseNotificationResponse> sink = sinks.remove(userId.value());
        if (sink != null) {
            sink.tryEmitEmpty();
            log.info("[SINK-REGISTRY] Sink 제거 userId={}", userId.value());
        }
    }

    public boolean contains(UserId userId) {
        return sinks.containsKey(userId.value());
    }
}
