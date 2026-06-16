package kr.magicbox.ssegateway.adapter.out.redis;

import tools.jackson.databind.ObjectMapper;
import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.cache.SseSinkRegistry;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Stream 기반 SSE 알림 어댑터.
 *
 * 스트림 키 명명 규칙:
 *   - 구매 토큰 알림: sse:stream:notification:{userId}
 *   - 로그아웃:       sse:stream:logout:{userId}
 *
 * 클라이언트가 SSE에 연결되면 해당 userId의 스트림을 XREAD 폴링으로 구독하고,
 * 연결이 끊기면 구독을 해제한다.
 * 메시지는 영속화되어 연결 전 발행된 메시지도 lastConsumed 이후 재조회 가능.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisStreamAdapter {

    private static final String NOTIFICATION_STREAM_PREFIX = "sse:stream:notification:";
    private static final String LOGOUT_STREAM_PREFIX = "sse:stream:logout:";
    private static final String PAYLOAD_FIELD = "payload";

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final SseSinkRegistry sinkRegistry;
    private final ObjectMapper objectMapper;

    private final Map<Long, Disposable> subscriptions = new ConcurrentHashMap<>();

    public void subscribe(UserId userId) {
        String notificationStream = NOTIFICATION_STREAM_PREFIX + userId.value();
        String logoutStream = LOGOUT_STREAM_PREFIX + userId.value();

        Disposable subscription = pollStream(userId, notificationStream, logoutStream);
        subscriptions.put(userId.value(), subscription);
        log.debug("Redis Stream 구독 등록 userId={}", userId.value());
    }

    public void unsubscribe(UserId userId) {
        Disposable subscription = subscriptions.remove(userId.value());
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
            log.debug("Redis Stream 구독 해제 userId={}", userId.value());
        }
    }

    public Mono<String> appendNotification(UserId userId, SseNotificationResponse payload) {
        String stream = NOTIFICATION_STREAM_PREFIX + userId.value();
        String json = serialize(payload);
        return reactiveRedisTemplate.opsForStream()
                .add(MapRecord.create(stream, Map.of(PAYLOAD_FIELD, json)))
                .map(recordId -> {
                    log.debug("Redis Stream 알림 적재 userId={} id={}", userId.value(), recordId);
                    return recordId.getValue();
                });
    }

    public Mono<String> appendLogout(UserId userId) {
        String stream = LOGOUT_STREAM_PREFIX + userId.value();
        return reactiveRedisTemplate.opsForStream()
                .add(MapRecord.create(stream, Map.of(PAYLOAD_FIELD, "logout")))
                .map(recordId -> {
                    log.debug("Redis Stream 로그아웃 적재 userId={} id={}", userId.value(), recordId);
                    return recordId.getValue();
                });
    }

    private Disposable pollStream(UserId userId, String notificationStream, String logoutStream) {
        return readLatest(notificationStream)
                .mergeWith(readLatest(logoutStream))
                .repeat()
                .flatMap(record -> handleRecord(userId, record))
                .subscribe(
                        unused -> {},
                        error -> log.error("Redis Stream 처리 오류 userId={}", userId.value(), error)
                );
    }

    private Flux<MapRecord<String, Object, Object>> readLatest(String stream) {
        return reactiveRedisTemplate.opsForStream()
                .read(StreamOffset.create(stream, ReadOffset.lastConsumed()))
                .onErrorResume(e -> {
                    log.warn("Redis Stream 읽기 오류 stream={}: {}", stream, e.getMessage());
                    return Flux.empty();
                });
    }

    private Mono<Void> handleRecord(UserId userId, MapRecord<String, Object, Object> record) {
        String stream = record.getStream();
        String payload = String.valueOf(record.getValue().get(PAYLOAD_FIELD));

        if (stream.startsWith(LOGOUT_STREAM_PREFIX)) {
            if (sinkRegistry.contains(userId)) {
                log.debug("로그아웃 스트림 수신, Sink 종료 userId={}", userId.value());
                sinkRegistry.remove(userId);
            }
            return Mono.empty();
        }

        if (stream.startsWith(NOTIFICATION_STREAM_PREFIX)) {
            if (!sinkRegistry.contains(userId)) {
                return Mono.empty();
            }
            SseNotificationResponse response = deserialize(payload);
            if (response != null) {
                log.debug("알림 스트림 수신, Sink emit userId={}", userId.value());
                sinkRegistry.emit(userId, response);
            }
            return Mono.empty();
        }

        return Mono.empty();
    }

    private String serialize(SseNotificationResponse payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.error("SseNotificationResponse 직렬화 실패", e);
            return "{}";
        }
    }

    private SseNotificationResponse deserialize(String message) {
        try {
            return objectMapper.readValue(message, SseNotificationResponse.class);
        } catch (Exception e) {
            log.error("SseNotificationResponse 역직렬화 실패: {}", message, e);
            return null;
        }
    }
}
