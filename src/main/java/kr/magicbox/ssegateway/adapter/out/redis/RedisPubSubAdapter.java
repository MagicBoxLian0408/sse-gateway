package kr.magicbox.ssegateway.adapter.out.redis;

import tools.jackson.databind.ObjectMapper;
import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.cache.SseSinkRegistry;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Pub/Sub 기반 SSE 알림 어댑터.
 *
 * 채널 명명 규칙:
 *   - 구매 토큰 알림: sse:notification:{userId}
 *   - 로그아웃:       sse:logout:{userId}
 *
 * 클라이언트가 SSE에 연결되면 해당 userId의 채널을 subscribe하고,
 * 연결이 끊기면 구독을 해제한다.
 * 모든 인스턴스가 동일한 채널을 구독하지만, 로컬 Sink가 있는 인스턴스만 emit한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisPubSubAdapter {

    private static final String NOTIFICATION_CHANNEL_PREFIX = "sse:notification:";
    private static final String LOGOUT_CHANNEL_PREFIX = "sse:logout:";

    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final ReactiveStringRedisTemplate redisTemplate;
    private final SseSinkRegistry sinkRegistry;
    private final ObjectMapper objectMapper;

    private final Map<Long, Disposable> subscriptions = new ConcurrentHashMap<>();

    public void subscribe(UserId userId) {
        String notificationChannel = NOTIFICATION_CHANNEL_PREFIX + userId.value();
        String logoutChannel = LOGOUT_CHANNEL_PREFIX + userId.value();

        log.info("[REDIS-PUBSUB] 채널 구독 시작 userId={} channels=[{}, {}]",
                userId.value(), notificationChannel, logoutChannel);

        Disposable subscription = listenerContainer
                .receive(ChannelTopic.of(notificationChannel), ChannelTopic.of(logoutChannel))
                .flatMap(message -> handleMessage(userId, message))
                .subscribe(
                        unused -> {},
                        error -> log.error("[REDIS-PUBSUB] 메시지 처리 오류 userId={}", userId.value(), error)
                );

        subscriptions.put(userId.value(), subscription);
    }

    public void unsubscribe(UserId userId) {
        Disposable subscription = subscriptions.remove(userId.value());
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
            log.info("[REDIS-PUBSUB] 채널 구독 해제 userId={}", userId.value());
        }
    }

    public Mono<Long> publishNotification(UserId userId, SseNotificationResponse payload) {
        String channel = NOTIFICATION_CHANNEL_PREFIX + userId.value();
        String message = serialize(payload);
        log.info("[REDIS-PUBSUB] PUBLISH 시도 userId={} channel={}", userId.value(), channel);
        return redisTemplate.convertAndSend(channel, message)
                .doOnNext(count -> log.info("[REDIS-PUBSUB] PUBLISH 완료 userId={} channel={} receivers={}", userId.value(), channel, count));
    }

    public Mono<Long> publishLogout(UserId userId) {
        String channel = LOGOUT_CHANNEL_PREFIX + userId.value();
        return redisTemplate.convertAndSend(channel, "logout")
                .doOnNext(count -> log.debug("Redis 로그아웃 발행 userId={} channel={} receivers={}", userId.value(), channel, count));
    }

    private Mono<Void> handleMessage(UserId userId, ReactiveSubscription.Message<String, String> message) {
        String channel = message.getChannel();
        log.info("[REDIS-PUBSUB] 메시지 수신 userId={} channel={}", userId.value(), channel);

        if (channel.startsWith(LOGOUT_CHANNEL_PREFIX)) {
            if (sinkRegistry.contains(userId)) {
                log.info("[REDIS-PUBSUB] 로그아웃 메시지 수신 → Sink 종료 userId={}", userId.value());
                sinkRegistry.remove(userId);
            }
            return Mono.empty();
        }

        if (channel.startsWith(NOTIFICATION_CHANNEL_PREFIX)) {
            if (!sinkRegistry.contains(userId)) {
                log.warn("[REDIS-PUBSUB] Sink 없음 → 메시지 DROP userId={} (SSE 미연결 또는 이미 완료)", userId.value());
                return Mono.empty();
            }
            SseNotificationResponse payload = deserialize(message.getMessage());
            if (payload != null) {
                log.info("[REDIS-PUBSUB] Sink emit 시도 userId={}", userId.value());
                sinkRegistry.emit(userId, payload);
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
