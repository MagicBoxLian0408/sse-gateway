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

        Disposable subscription = listenerContainer
                .receive(ChannelTopic.of(notificationChannel), ChannelTopic.of(logoutChannel))
                .flatMap(message -> handleMessage(userId, message))
                .subscribe(
                        unused -> {},
                        error -> log.error("Redis 메시지 처리 오류 userId={}", userId.value(), error)
                );

        subscriptions.put(userId.value(), subscription);
        log.debug("Redis 채널 구독 등록 userId={}", userId.value());
    }

    public void unsubscribe(UserId userId) {
        Disposable subscription = subscriptions.remove(userId.value());
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
            log.debug("Redis 채널 구독 해제 userId={}", userId.value());
        }
    }

    public Mono<Long> publishNotification(UserId userId, SseNotificationResponse payload) {
        String channel = NOTIFICATION_CHANNEL_PREFIX + userId.value();
        String message = serialize(payload);
        return redisTemplate.convertAndSend(channel, message)
                .doOnNext(count -> log.debug("Redis 알림 발행 userId={} channel={} receivers={}", userId.value(), channel, count));
    }

    public Mono<Long> publishLogout(UserId userId) {
        String channel = LOGOUT_CHANNEL_PREFIX + userId.value();
        return redisTemplate.convertAndSend(channel, "logout")
                .doOnNext(count -> log.debug("Redis 로그아웃 발행 userId={} channel={} receivers={}", userId.value(), channel, count));
    }

    private Mono<Void> handleMessage(UserId userId, ReactiveSubscription.Message<String, String> message) {
        String channel = message.getChannel();

        if (channel.startsWith(LOGOUT_CHANNEL_PREFIX)) {
            if (sinkRegistry.contains(userId)) {
                log.debug("로그아웃 메시지 수신, Sink 종료 userId={}", userId.value());
                sinkRegistry.remove(userId);
            }
            return Mono.empty();
        }

        if (channel.startsWith(NOTIFICATION_CHANNEL_PREFIX)) {
            if (!sinkRegistry.contains(userId)) {
                return Mono.empty();
            }
            SseNotificationResponse payload = deserialize(message.getMessage());
            if (payload != null) {
                log.debug("알림 메시지 수신, Sink emit userId={}", userId.value());
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
