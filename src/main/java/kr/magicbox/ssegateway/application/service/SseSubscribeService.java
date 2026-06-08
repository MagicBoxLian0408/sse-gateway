package kr.magicbox.ssegateway.application.service;

import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.cache.SseSinkRegistry;
import kr.magicbox.ssegateway.adapter.out.kafka.SseEventKafkaAdapter;
import kr.magicbox.ssegateway.adapter.out.redis.RedisPubSubAdapter;
import kr.magicbox.ssegateway.application.port.in.SubscribeSseUseCase;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class SseSubscribeService implements SubscribeSseUseCase {

    private static final long HEARTBEAT_INTERVAL_SECONDS = 15;
    private static final long MAX_CONNECTION_MINUTES = 10;

    private final SseSinkRegistry sinkRegistry;
    private final SseEventKafkaAdapter sseEventKafkaAdapter;
    private final RedisPubSubAdapter redisPubSubAdapter;

    @Override
    public Flux<ServerSentEvent<SseNotificationResponse>> subscribe(UserId userId) {
        log.info("[SSE-SUBSCRIBE] SSE 연결 시작 userId={}", userId.value());
        Sinks.One<SseNotificationResponse> sink = sinkRegistry.register(userId);
        redisPubSubAdapter.subscribe(userId);
        sseEventKafkaAdapter.publishConnected(userId);

        Flux<ServerSentEvent<SseNotificationResponse>> connectedEvent = Flux.just(
                ServerSentEvent.<SseNotificationResponse>builder()
                        .event("connected")
                        .comment("SSE connection established")
                        .build()
        );

        Flux<ServerSentEvent<SseNotificationResponse>> notificationStream = sink.asMono()
                .doOnNext(payload -> log.info("[SSE-SUBSCRIBE] notification 전달 userId={} purchaseToken={}",
                        userId.value(), payload.purchaseToken()))
                .map(payload -> ServerSentEvent.<SseNotificationResponse>builder()
                        .event("notification")
                        .data(payload)
                        .build())
                .flux()
                .doFinally(signal -> {
                    log.info("[SSE-SUBSCRIBE] SSE 연결 종료 userId={} signal={}", userId.value(), signal);
                    redisPubSubAdapter.unsubscribe(userId);
                    sinkRegistry.remove(userId);
                    sseEventKafkaAdapter.publishDisconnected(userId);
                });

        Flux<ServerSentEvent<SseNotificationResponse>> heartbeat = Flux
                .interval(Duration.ofSeconds(HEARTBEAT_INTERVAL_SECONDS))
                .map(tick -> ServerSentEvent.<SseNotificationResponse>builder()
                        .event("heartbeat")
                        .comment("keep-alive")
                        .build());

        return Flux.concat(
                        connectedEvent,
                        Flux.merge(notificationStream, heartbeat)
                )
                .takeUntilOther(Mono.delay(Duration.ofMinutes(MAX_CONNECTION_MINUTES)));
    }
}
