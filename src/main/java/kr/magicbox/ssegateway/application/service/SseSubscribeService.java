package kr.magicbox.ssegateway.application.service;

import kr.magicbox.ssegateway.adapter.in.web.dto.response.SseNotificationResponse;
import kr.magicbox.ssegateway.adapter.out.cache.SseSinkRegistry;
import kr.magicbox.ssegateway.adapter.out.kafka.SseEventKafkaAdapter;
import kr.magicbox.ssegateway.adapter.out.redis.RedisPubSubAdapter;
import kr.magicbox.ssegateway.application.port.in.SubscribeSseUseCase;
import kr.magicbox.ssegateway.domain.vo.UserId;
import lombok.RequiredArgsConstructor;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

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
        Sinks.One<SseNotificationResponse> sink = sinkRegistry.register(userId);
        redisPubSubAdapter.subscribe(userId);
        sseEventKafkaAdapter.publishConnected(userId);

        Flux<ServerSentEvent<SseNotificationResponse>> notificationStream = sink.asMono()
                .map(payload -> ServerSentEvent.<SseNotificationResponse>builder()
                        .event("notification")
                        .data(payload)
                        .build())
                .flux()
                .doFinally(signal -> {
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

        return Flux.merge(notificationStream, heartbeat)
                .takeUntilOther(Mono.delay(Duration.ofMinutes(MAX_CONNECTION_MINUTES)));
    }
}
