package kr.magicbox.ssegateway.adapter.in.kafka.aop;

import kr.magicbox.ssegateway.adapter.in.kafka.event.InboxEvent;
import kr.magicbox.ssegateway.adapter.in.kafka.properties.InboxProperties;
import kr.magicbox.ssegateway.adapter.out.persistence.entity.SseGatewayInboxEntity;
import kr.magicbox.ssegateway.adapter.out.persistence.entity.SseGatewayInboxStatus;
import kr.magicbox.ssegateway.adapter.out.persistence.repository.SseGatewayInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class IdempotentAspect {

    private final SseGatewayInboxRepository sseGatewayInboxRepository;
    private final InboxProperties inboxProperties;

    @Around("@annotation(kr.magicbox.ssegateway.adapter.in.kafka.annotation.Idempotent)")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        ConsumerRecord<String, ?> consumerRecord = extractRecord(pjp);
        String eventKey = consumerRecord.key();
        InboxEvent event = (InboxEvent) consumerRecord.value();
        Instant occurredAt = event.occurredAt();

        if (isTooOld(occurredAt)) {
            log.warn("[Inbox] 만료된 메시지 DEAD_LETTERED 처리. key={}, occurredAt={}", eventKey, occurredAt);
            sseGatewayInboxRepository.save(SseGatewayInboxEntity.builder()
                    .eventKey(eventKey)
                    .topic(consumerRecord.topic())
                    .partition(consumerRecord.partition())
                    .offset(consumerRecord.offset())
                    .status(SseGatewayInboxStatus.DEAD_LETTERED)
                    .occurredAt(occurredAt)
                    .build()).block();
            return null;
        }

        Boolean exists = sseGatewayInboxRepository.existsByEventKey(eventKey).block();
        if (Boolean.TRUE.equals(exists)) {
            log.warn("[Inbox] 중복 메시지 폐기. key={}", eventKey);
            return null;
        }

        sseGatewayInboxRepository.save(SseGatewayInboxEntity.builder()
                .eventKey(eventKey)
                .topic(consumerRecord.topic())
                .partition(consumerRecord.partition())
                .offset(consumerRecord.offset())
                .status(SseGatewayInboxStatus.PROCESSED)
                .occurredAt(occurredAt)
                .build()).block();

        pjp.proceed();
        return null;
    }

    private boolean isTooOld(Instant occurredAt) {
        return occurredAt.isBefore(Instant.now().minus(inboxProperties.getMaxEventAgeMinutes(), ChronoUnit.MINUTES));
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecord<String, ?> extractRecord(ProceedingJoinPoint pjp) {
        return Arrays.stream(pjp.getArgs())
                .filter(ConsumerRecord.class::isInstance)
                .map(arg -> (ConsumerRecord<String, ?>) arg)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("@Idempotent 메서드에 ConsumerRecord 파라미터가 없습니다."));
    }
}
