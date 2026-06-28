package kr.magicbox.ssegateway.adapter.out.persistence.repository;

import kr.magicbox.ssegateway.adapter.out.persistence.entity.SseGatewayInboxEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Mono;

public interface SseGatewayInboxRepository extends R2dbcRepository<SseGatewayInboxEntity, Long> {
    Mono<Boolean> existsByEventKey(String eventKey);
    Mono<SseGatewayInboxEntity> findByTopicAndPartitionAndOffset(String topic, Integer partition, Long offset);
}
