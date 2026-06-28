package kr.magicbox.ssegateway.adapter.out.persistence.entity;

import lombok.Builder;
import lombok.Getter;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Getter
@Builder
@Table("sse_gateway_inbox")
public class SseGatewayInboxEntity {

    @Id
    private Long id;

    @Column("event_key")
    private String eventKey;

    @Column("topic")
    private String topic;

    @Column("kafka_partition")
    private Integer partition;

    @Column("kafka_offset")
    private Long offset;

    @Column("status")
    private SseGatewayInboxStatus status;

    @Column("occurred_at")
    private Instant occurredAt;

    @CreatedDate
    @Column("created_at")
    private Instant createdAt;

    @LastModifiedDate
    @Column("updated_at")
    private Instant updatedAt;

    public SseGatewayInboxEntity markDeadLettered() {
        return SseGatewayInboxEntity.builder()
                .id(this.id)
                .eventKey(this.eventKey)
                .topic(this.topic)
                .partition(this.partition)
                .offset(this.offset)
                .status(SseGatewayInboxStatus.DEAD_LETTERED)
                .occurredAt(this.occurredAt)
                .createdAt(this.createdAt)
                .updatedAt(this.updatedAt)
                .build();
    }
}
