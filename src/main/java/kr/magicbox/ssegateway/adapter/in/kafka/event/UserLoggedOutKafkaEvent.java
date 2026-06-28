package kr.magicbox.ssegateway.adapter.in.kafka.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.time.Instant;

@Builder
public record UserLoggedOutKafkaEvent(
        @JsonProperty("user_id") Long userId,
        @JsonProperty("occurred_at") Instant occurredAt
) implements InboxEvent {}
