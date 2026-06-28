package kr.magicbox.ssegateway.adapter.in.kafka.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.time.Instant;

@Builder
public record PurchaseTokenIssuedKafkaEvent(
        @JsonProperty("release_id") Long releaseId,
        @JsonProperty("user_id") Long userId,
        @JsonProperty("purchase_token") String purchaseToken,
        @JsonProperty("occurred_at") Instant occurredAt
) implements InboxEvent {}
