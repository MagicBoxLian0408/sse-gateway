package kr.magicbox.ssegateway.adapter.in.kafka.event;

import lombok.Builder;

@Builder
public record PurchaseTokenIssuedKafkaEvent(
        Long releaseId,
        Long userId,
        String purchaseToken
) {}
