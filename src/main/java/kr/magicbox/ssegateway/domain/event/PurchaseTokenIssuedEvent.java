package kr.magicbox.ssegateway.domain.event;

import lombok.Builder;

@Builder
public record PurchaseTokenIssuedEvent(
        Long releaseId,
        Long userId,
        String purchaseToken
) {}
