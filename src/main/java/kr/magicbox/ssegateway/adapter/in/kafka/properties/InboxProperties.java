package kr.magicbox.ssegateway.adapter.in.kafka.properties;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "inbox")
public class InboxProperties {
    private final long maxEventAgeMinutes;
}
