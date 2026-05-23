package kr.magicbox.ssegateway.adapter.in.security.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "security.trusted")
public class TrustedIpProperties {

    private List<String> ips;
}
