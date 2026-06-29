package kr.magicbox.ssegateway.adapter.out.persistence.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@Configuration
@EnableR2dbcAuditing
@EnableR2dbcRepositories(basePackages = "kr.magicbox.ssegateway.adapter.out.persistence.repository")
public class R2dbcConfiguration {
}
