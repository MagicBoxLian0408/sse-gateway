package kr.magicbox.ssegateway.adapter.in.kafka;

import kr.magicbox.ssegateway.adapter.in.kafka.properties.InboxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@EnableKafkaRetryTopic
@Configuration
@EnableConfigurationProperties(InboxProperties.class)
public class KafkaConfiguration {

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-retry-");
        return scheduler;
    }
}
