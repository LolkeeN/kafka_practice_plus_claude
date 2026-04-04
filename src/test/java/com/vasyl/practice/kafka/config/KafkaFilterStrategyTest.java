package com.vasyl.practice.kafka.config;

import com.vasyl.practice.kafka.dto.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaFilterStrategyTest {

    // Mirrors the filter strategy defined in KafkaConfig.filterKafkaListenerContainerFactory()
    // Returns true (skip/filter out) when balance < 10,000
    private final RecordFilterStrategy<String, User> filterStrategy =
            record -> record.value().getBalance().compareTo(BigDecimal.valueOf(10_000)) < 0;

    private ConsumerRecord<String, User> recordWithBalance(BigDecimal balance) {
        User user = new User();
        user.setBalance(balance);
        return new ConsumerRecord<>("user-event", 0, 0L, "key", user);
    }

    @Test
    void filter_balanceStrictlyBelowThreshold_returnsTrue() {
        assertThat(filterStrategy.filter(recordWithBalance(BigDecimal.valueOf(9_999)))).isTrue();
    }

    @Test
    void filter_balanceExactlyAtThreshold_returnsFalse() {
        assertThat(filterStrategy.filter(recordWithBalance(BigDecimal.valueOf(10_000)))).isFalse();
    }

    @Test
    void filter_balanceAboveThreshold_returnsFalse() {
        assertThat(filterStrategy.filter(recordWithBalance(BigDecimal.valueOf(50_000)))).isFalse();
    }

    @Test
    void filter_zeroBalance_returnsTrue() {
        assertThat(filterStrategy.filter(recordWithBalance(BigDecimal.ZERO))).isTrue();
    }

    @Test
    void filter_balanceJustBelowThreshold_returnsTrue() {
        assertThat(filterStrategy.filter(recordWithBalance(new BigDecimal("9999.99")))).isTrue();
    }

    @Test
    void filter_balanceJustAboveThreshold_returnsFalse() {
        assertThat(filterStrategy.filter(recordWithBalance(new BigDecimal("10000.01")))).isFalse();
    }
}
