package com.vasyl.practice.kafka.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class User {
    private String username;
    private String realName;
    private int age;
    private BigDecimal balance;
}
