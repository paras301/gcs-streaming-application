package com.company.vo;

import lombok.Data;

@Data
public class Employee {
    private String name;
    private String department;
    private String salary;
    private String rating;
    private String period_of_service;
    private Double bonus;
}
