package com.example.com.example;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Employee {
    @JsonProperty("employee_name")
    private String name;
    @JsonInclude
    @JsonProperty("employee_email")
    private String email;
    @JsonFormat
    @JsonProperty("employee_dob")
    private String dob;
    
}
