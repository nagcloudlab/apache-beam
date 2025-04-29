package com.example.day2.json.model;

import com.example.day2.json.serde.CustomDateSerilizer;
import com.example.day2.json.serde.CustomDeserilizer;
import com.example.day2.json.views.Views;
import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;

import java.util.Date;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Employee {
    @JsonView(Views.Public.class)
    @JsonProperty("full_name")
    public String fullName;
    @JsonView(Views.Public.class)
    @JsonProperty("age")
    public int age;
    //@JsonIgnore
    @JsonView(Views.Internal.class)
    public double salary;
    @JsonView(Views.Public.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Address address;
    @JsonProperty("date_of_birth")
    @JsonView(Views.Public.class)
    //@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    @JsonSerialize(using = CustomDateSerilizer.class)
    @JsonDeserialize(using = CustomDeserilizer.class)
    public Date dateOfBirth;
}
