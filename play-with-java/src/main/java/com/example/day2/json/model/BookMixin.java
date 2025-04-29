package com.example.day2.json.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class BookMixin {
    // This class is used to define mixin annotations for the Book class
    // It can be used to customize serialization/deserialization behavior
    // without modifying the original Book class

    // Example: @JsonIgnoreProperties({"yearPublished"})
    // This would ignore the yearPublished property during serialization/deserialization

    public String title;
    public String author;
    @JsonProperty("year_published")
    public int yearPublished;

}
