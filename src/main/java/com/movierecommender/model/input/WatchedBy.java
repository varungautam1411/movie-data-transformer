// src/main/java/com/movierecommender/model/input/WatchedBy.java
package com.movierecommender.model.input;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WatchedBy {
    @JsonProperty("customer-id")
    private String customerId;
    @JsonProperty("movie-id")
    private String movieId;
    private int rating;
    private String date;

    // Getters and Setters
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
