// src/main/java/com/movierecommender/model/output/CustomerMovie.java
package com.movierecommender.model.output;

import java.util.List;

public class CustomerMovie {
    private String customerId;
    private List<WatchedMovie> watchedMovies;

    // Getters and Setters
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public List<WatchedMovie> getWatchedMovies() {
        return watchedMovies;
    }

    public void setWatchedMovies(List<WatchedMovie> watchedMovies) {
        this.watchedMovies = watchedMovies;
    }
}
