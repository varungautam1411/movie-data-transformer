// src/main/java/com/movierecommender/model/input/MovieInput.java
package com.movierecommender.model.input;

import java.util.List;

public class MovieInput {
    private List<WatchedBy> watchedBy;
    private String title;
    private String movieId;
    private int yearOfRelease;

    // Getters and Setters
    public List<WatchedBy> getWatchedBy() {
        return watchedBy;
    }

    public void setWatchedBy(List<WatchedBy> watchedBy) {
        this.watchedBy = watchedBy;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public int getYearOfRelease() {
        return yearOfRelease;
    }

    public void setYearOfRelease(int yearOfRelease) {
        this.yearOfRelease = yearOfRelease;
    }
}
