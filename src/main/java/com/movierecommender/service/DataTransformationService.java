// src/main/java/com/movierecommender/service/DataTransformationService.java
package com.movierecommender.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.movierecommender.model.input.MovieInput;
import com.movierecommender.model.output.CustomerMovie;
import com.movierecommender.model.output.WatchedMovie;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class DataTransformationService {

    @Autowired
    private AmazonS3 s3Client;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Value("${s3.bucket.name}")
    private String bucketName;

    @Value("${s3.file.prefix}")
    private String filePrefix;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void processData() {
        try {
            log.info("Starting data transformation process");
            Map<String, CustomerMovie> customerMovieMap = new HashMap<>();

            ListObjectsV2Request listRequest = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(filePrefix);

            ListObjectsV2Result listing;
            do {
                listing = s3Client.listObjectsV2(listRequest);
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    if (summary.getKey().endsWith(".json")) {
                        processFile(summary.getKey(), customerMovieMap);
                    }
                }
                listRequest.setContinuationToken(listing.getNextContinuationToken());
            } while (listing.isTruncated());

            saveToMemoryDB(customerMovieMap);
            log.info("Data transformation completed successfully");

        } catch (Exception e) {
            log.error("Error during data transformation: ", e);
        }
    }

    private void processFile(String key, Map<String, CustomerMovie> customerMovieMap) {
        log.info("Processing file: {}", key);
        try (S3Object s3Object = s3Client.getObject(bucketName, key);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                MovieInput movieInput = objectMapper.readValue(line, MovieInput.class);
                processMovieInput(movieInput, customerMovieMap);
            }

        } catch (Exception e) {
            log.error("Error processing file {}: ", key, e);
        }
    }

    private void processMovieInput(MovieInput movieInput, Map<String, CustomerMovie> customerMovieMap) {
        movieInput.getWatchedBy().forEach(watchedBy -> {
            CustomerMovie customerMovie = customerMovieMap.computeIfAbsent(
                    watchedBy.getCustomerId(),
                    k -> {
                        CustomerMovie cm = new CustomerMovie();
                        cm.setCustomerId(k);
                        cm.setWatchedMovies(new ArrayList<>());
                        return cm;
                    }
            );

            WatchedMovie watchedMovie = new WatchedMovie();
            watchedMovie.setMovieId(movieInput.getMovieId());
            watchedMovie.setTitle(movieInput.getTitle());
            watchedMovie.setYearOfRelease(movieInput.getYearOfRelease());
            watchedMovie.setRating(watchedBy.getRating());
            watchedMovie.setDate(watchedBy.getDate());

            customerMovie.getWatchedMovies().add(watchedMovie);
        });
    }

    private void saveToMemoryDB(Map<String, CustomerMovie> customerMovieMap) {
        log.info("Saving {} customer records to MemoryDB", customerMovieMap.size());
        customerMovieMap.forEach((customerId, customerMovie) -> {
            try {
                String key = "customer:" + customerId;
                String value = objectMapper.writeValueAsString(customerMovie);
                redisTemplate.opsForValue().set(key, value);
            } catch (Exception e) {
                log.error("Error saving customer {} to MemoryDB: ", customerId, e);
            }
        });
    }
}
