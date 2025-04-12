package com.movierecommender.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.movierecommender.model.input.MovieInput;
import com.movierecommender.model.output.CustomerMovie;
import com.movierecommender.model.output.WatchedMovie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class DataTransformationService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransformationService.class);
    private static final int BATCH_SIZE = 10;
    private static final int MAX_RETRIES = 3;

    @Autowired
    private AmazonS3 s3Client;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Value("${s3.bucket.name}")
    private String bucketName;

    @Value("${s3.file.prefix}")
    private String filePrefix;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executorService = Executors.newFixedThreadPool(BATCH_SIZE);

    @PostConstruct
    public void processData() {
        try {
            logger.info("Starting data transformation process");
            
            // Get all S3 objects
            List<S3ObjectSummary> allObjects = listAllS3Objects();
            logger.info("Found {} files to process", allObjects.size());

            // Process files in batches
            for (int i = 0; i < allObjects.size(); i += BATCH_SIZE) {
                int endIndex = Math.min(i + BATCH_SIZE, allObjects.size());
                List<S3ObjectSummary> batchObjects = allObjects.subList(i, endIndex);
                
                processBatch(batchObjects);
                
                logger.info("Completed batch processing for files {}-{} of {}", 
                    i + 1, endIndex, allObjects.size());
            }

            executorService.shutdown();
            logger.info("Data transformation completed successfully");

        } catch (Exception e) {
            logger.error("Error during data transformation: ", e);
        }
    }

    private List<S3ObjectSummary> listAllS3Objects() {
        List<S3ObjectSummary> allObjects = new ArrayList<>();
        ListObjectsV2Request listRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(filePrefix);

        ListObjectsV2Result listing;
        do {
            listing = s3Client.listObjectsV2(listRequest);
            allObjects.addAll(listing.getObjectSummaries().stream()
                    .filter(obj -> obj.getKey().endsWith(".json"))
                    .collect(Collectors.toList()));
            listRequest.setContinuationToken(listing.getNextContinuationToken());
        } while (listing.isTruncated());

        return allObjects;
    }

    private void processBatch(List<S3ObjectSummary> batchObjects) {
        Map<String, CustomerMovie> batchCustomerMovieMap = Collections.synchronizedMap(new HashMap<>());
        
        // Process each file in the batch concurrently
        List<CompletableFuture<Void>> futures = batchObjects.stream()
                .map(obj -> CompletableFuture.runAsync(() -> 
                    processFileWithRetry(obj.getKey(), batchCustomerMovieMap), executorService))
                .collect(Collectors.toList());

        // Wait for all files in batch to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Save batch results to MemoryDB
        saveToMemoryDB(batchCustomerMovieMap);
    }

    private void processFileWithRetry(String key, Map<String, CustomerMovie> customerMovieMap) {
        int attempts = 0;
        boolean success = false;
        
        while (!success && attempts < MAX_RETRIES) {
            attempts++;
            try {
                processFile(key, customerMovieMap);
                success = true;
                logger.info("Successfully processed file: {} on attempt {}", key, attempts);
            } catch (Exception e) {
                logger.error("Error processing file {} on attempt {}: ", key, attempts, e);
                if (attempts == MAX_RETRIES) {
                    logger.error("Failed to process file {} after {} attempts", key, MAX_RETRIES);
                }
            }
        }
    }

    private void processFile(String key, Map<String, CustomerMovie> customerMovieMap) {
        try (S3Object s3Object = s3Client.getObject(bucketName, key);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                MovieInput movieInput = objectMapper.readValue(line, MovieInput.class);
                processMovieInput(movieInput, customerMovieMap);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error processing file: " + key, e);
        }
    }

    private void processMovieInput(MovieInput movieInput, Map<String, CustomerMovie> customerMovieMap) {
        movieInput.getWatchedBy().forEach(watchedBy -> {
            CustomerMovie customerMovie = customerMovieMap.computeIfAbsent(
                    watchedBy.getCustomerId(),
                    k -> {
                        CustomerMovie cm = new CustomerMovie();
                        cm.setCustomerId(k);
                        cm.setWatchedMovies(Collections.synchronizedList(new ArrayList<>()));
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
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);

        logger.info("Saving batch of {} customer records to MemoryDB", customerMovieMap.size());
        
        customerMovieMap.forEach((customerId, newCustomerMovie) -> {
            try {
                String key = "customer:" + customerId;
                // Try to get existing customer data
                String existingValue = redisTemplate.opsForValue().get(key);
                
                if (existingValue != null) {
                    logger.info("This customer has already rated a movie "+existingValue);
                    // Merge existing and new data
                    CustomerMovie existingCustomerMovie = objectMapper.readValue(existingValue, CustomerMovie.class);
                    mergeCustomerMovies(existingCustomerMovie, newCustomerMovie);
                    String mergedValue = objectMapper.writeValueAsString(existingCustomerMovie);
                    redisTemplate.opsForValue().set(key, mergedValue);
                    updateCount.incrementAndGet();
                } else {
                    // New customer, just save the new data
                    String value = objectMapper.writeValueAsString(newCustomerMovie);
                    redisTemplate.opsForValue().set(key, value);
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.error("Error saving customer {} to MemoryDB: ", customerId, e);
            }
        });

        logger.info("Batch save completed. New: {}, Updates: {}, Failures: {}", 
            successCount.get(), updateCount.get(), failureCount.get());
    }

    private void mergeCustomerMovies(CustomerMovie existing, CustomerMovie newData) {
        Map<String, WatchedMovie> movieMap = new HashMap<>();

        // Add existing movies to map
        for (WatchedMovie movie : existing.getWatchedMovies()) {
            String movieKey = createMovieKey(movie);
            movieMap.put(movieKey, movie);
        }

        // Update or add new movies
        for (WatchedMovie newMovie : newData.getWatchedMovies()) {
            String movieKey = createMovieKey(newMovie);
            WatchedMovie existingMovie = movieMap.get(movieKey);
            
            if (existingMovie != null) {
                // Update existing movie if the new rating date is more recent
                if (isMoreRecent(newMovie.getDate(), existingMovie.getDate())) {
                    movieMap.put(movieKey, newMovie);
                }
            } else {
                // Add new movie
                movieMap.put(movieKey, newMovie);
            }
        }

        // Update the existing customer's movie list with merged data
        existing.setWatchedMovies(new ArrayList<>(movieMap.values()));
    }

    private String createMovieKey(WatchedMovie movie) {
        // Create a unique key for each movie using movieId
        return movie.getMovieId();
    }

    private boolean isMoreRecent(String newDate, String existingDate) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date1 = dateFormat.parse(newDate);
            Date date2 = dateFormat.parse(existingDate);
            return date1.after(date2);
        } catch (ParseException e) {
            logger.error("Error parsing dates: {} or {}", newDate, existingDate);
            return false;
        }
    }

  
}
