package com.movierecommender.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import // ... other imports

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
    public void transformData() {
        try {
            Map<String, CustomerMovie> customerMovieMap = new HashMap<>();
            
            // List all JSON files in the bucket with the specified prefix
            ListObjectsV2Request listRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(filePrefix);
            
            ListObjectsV2Result listing;
            do {
                listing = s3Client.listObjectsV2(listRequest);
                processS3Objects(listing.getObjectSummaries(), customerMovieMap);
                listRequest.setContinuationToken(listing.getNextContinuationToken());
            } while (listing.isTruncated());
            
            // Write to MemoryDB
            writeToMemoryDB(customerMovieMap);
            
        } catch (Exception e) {
            log.error("Error processing data: ", e);
            throw new DataProcessingException("Failed to process S3 files", e);
        }
    }
    
    private void processS3Objects(List<S3ObjectSummary> objectSummaries, 
                                Map<String, CustomerMovie> customerMovieMap) {
        for (S3ObjectSummary summary : objectSummaries) {
            String key = summary.getKey();
            if (!key.endsWith(".json")) {
                continue;
            }
            
            try {
                processS3File(key, customerMovieMap);
                log.info("Successfully processed file: {}", key);
            } catch (Exception e) {
                log.error("Error processing file {}: ", key, e);
            }
        }
    }
    
    private void processS3File(String key, Map<String, CustomerMovie> customerMovieMap) {
        try (S3Object s3Object = s3Client.getObject(bucketName, key);
             BufferedReader reader = new BufferedReader(
                 new InputStreamReader(s3Object.getObjectContent()))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    MovieInput movieInput = objectMapper.readValue(line, MovieInput.class);
                    processMovie(movieInput, customerMovieMap);
                } catch (JsonProcessingException e) {
                    log.error("Error parsing JSON line in file {}: {}", key, line, e);
                }
            }
        } catch (IOException e) {
            throw new DataProcessingException(
                String.format("Failed to process file %s", key), e);
        }
    }
    
    // Add batch processing capability
    private void writeToMemoryDB(Map<String, CustomerMovie> customerMovieMap) {
        List<String> failures = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        
        customerMovieMap.forEach((customerId, customerMovie) -> {
            try {
                String key = "customer:" + customerId;
                String value = objectMapper.writeValueAsString(customerMovie);
                redisTemplate.opsForValue().set(key, value);
                
                int processed = counter.incrementAndGet();
                if (processed % 1000 == 0) {
                    log.info("Processed {} customer records", processed);
                }
            } catch (JsonProcessingException e) {
                failures.add(customerId);
                log.error("Error writing customer data for {}: ", customerId, e);
            }
        });
        
        log.info("Completed processing {} customer records", counter.get());
        if (!failures.isEmpty()) {
            log.error("Failed to process {} customers: {}", 
                failures.size(), String.join(", ", failures));
        }
    }
}
