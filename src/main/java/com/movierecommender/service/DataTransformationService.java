// Service class
@Service
@Slf4j
public class DataTransformationService {
    @Autowired
    private AmazonS3 s3Client;
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Value("${s3.bucket.name}")
    private String bucketName;
    
    @Value("${s3.file.key}")
    private String fileKey;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @PostConstruct
    public void transformData() {
        try {
            // Read data from S3
            S3Object s3Object = s3Client.getObject(bucketName, fileKey);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            
            Map<String, CustomerMovie> customerMovieMap = new HashMap<>();
            
            String line;
            while ((line = reader.readLine()) != null) {
                MovieInput movieInput = objectMapper.readValue(line, MovieInput.class);
                processMovie(movieInput, customerMovieMap);
            }
            
            // Write to MemoryDB
            writeToMemoryDB(customerMovieMap);
            
        } catch (Exception e) {
            log.error("Error processing data: ", e);
        }
    }
    
    private void processMovie(MovieInput movieInput, Map<String, CustomerMovie> customerMovieMap) {
        for (WatchedBy watch : movieInput.getWatchedBy()) {
            CustomerMovie customerMovie = customerMovieMap.computeIfAbsent(
                watch.getCustomerId(),
                k -> new CustomerMovie(k, new ArrayList<>())
            );
            
            WatchedMovie watchedMovie = new WatchedMovie(
                movieInput.getMovieId(),
                movieInput.getTitle(),
                movieInput.getYearOfRelease(),
                watch.getRating(),
                watch.getDate()
            );
            
            customerMovie.getWatchedMovies().add(watchedMovie);
        }
    }
    
    private void writeToMemoryDB(Map<String, CustomerMovie> customerMovieMap) {
        customerMovieMap.forEach((customerId, customerMovie) -> {
            try {
                String key = "customer:" + customerId;
                String value = objectMapper.writeValueAsString(customerMovie);
                redisTemplate.opsForValue().set(key, value);
                log.info("Written customer data for: {}", customerId);
            } catch (JsonProcessingException e) {
                log.error("Error writing customer data for {}: ", customerId, e);
            }
        });
    }
}
