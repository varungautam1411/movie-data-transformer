// Configuration classes
@Configuration
public class S3Config {
  
    
    @Value("${aws.region}")
    private String region;
    
    @Bean
    public AmazonS3 s3Client() {
        return AmazonS3ClientBuilder.standard().withRegion(region)
            .build();
    }
}
