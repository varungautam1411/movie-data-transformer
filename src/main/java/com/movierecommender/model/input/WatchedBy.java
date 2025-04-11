@Data
@NoArgsConstructor
@AllArgsConstructor
class WatchedBy {
    @JsonProperty("customer-id")
    private String customerId;
    @JsonProperty("movie-id")
    private String movieId;
    private int rating;
    private String date;
}

