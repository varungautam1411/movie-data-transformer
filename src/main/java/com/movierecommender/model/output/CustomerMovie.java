@Data
@NoArgsConstructor
@AllArgsConstructor
class CustomerMovie {
    private String customerId;
    private List<WatchedMovie> watchedMovies;
}
