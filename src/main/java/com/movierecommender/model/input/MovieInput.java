// Model classes
@Data
@NoArgsConstructor
@AllArgsConstructor
class MovieInput {
    private List<WatchedBy> watchedBy;
    private String title;
    private String movieId;
    private int yearOfRelease;
}
