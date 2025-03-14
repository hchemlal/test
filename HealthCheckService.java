@Service
public class HealthCheckService {
    @Value("${app.health.topic}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, HealthStatus> kafkaTemplate;

    // Simple health check logic
    public HealthStatus checkHealth() {
        HealthStatus health = new HealthStatus();
        health.setAppName("my-springboot-app");
        health.setStatus("UP"); // Replace with actual checks
        health.setTimestamp(LocalDateTime.now());
        health.setDetails(Map.of("database", "connected", "diskSpace", "95% free"));
        return health;
    }

    public void publishHealthStatus() {
        HealthStatus health = checkHealth();
        kafkaTemplate.send(topicName, health);
    }
}
