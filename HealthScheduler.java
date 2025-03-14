@Configuration
@EnableScheduling
public class HealthScheduler {
    @Autowired
    private HealthCheckService healthCheckService;

    @Scheduled(fixedRate = 60000) // Every 60 seconds
    public void scheduleHealthCheck() {
        healthCheckService.publishHealthStatus();
    }
}
