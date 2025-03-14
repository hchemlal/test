import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
public class HealthCheckService {
    
    private final MongoTemplate mongoTemplate;
    private final LocalDateTime startupTime;
    private final String appName = "my-springboot-app";
    
    public HealthCheckService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.startupTime = LocalDateTime.now();
    }

    public HealthStatus checkHealth() {
        HealthStatus health = new HealthStatus();
        health.setAppName(appName);
        health.setTimestamp(LocalDateTime.now());
        
        Map<String, Object> components = new HashMap<>();
        
        // 1. Application self-status
        components.put("application", checkApplicationHealth());
        
        // 2. MongoDB connectivity check
        components.put("mongodb", checkMongoDBHealth());
        
        // Determine overall status
        boolean allUp = components.values().stream()
                .allMatch(status -> ((Map)status).get("status").equals("UP"));
        
        health.setStatus(allUp ? "UP" : "DOWN");
        health.setDetails(components);
        
        return health;
    }

    private Map<String, Object> checkApplicationHealth() {
        Map<String, Object> appHealth = new HashMap<>();
        appHealth.put("status", "UP");
        appHealth.put("uptime", formatUptime());
        appHealth.put("version", "1.0.0"); // Add actual version from properties
        return appHealth;
    }

    private Map<String, Object> checkMongoDBHealth() {
        Map<String, Object> mongoHealth = new HashMap<>();
        
        try {
            // Simple MongoDB ping command
            mongoTemplate.getDb().runCommand(new Document("ping", 1));
            mongoHealth.put("status", "UP");
            mongoHealth.put("server", mongoTemplate.getDb().getName());
            mongoHealth.put("pingTime", System.currentTimeMillis());
        } catch (Exception e) {
            mongoHealth.put("status", "DOWN");
            mongoHealth.put("error", e.getMessage());
        }
        
        return mongoHealth;
    }

    private String formatUptime() {
        Duration duration = Duration.between(startupTime, LocalDateTime.now());
        return String.format("%dd %dh %dm %ds", 
            duration.toDays(),
            duration.toHours() % 24,
            duration.toMinutes() % 60,
            duration.getSeconds() % 60);
    }
}
