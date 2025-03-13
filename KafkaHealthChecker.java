import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaHealthChecker {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int CHECK_INTERVAL_SEC = 5;
    private static final int REQUEST_TIMEOUT_MS = 3000;

    public static void main(String[] args) {
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Add shutdown hook to clean up resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            System.out.println("Scheduler shutdown complete");
        }));

        // Schedule periodic health checks
        scheduler.scheduleWithFixedDelay(
                KafkaHealthChecker::checkBrokerHealth,
                0,  // Initial delay
                CHECK_INTERVAL_SEC,
                TimeUnit.SECONDS
        );
    }

    private static void checkBrokerHealth() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);

        try (AdminClient admin = AdminClient.create(props)) {
            // Attempt a lightweight operation (listTopics with names only)
            admin.listTopics().names().get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.printf("[%s] Broker is reachable%n", System.currentTimeMillis());
        } catch (TimeoutException e) {
            System.err.printf("[%s] Broker unreachable (timeout): %s%n", 
                            System.currentTimeMillis(), e.getMessage());
        } catch (Exception e) {
            System.err.printf("[%s] Broker check failed: %s%n", 
                            System.currentTimeMillis(), e.getMessage());
        }
    }
}
