import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.*;

public class KafkaHealthProbe {

    private final AdminClient adminClient;
    private final String bootstrapServers;
    private final int timeoutMs;

    public KafkaHealthProbe(String bootstrapServers, int timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.timeoutMs = timeoutMs;
        this.adminClient = createAdminClient();
    }

    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        props.put(AdminClientConfig.RETRIES_CONFIG, 3); // Retry transient errors
        return AdminClient.create(props);
    }

    public HealthStatus checkHealth() {
        HealthStatus status = new HealthStatus();
        
        try {
            // 1. Check basic connectivity and cluster description
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            status.controllerId = clusterResult.controller().get().id();
            status.nodes = clusterResult.nodes().get();
            
            // 2. Check broker configurations
            Collection<ConfigResource> resources = new ArrayList<>();
            for (Node node : status.nodes) {
                resources.add(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
            }
            status.configs = adminClient.describeConfigs(resources).all().get();
            
            // 3. Check topic metadata
            status.topics = adminClient.listTopics().names().get();
            
            // 4. Check for under-replicated partitions
            status.isHealthy = true;
            status.error = null;
            
        } catch (ExecutionException e) {
            handleException(status, e.getCause());
        } catch (InterruptedException e) {
            status.isHealthy = false;
            status.error = "Operation interrupted: " + e.getMessage();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            handleException(status, e);
        }
        
        return status;
    }

    private void handleException(HealthStatus status, Throwable t) {
        status.isHealthy = false;
        status.error = t.getClass().getSimpleName() + ": " + t.getMessage();
        
        if (t instanceof TimeoutException) {
            status.errorType = ErrorType.CONNECTION_TIMEOUT;
        } else if (t instanceof org.apache.kafka.common.errors.AuthenticationException) {
            status.errorType = ErrorType.AUTHENTICATION_FAILURE;
        } else {
            status.errorType = ErrorType.UNKNOWN;
        }
    }

    public void shutdown() {
        adminClient.close();
    }

    public static class HealthStatus {
        public boolean isHealthy;
        public String error;
        public ErrorType errorType;
        public int controllerId;
        public Collection<Node> nodes;
        public Map<ConfigResource, Config> configs;
        public Set<String> topics;
    }

    public enum ErrorType {
        CONNECTION_TIMEOUT,
        AUTHENTICATION_FAILURE,
        AUTHORIZATION_FAILURE,
        UNKNOWN
    }

    // Usage example
    public static void main(String[] args) throws Exception {
        KafkaHealthProbe probe = new KafkaHealthProbe("localhost:9092", 5000);
        
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            HealthStatus status = probe.checkHealth();
            System.out.println("Kafka Health: " + (status.isHealthy ? "HEALTHY" : "UNHEALTHY"));
            if (!status.isHealthy) {
                System.err.println("Error: " + status.error);
            }
        }, 0, 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            probe.shutdown();
        }));
    }
}
