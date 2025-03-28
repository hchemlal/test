import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import java.util.Date;
import java.util.Map;

@Document(collection = "api_requests")
public class ExternalRequestDocument {
    @Id
    private String id;
    
    @Indexed(unique = true)
    private String requestId;
    
    private Date timestamp = new Date();
    private String status = "pending";
    private String endpoint;
    private Map<String, Object> payload;

    // Constructors, getters, setters
    public ApiRequest() {}
    
    public ApiRequest(String requestId, String status, String endpoint, Map<String, Object> payload) {
        this.requestId = requestId;
        this.status = status;
        this.endpoint = endpoint;
        this.payload = payload;
    }
    
    // Getters and setters for all fields
}
