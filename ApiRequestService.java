import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import java.security.MessageDigest;
import java.util.*;

@Service
public class ApiRequestService {
    
    private final ApiRequestRepository repository;
    private final MongoTemplate mongoTemplate;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public ApiRequestService(ApiRequestRepository repository, MongoTemplate mongoTemplate) {
        this.repository = repository;
        this.mongoTemplate = mongoTemplate;
    }

    public String generateRequestId(Map<String, Object> requestParams) throws Exception {
        Map<String, Object> sortedParams = sortMapKeys(requestParams);
        String paramString = objectMapper.writeValueAsString(sortedParams);
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(paramString.getBytes());
        return bytesToHex(hash);
    }

    private Map<String, Object> sortMapKeys(Map<String, Object> map) {
        return new TreeMap<>(map);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public ApiRequest makeApiRequest(Map<String, Object> requestParams, String endpoint) throws Exception {
        String requestId = generateRequestId(requestParams);
        
        // Check for existing request
        ApiRequest existing = repository.findByRequestId(requestId);
        if (existing != null) {
            System.out.println("Duplicate request detected - skipping");
            return existing;
        }

        try {
            // Make your API call here (e.g., using RestTemplate)
            // ResponseEntity<String> response = restTemplate.postForEntity(endpoint, requestParams, String.class);
            
            // If successful, save with status "success"
            ApiRequest request = new ApiRequest(requestId, "success", endpoint, requestParams);
            return repository.save(request);
            
        } catch (DuplicateKeyException e) {
            System.out.println("Duplicate request detected concurrently");
            return repository.findByRequestId(requestId);
        } catch (Exception e) {
            // Save failed attempt
            ApiRequest request = new ApiRequest(requestId, "failure", endpoint, requestParams);
            repository.save(request);
            throw e;
        }
    }
}
