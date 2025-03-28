import org.springframework.data.mongodb.repository.MongoRepository;

public interface ApiRequestRepository extends MongoRepository<ApiRequest, String> {
    ApiRequest findByRequestId(String requestId);
}
