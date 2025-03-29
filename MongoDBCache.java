import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class MongoDBCache implements Cache {
    private final MongoCollection<Document> collection;
    
    public MongoDBCache(MongoCollection<Document> collection) {
        this.collection = collection;
        createTTLIndex();
    }

    private void createTTLIndex() {
        // Create TTL index if not exists
        collection.createIndex(new Document("expirationTime", 1),
            new IndexOptions().expireAfter(0L, TimeUnit.SECONDS));
    }

    @Override
    public Object get(String key) {
        Document doc = collection.find(new Document("_id", key)).first();
        if (doc == null) return null;
        if (doc.getLong("expirationTime") < System.currentTimeMillis()) {
            delete(key);
            return null;
        }
        return doc.get("value");
    }

    @Override
    public void put(String key, Object value) {
        put(key, value, Long.MAX_VALUE);
    }

    @Override
    public void put(String key, Object value, long ttl) {
        long expirationTime = System.currentTimeMillis() + ttl;
        Document doc = new Document("_id", key)
            .append("value", value)
            .append("expirationTime", expirationTime);
        collection.replaceOne(new Document("_id", key), doc, 
            new ReplaceOptions().upsert(true));
    }

    @Override
    public void delete(String key) {
        collection.deleteOne(new Document("_id", key));
    }
}
