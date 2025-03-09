@SpringBootTest(classes = {KafkaConfig.class, OrderListener.class})
@EmbeddedKafka(topics = {"orders-topic", "orders-topic.DLT", "orders-success-topic"})
@DirtiesContext
class OrderListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Autowired
    private KafkaTemplate<String, String> dltKafkaTemplate;

    private Consumer<String, String> dltConsumer;
    private Consumer<String, Order> successConsumer;

    @BeforeEach
    void setup(EmbeddedKafkaBroker embeddedKafka) {
        // Setup DLT consumer
        Map<String, Object> dltConsumerConfig = KafkaTestUtils.consumerProps("dlt-test-group", "true", embeddedKafka);
        dltConsumer = new DefaultKafkaConsumerFactory<>(
            dltConsumerConfig, 
            new StringDeserializer(), 
            new StringDeserializer()
        ).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, "orders-topic.DLT");

        // Setup success topic consumer
        Map<String, Object> successConsumerConfig = KafkaTestUtils.consumerProps("success-test-group", "true", embeddedKafka);
        successConsumer = new DefaultKafkaConsumerFactory<>(
            successConsumerConfig,
            new StringDeserializer(),
            new JsonDeserializer<>(Order.class)
        ).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(successConsumer, "orders-success-topic");
    }

    @AfterEach
    void tearDown() {
        dltConsumer.close();
        successConsumer.close();
    }

    @Test
    void shouldProcessValidOrderAndSendToSuccessTopic() {
        // Given
        Order validOrder = new Order("123", 100.0);
        
        // When
        kafkaTemplate.send("orders-topic", validOrder.orderId(), validOrder);
        
        // Then
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConsumerRecord<String, Order> record = KafkaTestUtils.getSingleRecord(successConsumer, "orders-success-topic");
            assertThat(record.value()).isEqualTo(validOrder);
        });
    }

    @Test
    void shouldSendInvalidOrderToDltWithCustomMessage() {
        // Given
        Order invalidOrder = new Order("invalid-123", -100.0);
        String expectedDltMessage = "DLT: Invalid order amount";
        
        // When
        kafkaTemplate.send("orders-topic", invalidOrder.orderId(), invalidOrder);
        
        // Then
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConsumerRecord<String, String> dltRecord = KafkaTestUtils.getSingleRecord(dltConsumer, "orders-topic.DLT");
            assertThat(dltRecord.value()).isEqualTo(expectedDltMessage);
            assertThat(dltRecord.key()).isEqualTo(invalidOrder.orderId());
        });
    }
}
