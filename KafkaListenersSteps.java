package com.example.steps;

import io.cucumber.java.Before;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Map;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(topics = {"orders-topic", "orders-topic.DLT", "orders-success-topic"})
public class KafkaListenersSteps {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;
    
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;
    
    private Consumer<String, Order> successConsumer;
    private Consumer<String, String> dltConsumer;
    private Order currentOrder;

    @Before
    public void setup() {
        // Setup success topic consumer
        Map<String, Object> successConfig = KafkaTestUtils.consumerProps("success-group", "true", embeddedKafka);
        successConsumer = new DefaultKafkaConsumerFactory<>(
            successConfig, 
            new StringDeserializer(), 
            new JsonDeserializer<>(Order.class)
        ).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(successConsumer, "orders-success-topic");

        // Setup DLT consumer
        Map<String, Object> dltConfig = KafkaTestUtils.consumerProps("dlt-group", "true", embeddedKafka);
        dltConsumer = new DefaultKafkaConsumerFactory<>(
            dltConfig, 
            new StringDeserializer(), 
            new StringDeserializer()
        ).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, "orders-topic.DLT");
    }

    @After
    public void tearDown() {
        successConsumer.close();
        dltConsumer.close();
    }

    @Given("a valid order with ID {string} and amount {double}")
    public void createValidOrder(String id, double amount) {
        currentOrder = new Order(id, amount);
    }

    @Given("an invalid order with ID {string} and amount {double}")
    public void createInvalidOrder(String id, double amount) {
        currentOrder = new Order(id, amount);
    }

    @When("the order is sent to {string}")
    public void sendOrderToTopic(String topic) {
        kafkaTemplate.send(topic, currentOrder.orderId(), currentOrder);
    }

    @Then("the order should appear in {string}")
    public void verifyOrderInTopic(String topic) {
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConsumerRecord<String, Order> record = KafkaTestUtils.getSingleRecord(successConsumer, topic);
            assertEquals(currentOrder, record.value());
        });
    }

    @Then("a DLT message {string} should appear in {string}")
    public void verifyDltMessage(String expectedMessage, String topic) {
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(dltConsumer, topic);
            assertEquals(expectedMessage, record.value());
            assertEquals(currentOrder.orderId(), record.key());
        });
    }
}
