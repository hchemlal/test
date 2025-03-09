@TestConfiguration
static class TestConfig {
    @Bean
    public KafkaTemplate<String, Order> orderKafkaTemplate(
        ProducerFactory<String, Order> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(
        ProducerFactory<String, String> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }
}
