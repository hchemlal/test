@ExtendWith(MockitoExtension.class)
class OrderListenerMockTest {

    @Mock
    private OrderService orderService;
    
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private OrderListener orderListener;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> producerRecordCaptor;

    @Test
    void shouldProcessValidOrderAndCallService() {
        // Given
        Order validOrder = new Order("123", 100.0);
        ConsumerRecord<String, Order> record = 
            new ConsumerRecord<>("orders-topic", 0, 0, "123", validOrder);

        // When
        orderListener.processOrder(record, mock(Acknowledgment.class));

        // Then
        verify(orderService).processOrder(validOrder);
        verify(kafkaTemplate, never()).send(any(), any());
    }

    @Test
    void shouldSendInvalidOrderToDlt() {
        // Given
        Order invalidOrder = new Order("456", -50.0);
        ConsumerRecord<String, Order> record = 
            new ConsumerRecord<>("orders-topic", 0, 0, "456", invalidOrder);
        
        doThrow(new ValidationException("Invalid amount"))
            .when(orderService).processOrder(any());

        // When
        orderListener.processOrder(record, mock(Acknowledgment.class));

        // Then
        verify(kafkaTemplate).send(producerRecordCaptor.capture());
        
        ProducerRecord<String, String> sentRecord = producerRecordCaptor.getValue();
        assertThat(sentRecord.topic()).isEqualTo("orders-topic.DLT");
        assertThat(sentRecord.key()).isEqualTo("456");
        assertThat(sentRecord.value()).contains("Invalid amount");
    }
}
