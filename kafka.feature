Feature: Order Processing via Kafka
  Scenario: Process valid order
    Given a valid order with ID "123" and amount 100.0
    When the order is sent to "orders-topic"
    Then the order should appear in "orders-success-topic"

  Scenario: Process invalid order
    Given an invalid order with ID "456" and amount -50.0
    When the order is sent to "orders-topic"
    Then a DLT message "DLT: Invalid order amount" should appear in "orders-topic.DLT"
