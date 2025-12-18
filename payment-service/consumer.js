const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'payment-service',
  brokers: [ 'localhost:9092' ],
});

const consumer = kafka.consumer({
  groupId: 'payment-group',
});

async function startConsumer() {
  await consumer.connect();
  await consumer.subscribe({
    topic: 'order.created',
    fromBeginning: false,
  });

  console.log('Payment Service started');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      console.log(
        `Processing payment for order ${ order.orderId } (partition ${ partition })`
      );

      // Simulate payment delay
      await new Promise((res) => setTimeout(res, 2000));

      console.log(`Payment completed for order ${ order.orderId }`);
    },
  });
}

startConsumer().catch(console.error);