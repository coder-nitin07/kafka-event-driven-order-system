const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'notification-group',
});

async function startConsumer() {
  await consumer.connect();
  console.log('Notification Service connected to Kafka');

  await consumer.subscribe({
    topic: 'order.created',
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      console.log(
        `Sending notification for order ${ order.orderId } (partition ${ partition })`
      );
    },
  });
}

startConsumer().catch(console.error);