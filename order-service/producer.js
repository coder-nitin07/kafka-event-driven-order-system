const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'order-service',
    brokers: [ 'localhost:9092' ]
});

const producer = kafka.producer();

async function connectProducer(){
    await producer.connect();
    console.log(`Kafka Producer connected`);
}

async function publishOrderCreated(order){
    await producer.new({
        topic: 'order.created',
        message: [
            {
                key: order.orderId,
                value: JSON.stringify(order)
            }
        ]
    });
}

module.exports = { connectProducer, publishOrderCreated }