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
    await producer.send({
        topic: 'order.created',
        messages: [
            {
                key: order.orderId,
                value: JSON.stringify(order)
            }
        ]
    });
}

module.exports = { connectProducer, publishOrderCreated }