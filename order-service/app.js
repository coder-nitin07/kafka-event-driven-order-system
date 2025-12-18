const express = require('express');
const app = express();
const { v4: uuidv4 } = require('uuid');
const { publishOrderCreated, connectProducer } = require('./producer');

app.use(express.json());

app.post('/orders', async (req, res) => {
  try {
    const order = {
      orderId: uuidv4(),
      userId: req.body.userId,
      amount: req.body.amount,
      createdAt: new Date().toISOString(),
    };

    await publishOrderCreated(order);

    res.status(201).json({
      message: 'Order created',
      orderId: order.orderId,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to create order' });
  }
});

const PORT = 3000;

app.listen(PORT, async ()=>{
    await connectProducer();

    console.log(`Order Service running on PORT ${ PORT }`);
});