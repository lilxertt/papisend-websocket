const WebSocket = require('ws');
const port = process.env.PORT || 3001;

const server = new WebSocket.Server({ port });

console.log(`WebSocket server running on port ${port}`);

server.on('connection', (socket) => {
  console.log('Client connected');
  
  socket.on('message', (message) => {
    console.log('Received:', message.toString());
    socket.send(`Echo: ${message}`);
  });
  
  socket.on('close', () => {
    console.log('Client disconnected');
  });
});
