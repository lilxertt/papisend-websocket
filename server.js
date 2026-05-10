const express = require('express');
const expressWs = require('express-ws');
const app = express();
const port = process.env.PORT || 3001;

expressWs(app);

app.ws('/', (ws, req) => {
    console.log('✅ Nowe połączenie WebSocket!');
    
    ws.send(JSON.stringify({ type: 'connected', message: 'Połączono z serwerem' }));
    
    ws.on('message', (msg) => {
        console.log('📨 Otrzymano:', msg.toString());
        
        try {
            const data = JSON.parse(msg);
            
            if (data.type === 'get_qr') {
                ws.send(JSON.stringify({ type: 'qr', code: 'TEST_QR_CODE_123' }));
            } else if (data.type === 'get_session') {
                ws.send(JSON.stringify({ type: 'session', id: Math.random().toString(36).substring(2, 10) }));
            } else {
                ws.send(JSON.stringify({ type: 'echo', data: msg.toString() }));
            }
        } catch (e) {
            ws.send(JSON.stringify({ type: 'echo', data: msg.toString() }));
        }
    });
    
    ws.on('close', () => {
        console.log('❌ Klient rozłączony');
    });
});

app.get('/', (req, res) => {
    res.send(`
        <html>
            <body>
                <h1>PapiSend WebSocket Server</h1>
                <p>Status: RUNNING</p>
                <p>WebSocket endpoint: wss://${req.headers.host}/</p>
            </body>
        </html>
    `);
});

app.listen(port, () => {
    console.log(`🚀 Serwer WebSocket nasłuchuje na porcie ${port}`);
});
