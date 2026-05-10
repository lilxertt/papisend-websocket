const { createServer } = require("http");
const { Server } = require("socket.io");
const { v4: uuidv4 } = require("uuid");

const PORT = process.env.PORT || 3001;

const httpServer = createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", sessions: Object.keys(sessions).length }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  maxHttpBufferSize: 1e8, // 100MB
});

// sessions: { [sessionId]: { desktop: socket, mobile: socket, files: [], createdAt } }
const sessions = {};
const socketToSession = {}; // socketId -> { sessionId, role }

function broadcastSessionState(sessionId) {
  const session = sessions[sessionId];
  if (!session) return;
  const state = {
    sessionId,
    desktopConnected: !!session.desktop,
    mobileConnected: !!session.mobile,
    deviceName: session.deviceName || null,
    desktopName: session.desktopName || "Desktop",
    files: session.files || [],
    createdAt: session.createdAt,
    ping: session.ping || 0,
    battery: session.battery || null,
    transferQueue: session.transferQueue || [],
  };
  if (session.desktop) session.desktop.emit("session:state", state);
  if (session.mobile) session.mobile.emit("session:state", state);
}

io.on("connection", (socket) => {
  console.log(`[Socket] Connected: ${socket.id}`);

  // Desktop creates a session and gets a sessionId for QR
  socket.on("session:create", ({ desktopName } = {}) => {
    const sessionId = uuidv4().slice(0, 8).toUpperCase();
    sessions[sessionId] = {
      desktop: socket,
      mobile: null,
      files: [],
      transferQueue: [],
      createdAt: Date.now(),
      desktopName: desktopName || "Desktop",
      ping: 0,
      battery: null,
    };
    socketToSession[socket.id] = { sessionId, role: "desktop" };
    socket.join(sessionId);
    socket.emit("session:created", { sessionId });
    console.log(`[Session] Created: ${sessionId} by desktop ${socket.id}`);
    broadcastSessionState(sessionId);
  });

  // Mobile joins using sessionId from QR
  socket.on("session:join", ({ sessionId, deviceName, battery }) => {
    const session = sessions[sessionId];
    if (!session) {
      socket.emit("session:error", { message: "Session not found or expired." });
      return;
    }
    if (session.mobile) {
      socket.emit("session:error", { message: "Session already occupied." });
      return;
    }
    session.mobile = socket;
    session.deviceName = deviceName || "iPhone";
    session.battery = battery || null;
    socketToSession[socket.id] = { sessionId, role: "mobile" };
    socket.join(sessionId);
    socket.emit("session:joined", { sessionId, desktopName: session.desktopName });
    if (session.desktop) session.desktop.emit("mobile:connected", { deviceName: session.deviceName, battery });
    console.log(`[Session] Mobile joined: ${sessionId} device=${session.deviceName}`);
    broadcastSessionState(sessionId);
  });

  // Ping/pong for latency measurement
  socket.on("ping:send", ({ sessionId, timestamp }) => {
    const session = sessions[sessionId];
    if (session) {
      const target = socketToSession[socket.id]?.role === "desktop" ? session.mobile : session.desktop;
      if (target) target.emit("ping:receive", { timestamp });
    }
  });

  socket.on("ping:response", ({ sessionId, timestamp }) => {
    const latency = Date.now() - timestamp;
    const session = sessions[sessionId];
    if (session) {
      session.ping = latency;
      broadcastSessionState(sessionId);
    }
  });

  // File transfer: mobile sends file metadata to desktop
  socket.on("transfer:start", ({ sessionId, file }) => {
    const session = sessions[sessionId];
    if (!session) return;
    const transferId = uuidv4();
    const transfer = {
      id: transferId,
      name: file.name,
      size: file.size,
      type: file.type,
      progress: 0,
      status: "uploading",
      startedAt: Date.now(),
    };
    session.transferQueue.push(transfer);
    io.to(sessionId).emit("transfer:initiated", transfer);
    broadcastSessionState(sessionId);
  });

  socket.on("transfer:progress", ({ sessionId, transferId, progress }) => {
    const session = sessions[sessionId];
    if (!session) return;
    const t = session.transferQueue.find((t) => t.id === transferId);
    if (t) {
      t.progress = progress;
      t.status = progress >= 100 ? "complete" : "uploading";
    }
    io.to(sessionId).emit("transfer:progress", { transferId, progress });
    if (progress >= 100) broadcastSessionState(sessionId);
  });

  socket.on("transfer:chunk", ({ sessionId, transferId, chunk, chunkIndex, totalChunks }) => {
    const session = sessions[sessionId];
    if (!session || !session.desktop) return;
    session.desktop.emit("transfer:chunk", { transferId, chunk, chunkIndex, totalChunks });
  });

  socket.on("transfer:complete", ({ sessionId, transferId, dataUrl, fileName, fileType }) => {
    const session = sessions[sessionId];
    if (!session) return;
    const t = session.transferQueue.find((t) => t.id === transferId);
    if (t) { t.status = "complete"; t.progress = 100; }
    session.files.push({ id: transferId, name: fileName, type: fileType, dataUrl, receivedAt: Date.now() });
    if (session.desktop) session.desktop.emit("transfer:received", { transferId, fileName, fileType, dataUrl });
    broadcastSessionState(sessionId);
  });

  socket.on("battery:update", ({ sessionId, battery }) => {
    const session = sessions[sessionId];
    if (session) { session.battery = battery; broadcastSessionState(sessionId); }
  });

  socket.on("disconnect", () => {
    const info = socketToSession[socket.id];
    if (!info) return;
    const { sessionId, role } = info;
    const session = sessions[sessionId];
    if (session) {
      if (role === "desktop") {
        session.desktop = null;
        if (session.mobile) session.mobile.emit("peer:disconnected", { role: "desktop" });
        // Clean up session after timeout
        setTimeout(() => {
          if (sessions[sessionId] && !sessions[sessionId].desktop) {
            delete sessions[sessionId];
            console.log(`[Session] Cleaned up: ${sessionId}`);
          }
        }, 30000);
      } else {
        session.mobile = null;
        if (session.desktop) session.desktop.emit("peer:disconnected", { role: "mobile" });
      }
    }
    delete socketToSession[socket.id];
    console.log(`[Socket] Disconnected: ${socket.id} role=${role}`);
  });
});

httpServer.listen(PORT, () => {
  console.log(`[PapiSend] Socket.io server running on port ${PORT}`);
});
