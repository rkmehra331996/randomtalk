const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

let waitingUser = null;
const rooms = new Map();

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('find_partner', () => {
    if (waitingUser && waitingUser.id !== socket.id) {
      const roomId = `room_${Date.now()}`;
      const partner = waitingUser;
      waitingUser = null;

      socket.join(roomId);
      partner.join(roomId);

      rooms.set(socket.id, { roomId, partnerId: partner.id });
      rooms.set(partner.id, { roomId, partnerId: socket.id });

      io.to(partner.id).emit('partner_found', { roomId, isInitiator: true });
      io.to(socket.id).emit('partner_found', { roomId, isInitiator: false });

      console.log(`Matched: ${socket.id} <-> ${partner.id} in ${roomId}`);
    } else {
      waitingUser = socket;
      socket.emit('waiting');
    }
  });

  socket.on('signal', ({ to, signal }) => {
    io.to(to).emit('signal', { from: socket.id, signal });
  });

  socket.on('send_signal', ({ roomId, signal }) => {
    const room = rooms.get(socket.id);
    if (room) {
      io.to(room.partnerId).emit('receive_signal', { from: socket.id, signal });
    }
  });

  socket.on('next', () => {
    const room = rooms.get(socket.id);
    if (room) {
      const { roomId, partnerId } = room;
      io.to(partnerId).emit('partner_left');
      rooms.delete(socket.id);
      rooms.delete(partnerId);
      io.socketsLeave(roomId);
    }
    if (waitingUser && waitingUser.id === socket.id) {
      waitingUser = null;
    }
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
    if (waitingUser && waitingUser.id === socket.id) {
      waitingUser = null;
    }
    const room = rooms.get(socket.id);
    if (room) {
      io.to(room.partnerId).emit('partner_left');
      rooms.delete(room.partnerId);
      rooms.delete(socket.id);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
