'use strict';

/**
 * RaniRajaCall — anonymous random-stranger audio chat server
 *
 * PRIVACY DESIGN NOTES (read before modifying):
 * - There is NO database and NO disk persistence anywhere in this file.
 * - Everything lives in process memory (plain JS objects/Maps) and is lost on restart.
 * - Chat messages are NEVER stored server-side at all — they are relayed socket->socket
 *   and not kept in any structure once the relay call returns.
 * - Images are relayed as base64 strings through Socket.io and are not written to disk.
 *   A timer reference is kept ONLY so we can tell the room "this media should be gone now";
 *   the actual image bytes are not retained in any server-side variable beyond the single
 *   synchronous relay step.
 * - The only thing the server "stores" beyond a live socket's lifetime is the reconnect
 *   code map (random code -> two anonymous socket ids), which expires in 24h and is swept
 *   hourly. No identifying info (no IP, no name, no account) is in that map.
 *
 * IMPORTANT / NOT INCLUDED ON PURPOSE:
 * This app intentionally does NOT implement age verification or identity checks, because
 * doing so would require collecting and storing personal data — which conflicts directly
 * with the "no accounts, no storage" requirement in the spec. Anonymous random-stranger
 * audio/video chat tools have historically been misused to target minors. If you deploy
 * this publicly, you are responsible for adding appropriate safeguards (age gating,
 * reporting/moderation tooling, abuse monitoring) before going live. See the
 * `// SAFETY TODO` markers below for the spots where that logic would plug in.
 */

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 12 * 1024 * 1024, // ~12MB to comfortably allow a 10MB base64 image (base64 inflates size ~33%)
});

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

// SAFETY TODO: add an age-confirmation / terms gate here before serving the app
// in any real deployment. Not implemented because it would require storing consent
// state, which conflicts with the no-storage spec as written.
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---------------------------------------------------------------------------
// In-memory state (ALL of it — nothing here ever touches a disk or DB)
// ---------------------------------------------------------------------------

/** socketId -> { status: 'idle'|'waiting'|'paired', partnerId, nextCount, muted } */
const users = new Map();

/** queue of socketIds waiting for a partner */
let waitingQueue = [];

/** code -> { socketIdA, socketIdB, expires } */
const reconnectCodes = new Map();

const ADJECTIVES = [
  'Rani', 'Raja', 'Babu', 'Shona', 'Sona', 'Chanda', 'Jaan', 'Pyaari',
  'Sundar', 'Sher', 'Hira', 'Moti', 'Tara', 'Veer', 'Shakti', 'Kiran',
  'Mithu', 'Gudiya', 'Heera', 'Ranjha'
];

function generateReconnectCode() {
  let code;
  do {
    const word = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
    const num = Math.floor(1000 + Math.random() * 9000);
    code = `${word}-${num}`;
  } while (reconnectCodes.has(code));
  return code;
}

function makeUser() {
  return {
    status: 'idle',
    partnerId: null,
    nextCount: 0,
  };
}

function broadcastOnlineCount() {
  io.emit('online-count', users.size);
}

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

function removeFromQueue(socketId) {
  waitingQueue = waitingQueue.filter((id) => id !== socketId);
}

function tryMatch(socketId) {
  removeFromQueue(socketId);

  // find first waiting user that is still connected and isn't this socket
  while (waitingQueue.length > 0) {
    const candidateId = waitingQueue.shift();
    if (candidateId === socketId) continue;
    const candidateSocket = io.sockets.sockets.get(candidateId);
    const candidateUser = users.get(candidateId);
    if (!candidateSocket || !candidateUser || candidateUser.status !== 'waiting') {
      continue; // stale entry, skip it
    }
    pairUsers(socketId, candidateId);
    return;
  }

  // no one available — go on the queue
  const user = users.get(socketId);
  if (user) {
    user.status = 'waiting';
    user.partnerId = null;
  }
  waitingQueue.push(socketId);
  io.to(socketId).emit('searching');
}

function pairUsers(idA, idB) {
  const userA = users.get(idA);
  const userB = users.get(idB);
  if (!userA || !userB) return;

  userA.status = 'paired';
  userA.partnerId = idB;
  userB.status = 'paired';
  userB.partnerId = idA;

  const code = generateReconnectCode();
  reconnectCodes.set(code, {
    socketIdA: idA,
    socketIdB: idB,
    expires: Date.now() + 24 * 60 * 60 * 1000, // 24 hours
  });

  // idA is the "initiator" / offer-creator by convention
  io.to(idA).emit('matched', { initiator: true, code });
  io.to(idB).emit('matched', { initiator: false, code });
}

function endPairing(socketId, { notifyPartner = true, reason = 'left' } = {}) {
  const user = users.get(socketId);
  if (!user) return;

  const partnerId = user.partnerId;
  user.status = 'idle';
  user.partnerId = null;

  if (partnerId) {
    const partner = users.get(partnerId);
    if (partner) {
      partner.status = 'idle';
      partner.partnerId = null;
      if (notifyPartner) {
        io.to(partnerId).emit('partner-left', { reason });
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Socket.io handlers
// ---------------------------------------------------------------------------

io.on('connection', (socket) => {
  users.set(socket.id, makeUser());
  broadcastOnlineCount();

  socket.emit('online-count', users.size);

  // ---- Matching ----
  socket.on('find-stranger', () => {
    const user = users.get(socket.id);
    if (!user) return;
    if (user.status === 'paired') {
      endPairing(socket.id, { reason: 'left' });
    }
    tryMatch(socket.id);
  });

  socket.on('next-stranger', () => {
    const user = users.get(socket.id);
    if (!user) return;
    user.nextCount += 1;
    endPairing(socket.id, { reason: 'skipped' });
    tryMatch(socket.id);
    socket.emit('next-count', user.nextCount);
  });

  socket.on('stop-call', () => {
    endPairing(socket.id, { reason: 'stopped' });
    removeFromQueue(socket.id);
    const user = users.get(socket.id);
    if (user) user.status = 'idle';
  });

  // ---- WebRTC signaling (pure relay, nothing stored) ----
  socket.on('webrtc-offer', (payload) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('webrtc-offer', payload);
    }
  });

  socket.on('webrtc-answer', (payload) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('webrtc-answer', payload);
    }
  });

  socket.on('webrtc-ice-candidate', (payload) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('webrtc-ice-candidate', payload);
    }
  });

  // ---- Text chat (relay only — server never stores a single message) ----
  socket.on('chat-message', (text) => {
    const user = users.get(socket.id);
    if (!user?.partnerId) return;
    if (typeof text !== 'string' || text.length === 0 || text.length > 2000) return;
    io.to(user.partnerId).emit('chat-message', {
      text,
      ts: Date.now(),
    });
  });

  socket.on('typing', (isTyping) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('typing', !!isTyping);
    }
  });

  // ---- Media (image) relay — base64 in, base64 out, never written anywhere ----
  socket.on('media-message', (payload) => {
    const user = users.get(socket.id);
    if (!user?.partnerId) return;
    if (!payload || typeof payload.dataUrl !== 'string') return;

    // crude size guard server-side too (10MB raw -> ~13.3MB base64 string)
    if (payload.dataUrl.length > 14 * 1024 * 1024) {
      socket.emit('media-error', 'Image too large (max 10MB).');
      return;
    }

    const mediaId = `${socket.id}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    io.to(user.partnerId).emit('media-message', {
      mediaId,
      dataUrl: payload.dataUrl,
      ts: Date.now(),
    });

    // We do not keep payload.dataUrl referenced anywhere past this point.
    // The 30s auto-delete is enforced client-side (both sender preview and
    // receiver view); this server-side timer just tells both ends to purge,
    // as a backstop in case a client-side timer is tampered with.
    setTimeout(() => {
      io.to(socket.id).emit('media-expire', mediaId);
      io.to(user.partnerId).emit('media-expire', mediaId);
    }, 30 * 1000);
  });

  // ---- Reconnect via secret code ----
  socket.on('reconnect-with-code', (rawCode) => {
    const code = String(rawCode || '').trim();
    const entry = reconnectCodes.get(code);

    if (!entry || entry.expires < Date.now()) {
      socket.emit('reconnect-failed', 'Invalid or expired code.');
      return;
    }

    const { socketIdA, socketIdB } = entry;
    const isParticipant = socket.id === socketIdA || socket.id === socketIdB;
    if (!isParticipant) {
      socket.emit('reconnect-failed', 'This code does not belong to you.');
      return;
    }

    const otherId = socket.id === socketIdA ? socketIdB : socketIdA;
    const otherSocket = io.sockets.sockets.get(otherId);
    if (!otherSocket) {
      socket.emit('reconnect-failed', 'The other person is not online right now.');
      return;
    }

    const selfUser = users.get(socket.id);
    const otherUser = users.get(otherId);
    if (!selfUser || !otherUser) {
      socket.emit('reconnect-failed', 'Could not reconnect.');
      return;
    }

    // pull both out of whatever they were doing
    endPairing(socket.id, { notifyPartner: true, reason: 'reconnect' });
    endPairing(otherId, { notifyPartner: true, reason: 'reconnect' });
    removeFromQueue(socket.id);
    removeFromQueue(otherId);

    selfUser.status = 'paired';
    selfUser.partnerId = otherId;
    otherUser.status = 'paired';
    otherUser.partnerId = socket.id;

    io.to(socket.id).emit('matched', { initiator: true, code, reconnected: true });
    io.to(otherId).emit('matched', { initiator: false, code, reconnected: true });
  });

  // ---- Mute state (purely informational, for UI on partner's side if desired) ----
  socket.on('mute-state', (isMuted) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('partner-mute-state', !!isMuted);
    }
  });

  // ---- Speaking indicator passthrough (for "stranger is speaking" visualizer) ----
  socket.on('speaking-state', (isSpeaking) => {
    const user = users.get(socket.id);
    if (user?.partnerId) {
      io.to(user.partnerId).emit('partner-speaking-state', !!isSpeaking);
    }
  });

  // ---- Disconnect cleanup ----
  socket.on('disconnect', () => {
    endPairing(socket.id, { notifyPartner: true, reason: 'disconnected' });
    removeFromQueue(socket.id);
    users.delete(socket.id);
    broadcastOnlineCount();
    // Note: any reconnect codes referencing this socket id simply become
    // unusable once the other side checks liveness above; we don't need to
    // scrub them immediately since they carry no personal data and expire
    // naturally within 24h (swept hourly below).
  });
});

// ---------------------------------------------------------------------------
// Hourly sweep of expired reconnect codes (spec requirement)
// ---------------------------------------------------------------------------
setInterval(() => {
  const now = Date.now();
  for (const [code, entry] of reconnectCodes.entries()) {
    if (entry.expires < now) {
      reconnectCodes.delete(code);
    }
  }
}, 60 * 60 * 1000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`RaniRajaCall server running on http://localhost:${PORT}`);
  console.log('In-memory only. No database. No files written. Restart wipes everything.');
});
