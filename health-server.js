/**
 * HealTogether — 실시간 채팅 서버
 * Node.js + Express + Socket.io
 *
 * 실행: node health-server.js
 * 의존성: npm install express socket.io
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

// ── Static files ──────────────────────────────────
app.use(express.static(path.join(__dirname)));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'health-community.html')));

// ── In-Memory Store ───────────────────────────────
const rooms = new Map();       // roomId → Room
const messages = new Map();    // roomId → Message[]
const roomUsers = new Map();   // roomId → Set<socketId>
const socketNick = new Map();  // socketId → { nick, roomId }
let totalMsgsToday = 0;

// Seed demo rooms
const seedRooms = [
  { id: 'r1', name: '루게릭병 (ALS)', desc: '근위축성 측삭 경화증 환자와 가족들의 공간입니다.', tags: ['희귀병', '신경질환', '치료중'] },
  { id: 'r2', name: '1형 당뇨', desc: '인슐린 의존 당뇨병 환자들의 경험과 혈당 관리 팁을 공유해요.', tags: ['당뇨', '인슐린', '만성질환'] },
  { id: 'r3', name: '갑상선암', desc: '갑상선암 진단부터 수술, 추적 관찰까지 함께 이야기 나눠요.', tags: ['암', '갑상선', '완치자환영'] },
  { id: 'r4', name: '루푸스 (SLE)', desc: '전신홍반성루푸스 환우들의 방. 증상 관리 팁을 나눠요.', tags: ['루푸스', '자가면역', '희귀병'] },
  { id: 'r5', name: '우울증·불안장애', desc: '정신건강에 어려움을 겪는 분들의 안전한 공간.', tags: ['우울증', '불안', '정신건강'] },
  { id: 'r6', name: '파킨슨병', desc: '파킨슨병 환자와 가족들의 일상, 약물 치료 경험을 나눠요.', tags: ['파킨슨', '신경질환', '가족'] },
];
seedRooms.forEach(r => {
  rooms.set(r.id, { ...r, onlineCount: 0, msgCount: 0, createdAt: Date.now() });
  messages.set(r.id, []);
  roomUsers.set(r.id, new Set());
});

// ── Helpers ───────────────────────────────────────
function getRoomList() {
  return Array.from(rooms.values()).map(r => ({
    ...r,
    onlineCount: (roomUsers.get(r.id) || new Set()).size,
    msgCount: (messages.get(r.id) || []).length,
  }));
}

function getStats() {
  let users = 0;
  roomUsers.forEach(s => users += s.size);
  return { rooms: rooms.size, users, msgs: totalMsgsToday };
}

function broadcastStats() {
  io.emit('stats', getStats());
}

function broadcastRoomUpdate(roomId) {
  const r = rooms.get(roomId);
  if (!r) return;
  io.emit('roomUpdated', {
    ...r,
    onlineCount: (roomUsers.get(roomId) || new Set()).size,
    msgCount: (messages.get(roomId) || []).length,
  });
}

// ── Socket.io Events ─────────────────────────────
io.on('connection', (socket) => {
  console.log(`[+] 연결: ${socket.id}`);

  // Send room list on connect
  socket.emit('rooms', getRoomList());
  socket.emit('stats', getStats());

  // ── Get Rooms
  socket.on('getRooms', () => socket.emit('rooms', getRoomList()));
  socket.on('getStats', () => socket.emit('stats', getStats()));

  // ── Create Room
  socket.on('createRoom', (data) => {
    const id = data.id || 'r' + crypto.randomBytes(4).toString('hex');
    const room = {
      id, name: data.name, desc: data.desc || '',
      tags: data.tags || [], onlineCount: 0, msgCount: 0,
      createdAt: Date.now(),
    };
    rooms.set(id, room);
    messages.set(id, []);
    roomUsers.set(id, new Set());
    io.emit('roomCreated', { ...room });
    io.emit('rooms', getRoomList());
    broadcastStats();
    console.log(`[방] 생성: "${room.name}" (${id})`);
  });

  // ── Join Room
  socket.on('joinRoom', ({ roomId, nick }) => {
    if (!rooms.has(roomId)) return;

    // Leave previous room if any
    const prev = socketNick.get(socket.id);
    if (prev && prev.roomId !== roomId) {
      socket.leave(prev.roomId);
      const prevUsers = roomUsers.get(prev.roomId);
      if (prevUsers) prevUsers.delete(socket.id);
      io.to(prev.roomId).emit('userLeft', prev.nick);
      broadcastRoomUpdate(prev.roomId);
    }

    socketNick.set(socket.id, { nick, roomId });
    socket.join(roomId);

    const users = roomUsers.get(roomId);
    users.add(socket.id);

    // Send history (last 100 msgs)
    const hist = (messages.get(roomId) || []).slice(-100);
    socket.emit('history', hist);

    // Online users list
    const nickList = Array.from(users).map(sid => {
      const info = socketNick.get(sid);
      return info ? info.nick : 'Unknown';
    });
    io.to(roomId).emit('onlineUsers', nickList);
    io.to(roomId).emit('userJoined', nick);

    broadcastRoomUpdate(roomId);
    broadcastStats();
    console.log(`[입장] ${nick} → ${rooms.get(roomId)?.name}`);
  });

  // ── Message
  socket.on('message', (msg) => {
    const info = socketNick.get(socket.id);
    if (!info) return;
    const { roomId } = info;
    if (!rooms.has(roomId)) return;

    const message = {
      id: crypto.randomBytes(8).toString('hex'),
      roomId, nick: info.nick,
      type: msg.type || 'exp',
      text: String(msg.text || '').trim().slice(0, 2000),
      ts: Date.now(),
      likes: 0,
    };
    if (!message.text) return;

    const roomMsgs = messages.get(roomId);
    roomMsgs.push(message);
    if (roomMsgs.length > 500) roomMsgs.splice(0, roomMsgs.length - 500);

    totalMsgsToday++;
    io.to(roomId).emit('message', message);
    broadcastRoomUpdate(roomId);
    broadcastStats();
  });

  // ── Like Message
  socket.on('likeMsg', ({ msgId, roomId }) => {
    const roomMsgs = messages.get(roomId) || [];
    const m = roomMsgs.find(m => m.id === msgId);
    if (m) { m.likes = (m.likes || 0) + 1; }
  });

  // ── Leave Room (explicit)
  socket.on('leaveRoom', ({ roomId, nick }) => {
    socket.leave(roomId);
    const users = roomUsers.get(roomId);
    if (users) users.delete(socket.id);
    socketNick.delete(socket.id);
    io.to(roomId).emit('userLeft', nick);
    broadcastRoomUpdate(roomId);
    broadcastStats();
  });

  // ── Disconnect
  socket.on('disconnect', () => {
    const info = socketNick.get(socket.id);
    if (info) {
      const { nick, roomId } = info;
      const users = roomUsers.get(roomId);
      if (users) users.delete(socket.id);
      io.to(roomId).emit('userLeft', nick);
      const nickList = Array.from(users || []).map(sid => {
        const i = socketNick.get(sid); return i ? i.nick : 'Unknown';
      });
      io.to(roomId).emit('onlineUsers', nickList);
      broadcastRoomUpdate(roomId);
    }
    socketNick.delete(socket.id);
    broadcastStats();
    console.log(`[-] 종료: ${socket.id}`);
  });
});

// ── Reset daily message count at midnight ────────
setInterval(() => {
  const now = new Date();
  if (now.getHours() === 0 && now.getMinutes() === 0) totalMsgsToday = 0;
}, 60000);

// ── Start ─────────────────────────────────────────
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`\n💚 HealTogether 서버 실행 중`);
  console.log(`   http://localhost:${PORT}\n`);
  console.log(`   health-community.html 에서 접속하세요`);
});
