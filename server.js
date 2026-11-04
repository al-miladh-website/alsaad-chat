
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import crypto from 'crypto';
import 'dotenv/config';

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

const rooms = new Map();
const MAX_LENGTH = 500;

async function translate(text, from, to) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12000);

  try {
    const params = new URLSearchParams({
      q: text,
      langpair: `${from}|${to}`
    });

    const url =
      `https://api.mymemory.translated.net/get?${params}`;

    const response = await fetch(url, {
      method: 'GET',
      signal: controller.signal,
      headers: {
        Accept: 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    const translated = data?.responseData?.translatedText;
    const status = Number(data?.responseStatus);

    if (
      status !== 200 ||
      typeof translated !== 'string' ||
      !translated.trim()
    ) {
      return {
        text,
        translated: false
      };
    }

    return {
      text: translated.trim(),
      translated: true
    };

  } catch (error) {
    console.error('Translation error:', error.message);

    return {
      text,
      translated: false
    };

  } finally {
    clearTimeout(timeout);
  }
}

io.on('connection', socket => {

  socket.on('join', ({ room, secret, role } = {}, ack) => {

    if (
      typeof room !== 'string' ||
      !/^\d{6}$/.test(room) ||
      typeof secret !== 'string' ||
      secret.length < 8 ||
      secret.length > 100 ||
      !['ar', 'ckb'].includes(role)
    ) {
      return ack?.({
        ok: false,
        error: 'بيانات الغرفة غير صحيحة'
      });
    }

    const digest = crypto
      .createHash('sha256')
      .update(secret)
      .digest('hex');

    let state = rooms.get(room);

    if (state && state.digest !== digest) {
      return ack?.({
        ok: false,
        error: 'كلمة سر الغرفة غير صحيحة'
      });
    }

    if (!state) {
      state = {
        digest,
        members: new Set()
      };

      rooms.set(room, state);
    }

    if (
      state.members.size >= 2 &&
      !state.members.has(socket.id)
    ) {
      return ack?.({
        ok: false,
        error: 'الغرفة ممتلئة'
      });
    }

    if (socket.data.room && socket.data.room !== room) {
      const previousRoom = socket.data.room;
      const previousState = rooms.get(previousRoom);

      socket.leave(previousRoom);
      previousState?.members.delete(socket.id);

      if (previousState) {
        io.to(previousRoom).emit('presence', {
          count: previousState.members.size
        });

        if (previousState.members.size === 0) {
          rooms.delete(previousRoom);
        }
      }
    }

    socket.data.room = room;
    socket.data.role = role;

    state.members.add(socket.id);
    socket.join(room);

    ack?.({ ok: true });

    io.to(room).emit('presence', {
      count: state.members.size
    });
  });

  socket.on('message', async (text, ack) => {

    const room = socket.data.room;
    const role = socket.data.role;

    if (
      !room ||
      typeof text !== 'string' ||
      !text.trim() ||
      text.length > MAX_LENGTH
    ) {
      return ack?.({
        ok: false,
        error: 'الرسالة غير صحيحة'
      });
    }

    const original = text.trim();

    const target = role === 'ar' ? 'ckb' : 'ar';

    const result = await translate(
      original,
      role,
      target
    );

    io.to(room).emit('message', {
      id: crypto.randomUUID(),
      from: role,
      original,
      translated: result.text,
      translatedOk: result.translated,
      time: Date.now()
    });

    ack?.({ ok: true });
  });

  socket.on('disconnect', () => {

    const room = socket.data.room;

    if (!room) return;

    const state = rooms.get(room);

    if (!state) return;

    state.members.delete(socket.id);

    io.to(room).emit('presence', {
      count: state.members.size
    });

    if (state.members.size === 0) {
      rooms.delete(room);
    }
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`ALSAAD Chat running on port ${PORT}`);
});
