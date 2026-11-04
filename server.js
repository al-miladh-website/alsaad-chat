
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
const cache = new Map();

async function translate(text, from, to) {
  const key = `${from}:${to}:${text}`;

  if (cache.has(key)) {
    return {
      text: cache.get(key),
      translated: true
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    12000
  );

  try {
    const params = new URLSearchParams({
      client: 'dict-chrome-ex',
      sl: from,
      tl: to,
      q: text
    });

    const url =
      `https://translate.googleapis.com/translate_a/t?${params}`;

    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept: 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    let translated = '';

    if (Array.isArray(data)) {
      translated = data.map(item => {
        if (Array.isArray(item)) {
          return typeof item[0] === 'string'
            ? item[0]
            : '';
        }

        return typeof item === 'string'
          ? item
          : '';
      }).join('');
    }

    if (
      !translated.trim() ||
      translated.trim() === text.trim()
    ) {
      return {
        text,
        translated: false
      };
    }

    if (cache.size >= 300) {
      cache.delete(cache.keys().next().value);
    }

    cache.set(key, translated.trim());

    return {
      text: translated.trim(),
      translated: true
    };

  } catch (error) {
    console.error(
      'Translation error:',
      error.message
    );

    return {
      text,
      translated: false
    };

  } finally {
    clearTimeout(timeout);
  }
}

io.on('connection', socket => {

  socket.on('join', (
    { room, secret, role } = {},
    ack
  ) => {

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

    if (
      socket.data.room &&
      socket.data.room !== room
    ) {
      const previousRoom = socket.data.room;
      const previous = rooms.get(previousRoom);

      socket.leave(previousRoom);
      previous?.members.delete(socket.id);

      if (previous) {
        io.to(previousRoom).emit('presence', {
          count: previous.members.size
        });

        if (!previous.members.size) {
          rooms.delete(previousRoom);
        }
      }
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
    const target = role === 'ar'
      ? 'ckb'
      : 'ar';

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

    if (!state.members.size) {
      rooms.delete(room);
    }
  });
});

server.listen(
  process.env.PORT || 3000,
  '0.0.0.0',
  () => {
    console.log('ALSAAD Chat running');
  }
);
