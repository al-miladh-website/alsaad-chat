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
const cache = new Map();
const MAX_LENGTH = 500;

async function translate(text, from, to) {
  const key = process.env.AZURE_TRANSLATOR_KEY;

  if (!key) {
    console.error('AZURE_TRANSLATOR_KEY is missing');
    return { text, translated: false };
  }

  const source = from === 'ckb' ? 'ku' : from;
  const target = to === 'ckb' ? 'ku' : to;
  const cacheKey = `${source}:${target}:${text}`;

  if (cache.has(cacheKey)) {
    return { text: cache.get(cacheKey), translated: true };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    const params = new URLSearchParams({
      'api-version': '3.0',
      from: source,
      to: target
    });

    const response = await fetch(
      `https://api.cognitive.microsofttranslator.com/translate?${params}`,
      {
        method: 'POST',
        signal: controller.signal,
        headers: {
          'Ocp-Apim-Subscription-Key': key,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify([{ Text: text }])
      }
    );

    const data = await response.json();

    if (!response.ok) {
      throw new Error(
        `Azure HTTP ${response.status}: ${JSON.stringify(data)}`
      );
    }

    const translatedText =
      data?.[0]?.translations?.[0]?.text?.trim();

    if (!translatedText || translatedText === text.trim()) {
      return { text, translated: false };
    }

    if (cache.size >= 300) {
      cache.delete(cache.keys().next().value);
    }

    cache.set(cacheKey, translatedText);

    return { text: translatedText, translated: true };
  } catch (error) {
    console.error('Azure translation error:', error.message);
    return { text, translated: false };
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

    if (socket.data.room && socket.data.room !== room) {
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
      state = { digest, members: new Set() };
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
    const target = role === 'ar' ? 'ckb' : 'ar';

    const result = await translate(original, role, target);

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
  () => console.log('ALSAAD Chat running')
);
