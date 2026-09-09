// server.js — JanitorAI → oai-reverse-proxy Bridge
// Forwards requests from JanitorAI to https://wulfs-den.ink/proxy
// No model mapping — the proxy handles its own models

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { StringDecoder } = require('string_decoder');
const { timingSafeEqual } = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

// ─── Configuration ───────────────────────────────────────────────────────────

const PROXY_BASE = process.env.PROXY_BASE || 'https://integrate.api.nvidia.com';
const CLIENT_AUTH_KEY = process.env.CLIENT_AUTH_KEY;
const PROXY_AUTH_KEY = process.env.PROXY_AUTH_KEY; // password for wulfs-den.ink/proxy

const REQUEST_TIMEOUT_MS = 180000;
const MAX_BUFFER_SIZE = 1024 * 1024; // 1MB

// ─── Config validation ───────────────────────────────────────────────────────

function validateConfig() {
  if (!CLIENT_AUTH_KEY) {
    console.warn('[WARN] CLIENT_AUTH_KEY not set. All requests will be rejected with 403.');
  }
}

validateConfig();

// ─── Middleware ──────────────────────────────────────────────────────────────

app.use(cors());
app.use(express.json({ limit: '10mb' }));

function extractBearerToken(authHeader) {
  if (!authHeader || typeof authHeader !== 'string') return null;
  const parts = authHeader.trim().split(' ');
  if (parts.length !== 2 || parts[0] !== 'Bearer') return null;
  return parts[1];
}

function safeTimingEqual(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  try {
    return timingSafeEqual(Buffer.from(a), Buffer.from(b));
  } catch {
    return false;
  }
}

// Auth middleware — skip for health and model listing
app.use((req, res, next) => {
  if (req.path === '/health' || req.path === '/v1/models') {
    return next();
  }

  const token = extractBearerToken(req.headers.authorization);

  if (!token || !CLIENT_AUTH_KEY) {
    return res.status(403).json({
      error: {
        message: 'Forbidden: Invalid or missing authentication',
        type: 'authentication_error',
        code: 403
      }
    });
  }

  if (!safeTimingEqual(token, CLIENT_AUTH_KEY)) {
    return res.status(403).json({
      error: {
        message: 'Forbidden: Invalid authentication credentials',
        type: 'authentication_error',
        code: 403
      }
    });
  }

  next();
});

// ─── Helper: Safe Stream Writing ─────────────────────────────────────────────

function safeWrite(res, data) {
  try {
    if (!res.writableEnded && !res.destroyed && res.writable) {
      res.write(data);
      return true;
    }
  } catch (err) {
    console.warn('[STREAM] Write failed:', err.message);
  }
  return false;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  res.json({ status: 'ok', version: '3.0.0' });
});

// Forward /v1/models directly from the upstream proxy
app.get('/v1/models', async (req, res) => {
  try {
    const response = await axios.get(`${PROXY_BASE}/v1/models`, {
      headers: {
        ...(PROXY_AUTH_KEY && { Authorization: `Bearer ${PROXY_AUTH_KEY}` }),
        'Content-Type': 'application/json'
      },
      timeout: 15000
    });
    res.json(response.data);
  } catch (err) {
    console.warn('[MODELS] Failed to fetch model list from proxy:', err.message);
    res.status(502).json({
      error: {
        message: 'Could not retrieve model list from upstream proxy',
        type: 'proxy_error',
        code: 502
      }
    });
  }
});

app.post('/v1/chat/completions', async (req, res) => {
  let streamEndedCleanly = false;
  let upstreamStream = null;

  try {
    const { model, messages, temperature, max_tokens, stream, ...rest } = req.body;

    // Pass the request through as-is — no model remapping
    const forwardBody = {
      model,
      messages,
      temperature: temperature ?? 0.7,
      max_tokens: max_tokens ?? 2048,
      stream: stream || false,
      ...rest  // preserve any extra fields JanitorAI sends
    };

    console.log('[PROXY] Forwarding request → model:', model, '| stream:', !!stream);

    const upstreamResponse = await axios.post(
      `${PROXY_BASE}/v1/chat/completions`,
      forwardBody,
      {
        headers: {
          // Use the proxy's own token, not the client's
          Authorization: PROXY_AUTH_KEY ? `Bearer ${PROXY_AUTH_KEY}` : req.headers.authorization,
          'Content-Type': 'application/json'
        },
        responseType: stream ? 'stream' : 'json',
        timeout: REQUEST_TIMEOUT_MS
      }
    );

    upstreamStream = upstreamResponse.data;

    // ── Streaming response ────────────────────────────────────────────────────

    if (stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      const decoder = new StringDecoder('utf8');
      let buffer = '';
      let doneSent = false;
      let cleanedUp = false;

      const cleanup = () => {
        if (cleanedUp) return;
        cleanedUp = true;
        if (upstreamStream) upstreamStream.removeAllListeners();
        req.removeAllListeners('close');
      };

  
      const processLine = (line) => {
  if (!line.startsWith('data: ')) return;

  if (line.includes('[DONE]')) {
    if (!doneSent) {
      safeWrite(res, 'data: [DONE]\n\n');
      doneSent = true;
    }
    streamEndedCleanly = true;
    return;
  }

  try {
    const data = JSON.parse(line.slice(6));

    // Convert reasoning_content into normal content.
    // If both exist, reasoning comes first.
    if (Array.isArray(data.choices)) {
      for (const choice of data.choices) {
        if (!choice.delta) continue;

        const reasoning = choice.delta.reasoning_content;
        const content = choice.delta.content;

        if (reasoning != null) {
          choice.delta.content =
            String(reasoning) +
            (content != null ? String(content) : '');

          delete choice.delta.reasoning_content;
        }
      }
    }

    safeWrite(res, `data: ${JSON.stringify(data)}\n\n`);
  } catch {
    console.warn(
      '[STREAM] Skipping malformed chunk:',
      line.slice(0, 100)
    );
  }
};

      upstreamStream.on('data', chunk => {
        buffer += decoder.write(chunk);

        if (buffer.length > MAX_BUFFER_SIZE) {
          console.error('[STREAM] Buffer overflow, closing connection');
          safeWrite(res, 'data: [DONE]\n\n');
          res.end();
          upstreamStream.destroy();
          cleanup();
          return;
        }

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) processLine(line);
      });

      upstreamStream.on('end', () => {
        buffer += decoder.end();
        if (buffer.trim()) {
          for (const line of buffer.split('\n')) processLine(line);
        }
        if (!doneSent) safeWrite(res, 'data: [DONE]\n\n');
        streamEndedCleanly = true;
        if (!res.writableEnded) res.end();
        cleanup();
      });

      upstreamStream.on('error', err => {
        console.error('[STREAM] Upstream error:', err.message);
        if (!res.writableEnded) {
          safeWrite(res, `data: ${JSON.stringify({ error: { message: 'Stream interrupted', type: 'stream_error' } })}\n\n`);
          safeWrite(res, 'data: [DONE]\n\n');
          res.end();
        }
        cleanup();
      });

      req.on('close', () => {
        if (!streamEndedCleanly && (req.destroyed || !res.writable)) {
          console.warn('[STREAM] Client disconnected early');
        }
        if (upstreamStream && !upstreamStream.destroyed && !streamEndedCleanly) {
          upstreamStream.destroy();
        }
        cleanup();
      });

    // ── Non-streaming response ────────────────────────────────────────────────

    } else {
      res.json(upstreamResponse.data);
    }

  } catch (error) {
    console.error('[PROXY] Error:', error.message);
    if (error.response?.data) console.error('[PROXY] Upstream response:', error.response.data);

    if (!res.headersSent) {
      res.status(error.response?.status || 502).json({
        error: {
          message: error.message,
          type: 'proxy_error',
          code: error.response?.status || 502
        }
      });
    } else if (!res.writableEnded) {
      safeWrite(res, `data: ${JSON.stringify({ error: { message: error.message, type: 'proxy_error' } })}\n\n`);
      safeWrite(res, 'data: [DONE]\n\n');
      res.end();
    }

    if (upstreamStream && !upstreamStream.destroyed) {
      upstreamStream.destroy();
    }
  }
});

// ─── 404 fallback ─────────────────────────────────────────────────────────────

app.use((req, res) => {
  res.status(404).json({
    error: {
      message: `Endpoint ${req.method} ${req.path} not found`,
      type: 'invalid_request_error',
      code: 404
    }
  });
});

// ─── Startup ──────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`[PROXY] JanitorAI bridge running on port ${PORT}`);
  console.log(`[PROXY] Forwarding to: ${PROXY_BASE}`);
});
