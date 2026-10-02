/**
 * Spinning Door Prize — WebSocket & HTTP Server
 * Zero external dependencies (Pure Node.js built-in modules)
 * Enables real-time synchronization between Operator and Projector displays
 * across different browsers, laptops, tablets, and mobile devices on LAN/Wi-Fi.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

const PORT = parseInt(process.env.PORT || '3000', 10);
const ROOT_DIR = __dirname;

// MIME types dictionary
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.wav': 'audio/wav',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.csv': 'text/plain; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8'
};

// Discover local IPv4 addresses
function getLocalIpAddresses() {
  const interfaces = os.networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name] || []) {
      if (iface.family === 'IPv4' && !iface.internal) {
        addresses.push(iface.address);
      }
    }
  }
  return addresses;
}

// Active WebSocket clients set
const clients = new Set();

// Cached sync states for instant synchronization upon new screen connection
let cachedState = {
  category: null,
  slots: null,
  batchNumber: 1,
  grandPrize: null,
  lastAction: null
};

// WebSocket Frame Builders & Parsers (RFC 6455 Compliant)
function sendWsFrame(socket, opcode, payloadBuffer) {
  if (socket.destroyed || !socket.writable) return;
  const len = payloadBuffer.length;
  let header;

  if (len <= 125) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = len;
  } else if (len <= 65535) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | (opcode & 0x0f);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  try {
    socket.write(Buffer.concat([header, payloadBuffer]));
  } catch (err) {
    console.error('Socket write error:', err.message);
  }
}

function sendWsText(socket, text) {
  sendWsFrame(socket, 0x01, Buffer.from(text, 'utf8'));
}

function broadcastStats() {
  let operators = 0;
  let displays = 0;
  for (const client of clients) {
    if (client.role === 'operator') operators++;
    else if (client.role === 'display') displays++;
  }

  const statsMsg = JSON.stringify({
    type: 'SERVER_STATS',
    clients: {
      total: clients.size,
      operators,
      displays
    },
    timestamp: Date.now()
  });

  for (const client of clients) {
    sendWsText(client.socket, statsMsg);
  }
}

function broadcastMessage(senderClient, rawText) {
  for (const client of clients) {
    if (client !== senderClient) {
      sendWsText(client.socket, rawText);
    }
  }
}

// HTTP Server
const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  // API endpoint for server info & LAN IPs
  if (parsedUrl.pathname === '/api/info') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    let operators = 0;
    let displays = 0;
    for (const c of clients) {
      if (c.role === 'operator') operators++;
      else if (c.role === 'display') displays++;
    }
    res.end(JSON.stringify({
      port: PORT,
      localIps: getLocalIpAddresses(),
      clients: {
        total: clients.size,
        operators,
        displays
      }
    }));
    return;
  }

  // Serve static files
  let safePath = path.normalize(decodeURIComponent(parsedUrl.pathname));
  if (safePath === '/' || safePath === '\\') {
    safePath = '/index.html';
  }

  const filePath = path.join(ROOT_DIR, safePath);

  // Security check: ensure path is within ROOT_DIR
  if (!filePath.startsWith(ROOT_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('403 Forbidden');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    res.writeHead(200, {
      'Content-Type': contentType,
      'Content-Length': stats.size,
      'Cache-Control': 'no-cache'
    });

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  });
});

// WebSocket Upgrade Handler
server.on('upgrade', (req, socket, head) => {
  if (req.headers['upgrade'] && req.headers['upgrade'].toLowerCase() === 'websocket') {
    const key = req.headers['sec-websocket-key'];
    if (!key) {
      socket.destroy();
      return;
    }

    const acceptKey = crypto
      .createHash('sha1')
      .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
      .digest('base64');

    const headers = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${acceptKey}`,
      '\r\n'
    ];

    socket.write(headers.join('\r\n'));

    const client = {
      id: 'client-' + Date.now() + '-' + Math.random().toString(36).substr(2, 6),
      socket,
      role: 'unknown',
      connectedAt: Date.now()
    };

    clients.add(client);
    broadcastStats();

    // Stream buffer for incoming frames
    let buffer = Buffer.alloc(0);

    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      while (buffer.length >= 2) {
        const firstByte = buffer[0];
        const secondByte = buffer[1];
        const opcode = firstByte & 0x0f;
        const isMasked = (secondByte & 0x80) !== 0;
        let payloadLen = secondByte & 0x7f;
        let headerLen = 2;

        if (payloadLen === 126) {
          if (buffer.length < 4) break;
          payloadLen = buffer.readUInt16BE(2);
          headerLen = 4;
        } else if (payloadLen === 127) {
          if (buffer.length < 10) break;
          payloadLen = Number(buffer.readBigUInt64BE(2));
          headerLen = 10;
        }

        if (isMasked) {
          headerLen += 4;
        }

        if (buffer.length < headerLen + payloadLen) {
          break; // wait for remaining frame bytes
        }

        let payload = buffer.subarray(headerLen, headerLen + payloadLen);

        if (isMasked) {
          const mask = buffer.subarray(headerLen - 4, headerLen);
          const unmasked = Buffer.alloc(payloadLen);
          for (let i = 0; i < payloadLen; i++) {
            unmasked[i] = payload[i] ^ mask[i % 4];
          }
          payload = unmasked;
        }

        // Advance buffer
        buffer = buffer.subarray(headerLen + payloadLen);

        // Process Opcode
        if (opcode === 0x08) {
          // Connection Close
          socket.end();
          break;
        } else if (opcode === 0x09) {
          // Ping -> Reply Pong
          sendWsFrame(socket, 0x0a, payload);
        } else if (opcode === 0x0a) {
          // Pong
        } else if (opcode === 0x01) {
          // Text Frame
          const text = payload.toString('utf8');
          try {
            const data = JSON.parse(text);

            if (data.type === 'CLIENT_HELLO') {
              client.role = data.role || client.role;
              if (data.clientId) client.id = data.clientId;
              broadcastStats();

              // If display connected, send cached state if exists
              if (client.role === 'display') {
                if (cachedState.lastAction) {
                  sendWsText(client.socket, JSON.stringify(cachedState.lastAction));
                }
                // Also forward REQUEST_CURRENT_STATE to operator so fresh state is synced
                broadcastMessage(client, JSON.stringify({
                  type: 'REQUEST_CURRENT_STATE',
                  target: 'operator',
                  timestamp: Date.now()
                }));
              }
              continue;
            }

            if (data.type === 'REQUEST_CURRENT_STATE') {
              if (cachedState.lastAction) {
                sendWsText(client.socket, JSON.stringify(cachedState.lastAction));
              }
              broadcastMessage(client, text);
              continue;
            }

            // Cache important persistent state
            if (['CATEGORY_CHANGE', 'SLOTS_PREPARED', 'GRAND_PRIZE_INIT', 'GRAND_PRIZE_STATUS'].includes(data.type)) {
              cachedState.lastAction = data;
            } else if (data.type === 'EVENT_RESET' || data.type === 'SESSION_CLOSED') {
              cachedState.lastAction = data;
            }

            // Relay message to all other connected clients
            broadcastMessage(client, text);

          } catch (err) {
            console.error('Error processing WS frame:', err.message);
          }
        }
      }
    });

    const cleanup = () => {
      clients.delete(client);
      broadcastStats();
    };

    socket.on('close', cleanup);
    socket.on('error', cleanup);
  } else {
    socket.destroy();
  }
});

// Periodic Ping to keep WebSocket connections alive across firewalls/routers
setInterval(() => {
  for (const client of clients) {
    if (client.socket.writable) {
      sendWsFrame(client.socket, 0x09, Buffer.alloc(0));
    }
  }
}, 25000);

// Start server
server.listen(PORT, '0.0.0.0', () => {
  const localIps = getLocalIpAddresses();
  console.log('\n================================================================');
  console.log('  🎰 DOOR PRIZE — WEBSOCKET & DUAL SCREEN SERVER AKTIF');
  console.log('================================================================');
  console.log('  Buka di Komputer Ini (Localhost):');
  console.log(`    👉 Operator:    http://localhost:${PORT}/`);
  console.log(`    👉 Proyektor:   http://localhost:${PORT}/#/display\n`);

  if (localIps.length > 0) {
    console.log('  Buka di Komputer / Device Lain (Laptop Proyektor, HP, Tablet di Wi-Fi/LAN):');
    localIps.forEach(ip => {
      console.log(`    👉 Operator:    http://${ip}:${PORT}/`);
      console.log(`    👉 Proyektor:   http://${ip}:${PORT}/#/display`);
    });
  } else {
    console.log('  (Tidak terdeteksi IP Wi-Fi/LAN eksternal, gunakan localhost)');
  }
  console.log('================================================================\n');
});
