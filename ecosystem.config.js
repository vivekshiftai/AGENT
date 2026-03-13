// PM2 Ecosystem Configuration for InsightForge Analytics Backend
// This ensures proper WebSocket timeout settings for long-running LLM queries
// Configured with multiple workers for horizontal scaling

module.exports = {
  apps: [{
    name: 'analytics-backend',
    // Use uvicorn with WebSocket-specific settings and multiple workers for scaling
    script: 'uvicorn',
    args: [
      'src.main:app',
      '--host', '0.0.0.0',
      '--port', '2345',
      '--workers', '2',                        // Reduced to 2 workers to prevent memory issues (was 4)
      '--timeout-keep-alive', '0',             // No keep-alive timeout
      '--ws-ping-interval', '25',              // Send WebSocket ping every 25 seconds (aligned with application heartbeat)
      '--ws-ping-timeout', '3600',             // Wait 60 minutes for pong response (supports 60+ min queries)
      '--timeout-graceful-shutdown', '30',      // Graceful shutdown timeout
      '--limit-max-requests', '1000',          // Restart workers after 1000 requests to prevent memory leaks
    ],
    cwd: '/home/azureuser/dash/analytics-backend',
    interpreter: '/home/azureuser/dash/analytics-backend/venv/bin/python3',
    // Environment variables
    env: {
      NODE_ENV: 'production',
      PYTHONUNBUFFERED: '1',
    },
    // Auto-restart settings
    autorestart: true,
    watch: false,
    // No memory limit - workers can use as much memory as needed
    min_uptime: '10s',                          // Minimum uptime before considering process stable
    max_restarts: '10',                          // Max restarts in 1 minute
    restart_delay: '4000',                       // Delay between restarts (ms)
    // Scaling: PM2 manages 1 instance, uvicorn spawns multiple worker processes
    // Each worker can handle concurrent requests independently
    instances: 1,                                // Keep at 1 - uvicorn handles worker processes
    exec_mode: 'fork',                          // Fork mode (not cluster) - uvicorn manages workers
    // Logging
    error_file: '/home/azureuser/.pm2/logs/analytics-backend-error.log',
    out_file: '/home/azureuser/.pm2/logs/analytics-backend-out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    // Merge stdout and stderr
    merge_logs: true,
  }]
};

