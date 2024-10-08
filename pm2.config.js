const NODE_ENV = process.env.NODE_ENV || 'development';

const MAX_RESTART = 10;
const MIN_UPTIME = 60000;


module.exports = {
  apps : [
    {
      name   : "relayer-api",
      script: `poetry run python -m gunicorn_launcher`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      kill_timeout : 3000,
      env: {
        NODE_ENV: NODE_ENV,
      },
    },
    {
      name   : "tx_launcher_core",
      script: `poetry run python -m tx_launcher_core`,
      max_restarts: MAX_RESTART,
      min_uptime: MIN_UPTIME,
      env: {
        NODE_ENV: NODE_ENV,
      },
    },
  ]
}
