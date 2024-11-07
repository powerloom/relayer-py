const NODE_ENV = 'development';
const MIN_UPTIME = 60000; // 1 minute in milliseconds
const settings = require('./settings/settings.json');

const { signers } = settings;

// Common configuration for all apps
const commonConfig = {
  min_uptime: MIN_UPTIME,
  error_file: "/dev/null",
  out_file: "/dev/null",
  env: {
    NODE_ENV: NODE_ENV,
  },
};

// Helper function to create app configurations
const createAppConfig = (name, script, additionalConfig = {}) => ({
  name,
  script: `poetry run python -m ${script}`,
  ...commonConfig,
  ...additionalConfig,
});

// Helper function to create worker configurations
const createWorkerConfig = (name, script, instances) => {
  if (instances === 0) {
    return null; // Return null for zero count services
  }
  return createAppConfig(name, script, {
    instances,
  });
};


// Create app configurations
const appConfigs = [
  createAppConfig("relayer-api", "gunicorn_launcher", {
  }),
  createWorkerConfig("tx_worker", "tx_worker",
    signers.length),
  createWorkerConfig("tx_checker", "tx_checker",
    signers.length),
];

// Filter out null configurations (zero count services)
const validAppConfigs = appConfigs.filter(config => config !== null);

module.exports = {
  apps: validAppConfigs,
};
