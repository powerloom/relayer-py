const NODE_ENV = 'development';
const MIN_UPTIME = 60000; // 1 minute in milliseconds

// Read signers count from environment variables
const signerAddresses = process.env.VPA_SIGNER_ADDRESSES || '';
const signersCount = signerAddresses ? signerAddresses.split(',').filter(addr => addr.trim()).length : 1;

// Common configuration for all apps
const commonConfig = {
  min_uptime: MIN_UPTIME,
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
    signersCount),
];

// Filter out null configurations (zero count services)
const validAppConfigs = appConfigs.filter(config => config !== null);

module.exports = {
  apps: validAppConfigs,
};
