const fs = require('fs');
const path = require('path');

const NODE_ENV = 'development';
const MIN_UPTIME = 60000; // 1 minute in milliseconds

// Get signers count - try settings.json first, fallback to VPA_SIGNER_ADDRESSES env var
let signersCount = 1;

// Try to read from settings.json (for standalone usage)
const settingsPath = path.join(__dirname, 'settings', 'settings.json');
if (fs.existsSync(settingsPath)) {
  try {
    const settings = require(settingsPath);
    if (settings.signers && Array.isArray(settings.signers)) {
      signersCount = settings.signers.length;
    }
  } catch (e) {
    // Fall through to env var fallback
  }
}

// Fallback to VPA_SIGNER_ADDRESSES env var (for DSV node usage)
if (signersCount === 1) {
  const signerAddresses = process.env.VPA_SIGNER_ADDRESSES || '';
  if (signerAddresses) {
    signersCount = signerAddresses.split(',').filter(addr => addr.trim()).length;
  }
}

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
