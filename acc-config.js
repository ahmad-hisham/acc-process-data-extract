let config = {};

config.credentials = {};
config.credentials.client_id     = process.env.FORGE_CLIENT_ID;
config.credentials.client_secret = process.env.FORGE_CLIENT_SECRET;
config.scopes = ["data:read"];

module.exports = config;