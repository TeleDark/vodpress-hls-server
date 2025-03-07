const fs = require('fs').promises;
const crypto = require('crypto');
const path = require('path');
const dotenv = require('dotenv'); class APIKeyManager {
    constructor() {
        this.apiKeys = new Map();
        this.envPath = path.join(__dirname, '../../.env');
    }

    // Generate a secure API key
    generateAPIKey() {
        return crypto.randomBytes(32).toString('hex');
    }

    generateKeyHash(apiKey) {
        return crypto.createHash('sha256').update(apiKey).digest('hex');
    }

    // Add a new API key to .env file
    async addNewAPIKey() {
        try {
            const newKey = this.generateAPIKey();

            let envContent = '';
            try {
                envContent = await fs.readFile(this.envPath, 'utf8');
            } catch (error) {
                if (error.code !== 'ENOENT') throw error;
            }

            const currentEnv = dotenv.parse(envContent);
            let currentKeys = currentEnv.API_KEYS ? currentEnv.API_KEYS.split(',') : [];

            currentKeys.push(newKey);

            const newEnvContent = `${envContent}\nAPI_KEYS=${currentKeys.join(',')}\n`;
            await fs.writeFile(this.envPath, newEnvContent);

            dotenv.config();

            this.apiKeys.set(newKey, {
                createdAt: Date.now(),
                lastUsed: null
            });

            return newKey;

        } catch (error) {
            console.error('Failed to add new API key:', error);
            throw new Error('Failed to generate API key');
        }
    }

    async loadAPIKeys() {
        try {
            const envContent = await fs.readFile(this.envPath, 'utf8');
            const env = dotenv.parse(envContent);

            this.apiKeys.clear();

            if (env.API_KEYS) {
                const keys = env.API_KEYS.split(',');
                keys.forEach(key => {
                    this.apiKeys.set(key.trim(), {
                        createdAt: Date.now(),
                        lastUsed: null
                    });
                });
            }

            console.log(`Loaded ${this.apiKeys.size} API keys`);
        } catch (error) {
            console.error('Failed to load API keys:', error);
            throw new Error('Failed to load API keys');
        }
    }

    validateKeyHash(providedHash) {
        for (const apiKey of this.apiKeys.keys()) {
            const currentHash = this.generateKeyHash(apiKey);
            if (currentHash === providedHash) {
                return true;
            }
        }
        return false;
    }

    async removeAPIKey(keyToRemove) {
        try {
            const envContent = await fs.readFile(this.envPath, 'utf8');
            const env = dotenv.parse(envContent);

            let currentKeys = env.API_KEYS ? env.API_KEYS.split(',') : [];
            currentKeys = currentKeys.filter(key => key !== keyToRemove);

            const newEnvContent = envContent.replace(
                /API_KEYS=.*/,
                `API_KEYS=${currentKeys.join(',')}`
            );

            await fs.writeFile(this.envPath, newEnvContent);

            this.apiKeys.delete(keyToRemove);

            return true;
        } catch (error) {
            console.error('Failed to remove API key:', error);
            throw new Error('Failed to remove API key');
        }
    }

}// Create and export singleton instance
const apiKeyManager = new APIKeyManager();// Middleware for API key authentication
const authenticateAPIKey = async (req, res, next) => {
    try {
        const apiKeyHash = req.headers['x-api-key-hash'];

        if (!apiKeyHash) {
            return res.status(401).json({
                success: false,
                error: 'API key hash required'
            });
        }

        if (!apiKeyManager.validateKeyHash(apiKeyHash)) {
            return res.status(401).json({
                success: false,
                error: 'Invalid API key hash'
            });
        }

        next();
    } catch (error) {
        return res.status(500).json({
            success: false,
            error: error.message
        });
    }

}; module.exports = {
    apiKeyManager,
    authenticateAPIKey
};
