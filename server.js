const ffmpeg = require('fluent-ffmpeg');
const AWS = require('aws-sdk');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const express = require('express');
const app = express();
const { apiKeyManager, authenticateAPIKey } = require('./src/auth/api-key-manager');
const processingManager = require('./src/processing-manager');

let cleanupInterval;
app.use(express.json());
require('dotenv').config();

// Initialize API Key Manager when server starts
async function initServer() {
    try {
        await apiKeyManager.loadAPIKeys();
        console.log('API Keys loaded successfully');

        if (apiKeyManager.apiKeys.size === 0) {
            await apiKeyManager.addNewAPIKey();
            console.log('New API key generated');
        }
    } catch (error) {
        console.error('Failed to initialize API key manager:', error);
        process.exit(1);
    }
}

// AWS S3 Configuration
const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    endpoint: process.env.S3_ENDPOINT,
    s3ForcePathStyle: true,
    signatureVersion: 'v4'
});

// Check and configure FFMPEG
let ffmpegPath = process.env.FFMPEG_PATH || '/usr/local/bin/ffmpeg';
if (!fs.existsSync(ffmpegPath)) {
    const commonPaths = ['/usr/bin/ffmpeg', '/opt/local/bin/ffmpeg'];
    for (const path of commonPaths) {
        if (fs.existsSync(path)) {
            ffmpegPath = path;
            break;
        }
    }
}
if (!fs.existsSync(ffmpegPath)) {
    console.error('FFMPEG not found. Please install FFMPEG or set correct FFMPEG_PATH');
    process.exit(1);
}
ffmpeg.setFfmpegPath(ffmpegPath);

// Set up temporary directory
const TEMP_DIR = path.join(__dirname, 'temp');
if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR, { recursive: true });
}

// Video Download Function
async function downloadVideo(url, outputPath) {
    const writer = fs.createWriteStream(outputPath);
    try {
        const response = await axios({
            method: 'GET',
            url,
            responseType: 'stream',
            timeout: 300000,
            onDownloadProgress: (progressEvent) => {
                const percentage = Math.round((progressEvent.loaded * 100) / progressEvent.total);
                console.log(`Download Progress: ${percentage}%`);
            }
        });

        response.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
        });
    } catch (error) {
        writer.end();
        if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
        throw new Error(`Failed to download video: ${error.message}`);
    }
}

// Convert to HLS Function
function convertToHLS(inputPath, outputDir) {
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

    const qualities = ['1080p', '720p', '480p'];
    qualities.forEach(quality => {
        const qualityDir = path.join(outputDir, quality);
        if (!fs.existsSync(qualityDir)) fs.mkdirSync(qualityDir, { recursive: true });
    });

    return new Promise((resolve, reject) => {
        let conversionStarted = false;
        let conversionTimeout;

        const conversion = ffmpeg(inputPath)
            .outputOptions([
                '-preset faster',
                '-threads 0',
                '-movflags +faststart',
                '-max_muxing_queue_size 1024',
                '-probesize 10M',
                '-analyzeduration 10M',
                '-force_key_frames expr:gte(t,n_forced*2)',
                '-sc_threshold 0',
                '-g 48',
                '-keyint_min 48'
            ])
            .outputOptions([
                '-filter_complex', '[0:v]split=3[v1][v2][v3];[v1]scale=w=1920:h=1080:flags=lanczos[v1out];[v2]scale=w=1280:h=720:flags=lanczos[v2out];[v3]scale=w=854:h=480:flags=lanczos[v3out]',
                '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '5000k', '-maxrate:v:0', '5350k', '-bufsize:v:0', '7500k', '-crf:v:0', '23', '-profile:v', 'high', '-level:v', '4.1',
                '-map', '[v2out]', '-c:v:1', 'libx264', '-b:v:1', '2800k', '-maxrate:v:1', '2996k', '-bufsize:v:1', '4200k', '-crf:v:1', '23',
                '-map', '[v3out]', '-c:v:2', 'libx264', '-b:v:2', '1400k', '-maxrate:v:2', '1498k', '-bufsize:v:2', '2100k', '-crf:v:2', '23',
                '-map', 'a:0', '-c:a:0', 'aac', '-b:a:0', '192k', '-ac', '2', '-af', 'aresample=async=1000',
                '-map', 'a:0', '-c:a:1', 'aac', '-b:a:1', '128k', '-ac', '2',
                '-map', 'a:0', '-c:a:2', 'aac', '-b:a:2', '96k', '-ac', '2',
                '-f', 'hls', '-hls_time', '6', '-hls_playlist_type', 'vod', '-hls_flags', 'independent_segments+program_date_time', '-hls_segment_type', 'mpegts',
                '-hls_segment_filename', path.join(outputDir, 'stream_%v/data%03d.ts'), '-master_pl_name', 'master.m3u8', '-var_stream_map', 'v:0,a:0 v:1,a:1 v:2,a:2'
            ])
            .output(path.join(outputDir, 'stream_%v/playlist.m3u8'));

        conversion.on('start', () => { conversionStarted = true; });
        conversion.on('progress', (progress) => { console.log(`Processing: ${progress.percent}% done at ${progress.currentFps} fps`); });
        conversion.on('end', () => {
            clearTimeout(conversionTimeout);
            if (!conversionStarted) return reject(new Error('Conversion failed to start'));
            console.log('Conversion completed successfully');
            resolve();
        });
        conversion.on('error', (err) => {
            clearTimeout(conversionTimeout);
            console.error('Conversion error:', err);
            reject(new Error(`Conversion failed: ${err.message}`));
        });

        const cleanupAndExit = () => {
            clearTimeout(conversionTimeout);
            conversion.kill();
        };
        process.on('SIGINT', cleanupAndExit);
        process.on('SIGTERM', cleanupAndExit);

        try {
            conversion.run();
        } catch (error) {
            clearTimeout(conversionTimeout);
            reject(error);
        }
    });
}

// Upload to S3 Function
async function uploadToS3(directory, videoId, videoData) {
    const s3BaseUrl = process.env.S3_ENDPOINT;
    const s3FolderPath = `videos/${videoId}`;

    try {
        async function getFiles(dir) {
            const items = fs.readdirSync(dir, { withFileTypes: true });
            let files = [];
            for (const item of items) {
                const fullPath = path.join(dir, item.name);
                if (item.isDirectory()) files = files.concat(await getFiles(fullPath));
                else files.push(fullPath);
            }
            return files;
        }

        const allFiles = await getFiles(directory);
        console.log('Files to upload:', allFiles);

        for (const filePath of allFiles) {
            const relativePath = path.relative(directory, filePath);
            const s3Path = `${s3FolderPath}/${relativePath.replace(/\\/g, '/')}`;
            console.log(`Uploading ${filePath} to ${s3Path}`);

            const fileStream = fs.createReadStream(filePath);
            const contentType = path.extname(filePath) === '.m3u8' ? 'application/x-mpegURL' :
                path.extname(filePath) === '.ts' ? 'video/MP2T' : 'application/octet-stream';

            const params = {
                Bucket: process.env.S3_BUCKET,
                Key: s3Path,
                Body: fileStream,
                ContentType: contentType
            };
            await s3.upload(params).promise();
            console.log(`Successfully uploaded ${s3Path}`);
        }

        const publicUrlBase = videoData?.public_url_base || s3BaseUrl;
        const masterPlaylistUrl = `${publicUrlBase}/${s3FolderPath}/master.m3u8`;
        console.log('Master playlist URL:', masterPlaylistUrl);
        return masterPlaylistUrl;
    } catch (error) {
        console.error('S3 upload error:', error);
        throw new Error(`S3 upload failed: ${error.message}`);
    }
}

// Cleanup Function
async function cleanup(videoPath, outputDir) {
    try {
        if (fs.existsSync(videoPath)) {
            await fs.promises.unlink(videoPath);
            console.log(`Cleaned up video file: ${videoPath}`);
        }
        if (fs.existsSync(outputDir)) {
            await fs.promises.rm(outputDir, { recursive: true, force: true });
            console.log(`Cleaned up output directory: ${outputDir}`);
        }
    } catch (error) {
        console.error(`Cleanup error: ${error.message}`);
    }
}

// Middleware to log error responses
app.use((req, res, next) => {
    const originalSend = res.send;
    res.send = function (data) {
        if (res.statusCode >= 400) console.error(`Error response(${res.statusCode}):`, data);
        return originalSend.call(this, data);
    };
    next();
});

// API Route for Video Conversion
app.post('/api/convert', authenticateAPIKey, async (req, res) => {
    try {
        console.log('Received request body:', req.body);
        const { video_url, video_id, callback_url, site_url } = req.body;

        if (!video_url || !video_id || !callback_url) {
            return res.status(400).json({ success: false, error: 'Missing required parameters' });
        }

        if (processingManager.isProcessing()) {
            const currentVideo = processingManager.getProcessingVideo();
            return res.status(429).json({
                success: false,
                error: 'Another video is currently being processed',
                current_video_id: currentVideo,
                retry_after: 300
            });
        }

        if (!processingManager.startProcessing(video_id, callback_url)) {
            return res.status(429).json({ success: false, error: 'Failed to start processing, please try again later' });
        }

        const response = { success: true, message: 'Video processing started', video_id };
        console.log('Sending response:', response);
        res.json(response);

        processVideoInBackground(video_url, video_id, callback_url, { site_url })
            .then(() => processingManager.completeProcessing(video_id))
            .catch(error => {
                console.error('Error processing video:', error);
                processingManager.completeProcessing(video_id);
                sendCallback(callback_url, { video_id, status: 'failed', error: error.message })
                    .catch(callbackError => console.error('Error sending failure callback:', callbackError));
            });
    } catch (error) {
        processingManager.resetProcessing();
        console.error('Error in /api/convert:', error);
        return res.status(500).json({ success: false, error: error.message });
    }
});

// Process Video in Background
async function processVideoInBackground(video_url, video_id, callback_url, videoData) {
    const tempFilePath = path.join(TEMP_DIR, `video_${video_id}.mp4`);
    const outputDir = path.join(TEMP_DIR, `hls_${video_id}`);

    const updateStatus = async (status, error = null, conversion_url = null) => {
        try {
            const payload = { video_id, status, ...(error && { error: error.message }), ...(conversion_url && { conversion_url }) };
            await sendCallback(callback_url, payload);
        } catch (callbackError) {
            console.error(`Failed to send callback for status ${status}:`, callbackError);
        }
    };

    try {
        await updateStatus('downloading');
        await Promise.race([
            downloadVideo(video_url, tempFilePath),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Download timeout after 30 minutes')), 1800000))
        ]);

        await updateStatus('converting');
        await Promise.race([
            convertToHLS(tempFilePath, outputDir),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Conversion timeout after 90 minutes')), 5400000))
        ]);

        await updateStatus('uploading');
        const s3Url = await Promise.race([
            uploadToS3(outputDir, video_id, videoData),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Upload timeout after 60 minutes')), 3600000))
        ]);

        await updateStatus('completed', null, s3Url);
        await cleanup(tempFilePath, outputDir);
        return s3Url;
    } catch (error) {
        console.error(`Error processing video ${video_id}:`, error);
        const formattedError = { message: error.message || 'Unknown error occurred', code: error.code || 'UNKNOWN_ERROR', stack: error.stack };
        await updateStatus('failed', formattedError);
        await cleanup(tempFilePath, outputDir);
        throw error;
    }
}

// Send Callback Function
async function sendCallback(callbackUrl, data, maxRetries = 3) {
    let attempt = 0;
    while (attempt < maxRetries) {
        try {
            const payload = { ...data, timestamp: Date.now(), source: 'vodpress-conversion-server' };
            
            // Set request headers
            const headers = {
                'X-API-Key-Hash': apiKeyManager.generateKeyHash(process.env.API_KEYS.split(',')[0]),
                'Content-Type': 'application/json'
            };

            // Add Authorization header for HTTP Basic Auth
            if (process.env.BASIC_AUTH_USERNAME && process.env.BASIC_AUTH_PASSWORD) {
                const auth = Buffer.from(`${process.env.BASIC_AUTH_USERNAME}:${process.env.BASIC_AUTH_PASSWORD}`).toString('base64');
                headers['Authorization'] = `Basic ${auth}`;
            }
            
            const response = await axios({
                method: 'POST',
                url: callbackUrl,
                data: payload,
                headers,
                timeout: 30000,
                validateStatus: false
            });

            if (response.status !== 200) throw new Error(`HTTP ${response.status}: ${JSON.stringify(response.data)}`);
            return true;
        } catch (error) {
            attempt++;
            console.error(`Callback attempt ${attempt} failed:`, error.message);
            if (attempt >= maxRetries) throw new Error(`Failed to send callback after ${maxRetries} attempts: ${error.message}`);
            await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
        }
    }
}

// Cleanup Temporary Files
async function cleanupTempFiles() {
    const MAX_AGE = 24 * 60 * 60 * 1000; // 24 hours
    console.log('Starting temporary files cleanup...');

    try {
        const files = await fs.promises.readdir(TEMP_DIR, { withFileTypes: true });
        for (const file of files) {
            const filePath = path.join(TEMP_DIR, file.name);
            try {
                const stats = await fs.promises.stat(filePath);
                if (Date.now() - stats.mtime.getTime() > MAX_AGE) {
                    if (file.isDirectory()) {
                        await fs.promises.rm(filePath, { recursive: true, force: true });
                        console.log(`Successfully deleted old temp directory: ${filePath}`);
                    } else {
                        await fs.promises.unlink(filePath);
                        console.log(`Successfully deleted old temp file: ${filePath}`);
                    }
                }
            } catch (err) {
                console.error(`Error processing ${filePath}:`, err);
            }
        }
    } catch (err) {
        console.error('Error reading temp directory:', err);
    }
}

// Mark Video as Failed on Shutdown
async function markVideoAsFailed(videoId) {
    try {
        const callbackUrl = processingManager.getCallbackUrl(videoId);
        if (!callbackUrl) {
            console.error(`No callback URL found for video ${videoId}`);
            return;
        }
        await sendCallback(callbackUrl, { video_id: videoId, status: 'failed', error: 'Server shutdown during processing' });
        console.log(`Successfully marked video ${videoId} as failed during shutdown`);
    } catch (error) {
        console.error(`Failed to mark video ${videoId} as failed:`, error);
    }
}

// Shutdown Server Gracefully
async function shutdownServer() {
    console.log('Shutting down server...');
    const currentVideoId = processingManager.getProcessingVideo();

    if (currentVideoId) {
        console.log(`Found video ${currentVideoId} still processing during shutdown`);
        try {
            await markVideoAsFailed(currentVideoId);
        } catch (error) {
            console.error('Error marking video as failed during shutdown:', error);
        }
    }

    if (cleanupInterval) clearInterval(cleanupInterval);
    await new Promise(resolve => setTimeout(resolve, 2000));

    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });

    setTimeout(() => {
        console.error('Forcing server shutdown after timeout');
        process.exit(1);
    }, 10000);
}

process.on('SIGTERM', shutdownServer);
process.on('SIGINT', shutdownServer);

// Start Server
const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, async () => {
    console.log(`Server is running on port ${PORT}`);
    await initServer();
    cleanupInterval = setInterval(cleanupTempFiles, 60 * 60 * 1000);
    cleanupTempFiles();
});