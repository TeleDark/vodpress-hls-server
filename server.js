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

// Get video duration in seconds
async function getVideoDuration(inputPath) {
    return new Promise((resolve, reject) => {
        ffmpeg.ffprobe(inputPath, (err, metadata) => {
            if (err) {
                console.error('Error getting video duration:', err);
                return reject(err);
            }

            // Duration is in seconds
            const duration = Math.round(metadata.format.duration);
            console.log(`Video duration: ${duration} seconds`);
            resolve(duration);
        });
    });
}

// Get video resolution function
async function getVideoResolution(inputPath) {
    return new Promise((resolve, reject) => {
        ffmpeg.ffprobe(inputPath, (err, metadata) => {
            if (err) {
                console.error('Error getting video resolution:', err);
                return reject(err);
            }

            // Get video stream info
            const videoStream = metadata.streams.find(s => s.codec_type === 'video');
            if (!videoStream) {
                return reject(new Error('No video stream found'));
            }

            const width = videoStream.width;
            const height = videoStream.height;
            const isVertical = height > width;
            resolve({ width, height, isVertical });
        });
    });
}

// Convert to HLS Function
async function convertToHLS(inputPath, outputDir) {
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

    // Get video resolution first
    let is4K = false;
    let isVertical = false;
    try {
        const resolution = await getVideoResolution(inputPath);
        // Check if video is 4K (width ≥ 3840 or height ≥ 2160)
        is4K = resolution.width >= 3840 || resolution.height >= 2160;
        isVertical = resolution.isVertical;
    } catch (error) {
        console.error('Error determining video resolution:', error);
    }

    const qualities = is4K ? ['4K', '1080p', '720p', '480p'] : ['1080p', '720p', '480p'];
    qualities.forEach(quality => {
        const qualityDir = path.join(outputDir, quality);
        if (!fs.existsSync(qualityDir)) fs.mkdirSync(qualityDir, { recursive: true });
    });

    return new Promise((resolve, reject) => {
        let conversionStarted = false;
        let conversionTimeout;

        // For vertical videos
        let scaleFilters;
        if (isVertical) {
            if (is4K) {
                scaleFilters = '[0:v]split=4[v1][v2][v3][v4];' +
                    '[v1]scale=w=2160:h=3840:force_original_aspect_ratio=decrease,pad=2160:3840:(ow-iw)/2:(oh-ih)/2[v1out];' +
                    '[v2]scale=w=1080:h=1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2[v2out];' +
                    '[v3]scale=w=720:h=1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2[v3out];' +
                    '[v4]scale=w=480:h=854:force_original_aspect_ratio=decrease,pad=480:854:(ow-iw)/2:(oh-ih)/2[v4out]';
            } else {
                scaleFilters = '[0:v]split=3[v1][v2][v3];' +
                    '[v1]scale=w=1080:h=1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2[v1out];' +
                    '[v2]scale=w=720:h=1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2[v2out];' +
                    '[v3]scale=w=480:h=854:force_original_aspect_ratio=decrease,pad=480:854:(ow-iw)/2:(oh-ih)/2[v3out]';
            }
        } else {
            // Horizontal video scaling 
            if (is4K) {
                scaleFilters = '[0:v]split=4[v1][v2][v3][v4];' +
                    '[v1]scale=w=3840:h=2160:force_original_aspect_ratio=decrease,pad=3840:2160:(ow-iw)/2:(oh-ih)/2[v1out];' +
                    '[v2]scale=w=1920:h=1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[v2out];' +
                    '[v3]scale=w=1280:h=720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2[v3out];' +
                    '[v4]scale=w=854:h=480:force_original_aspect_ratio=decrease,pad=854:480:(ow-iw)/2:(oh-ih)/2[v4out]';
            } else {
                scaleFilters = '[0:v]split=3[v1][v2][v3];' +
                    '[v1]scale=w=1920:h=1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[v1out];' +
                    '[v2]scale=w=1280:h=720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2[v2out];' +
                    '[v3]scale=w=854:h=480:force_original_aspect_ratio=decrease,pad=854:480:(ow-iw)/2:(oh-ih)/2[v3out]';
            }
        }

        let conversion = ffmpeg(inputPath)
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
            ]);

        // Configure filter_complex based on whether video is 4K or not
        if (is4K) {
            conversion = conversion.outputOptions([
                '-filter_complex', scaleFilters,
                // 4K stream
                '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '14000k', '-maxrate:v:0', '14980k', '-bufsize:v:0', '21000k', '-crf:v:0', '22', '-profile:v', 'high', '-level:v', '5.1',
                // 1080p stream
                '-map', '[v2out]', '-c:v:1', 'libx264', '-b:v:1', '5000k', '-maxrate:v:1', '5350k', '-bufsize:v:1', '7500k', '-crf:v:1', '23', '-profile:v', 'high', '-level:v', '4.1',
                // 720p stream
                '-map', '[v3out]', '-c:v:2', 'libx264', '-b:v:2', '2800k', '-maxrate:v:2', '2996k', '-bufsize:v:2', '4200k', '-crf:v:2', '23',
                // 480p stream
                '-map', '[v4out]', '-c:v:3', 'libx264', '-b:v:3', '1400k', '-maxrate:v:3', '1498k', '-bufsize:v:3', '2100k', '-crf:v:3', '23',
                // Audio streams
                '-map', 'a:0', '-c:a:0', 'aac', '-b:a:0', '192k', '-ac', '2', '-af', 'aresample=async=1000',
                '-map', 'a:0', '-c:a:1', 'aac', '-b:a:1', '192k', '-ac', '2',
                '-map', 'a:0', '-c:a:2', 'aac', '-b:a:2', '128k', '-ac', '2',
                '-map', 'a:0', '-c:a:3', 'aac', '-b:a:3', '96k', '-ac', '2',
                // HLS settings
                '-f', 'hls', '-hls_time', '6', '-hls_playlist_type', 'vod', '-hls_flags', 'independent_segments+program_date_time', '-hls_segment_type', 'mpegts',
                '-hls_segment_filename', path.join(outputDir, 'stream_%v/data%03d.ts'), '-master_pl_name', 'master.m3u8',
                '-var_stream_map', 'v:0,a:0 v:1,a:1 v:2,a:2 v:3,a:3'
            ]);
        } else {
            // Original settings for non-4K videos
            conversion = conversion.outputOptions([
                '-filter_complex', scaleFilters,
                '-map', '[v1out]', '-c:v:0', 'libx264', '-b:v:0', '5000k', '-maxrate:v:0', '5350k', '-bufsize:v:0', '7500k', '-crf:v:0', '23', '-profile:v', 'high', '-level:v', '4.1',
                '-map', '[v2out]', '-c:v:1', 'libx264', '-b:v:1', '2800k', '-maxrate:v:1', '2996k', '-bufsize:v:1', '4200k', '-crf:v:1', '23',
                '-map', '[v3out]', '-c:v:2', 'libx264', '-b:v:2', '1400k', '-maxrate:v:2', '1498k', '-bufsize:v:2', '2100k', '-crf:v:2', '23',
                '-map', 'a:0', '-c:a:0', 'aac', '-b:a:0', '192k', '-ac', '2', '-af', 'aresample=async=1000',
                '-map', 'a:0', '-c:a:1', 'aac', '-b:a:1', '128k', '-ac', '2',
                '-map', 'a:0', '-c:a:2', 'aac', '-b:a:2', '96k', '-ac', '2',
                '-f', 'hls', '-hls_time', '6', '-hls_playlist_type', 'vod', '-hls_flags', 'independent_segments+program_date_time', '-hls_segment_type', 'mpegts',
                '-hls_segment_filename', path.join(outputDir, 'stream_%v/data%03d.ts'), '-master_pl_name', 'master.m3u8', '-var_stream_map', 'v:0,a:0 v:1,a:1 v:2,a:2'
            ]);
        }

        // Output to playlist
        conversion.output(path.join(outputDir, 'stream_%v/playlist.m3u8'));

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
async function uploadToS3(directory, videoUuid, videoData) {
    const s3BaseUrl = process.env.S3_ENDPOINT;
    const s3FolderPath = `videos/${videoUuid}`;

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
        const { video_url, video_uuid, callback_url, site_url } = req.body;

        if (!video_url || !video_uuid || !callback_url) {
            return res.status(400).json({ success: false, error: 'Missing required parameters' });
        }

        // Check if this video is currently processing
        if (processingManager.getProcessingVideo() === video_uuid) {
            return res.status(200).json({
                success: true,
                message: 'Video is already being processed',
                video_uuid,
                is_processing: true,
                currently_processing: video_uuid,
                queue_position: 0
            });
        }

        // Add video to processing queue
        const videoData = { site_url };
        const queueResult = processingManager.addToQueue(video_uuid, video_url, callback_url, videoData);
        
        let isProcessingThisVideo = false;

        // If the queue was empty and no video is processing, start processing this one immediately
        if (!processingManager.isProcessing()) {
            const nextVideo = processingManager.processNextInQueue();
            if (nextVideo && nextVideo.video_uuid === video_uuid) {
                isProcessingThisVideo = true;

                // Send status update immediately to mark as downloading
                sendCallback(callback_url, {
                    video_uuid: video_uuid,
                    status: 'downloading',
                    message: 'Starting video processing'
                }).catch(error => console.error('Failed to send initial status update:', error));

                // Start processing in background
                processVideoInBackground(nextVideo.videoUrl, nextVideo.video_uuid, nextVideo.callbackUrl, nextVideo.videoData)
                    .then(() => {
                        processingManager.completeProcessing(nextVideo.video_uuid);
                        // Process next video in queue if available
                        processNextVideoInQueue();
                    })
                    .catch(error => {
                        console.error(`Error processing video ${nextVideo.video_uuid}:`, error);
                        processingManager.completeProcessing(nextVideo.video_uuid);
                        sendCallback(nextVideo.callbackUrl, { video_uuid: nextVideo.video_uuid, status: 'failed', error: error.message })
                            .catch(callbackError => console.error('Error sending failure callback:', callbackError));
                        // Process next video in queue if available
                        processNextVideoInQueue();
                    });
            }
        }

        const response = {
            success: true,
            message: isProcessingThisVideo ?
                'Video processing started' : 'Video added to queue',
            video_uuid,
            queue_position: queueResult.position,
            currently_processing: processingManager.getProcessingVideo(),
            is_processing: isProcessingThisVideo
        };

        console.log('Sending response:', response);
        res.json(response);
    } catch (error) {
        console.error('Error in /api/convert:', error);
        return res.status(500).json({ success: false, error: error.message });
    }
});

// Add a new endpoint to get queue status
app.get('/api/queue-status', authenticateAPIKey, (req, res) => {
    try {
        const queueStatus = processingManager.getQueueStatus();
        res.json({
            success: true,
            queue: queueStatus
        });
    } catch (error) {
        console.error('Error getting queue status:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Add endpoint to remove video from queue
app.post('/api/remove-from-queue', authenticateAPIKey, (req, res) => {
    try {
        const { video_uuid } = req.body;

        if (!video_uuid) {
            return res.status(400).json({ success: false, error: 'Video UUID is required' });
        }

        // If video is currently processing, we can't remove it
        if (processingManager.getProcessingVideo() === video_uuid) {
            return res.status(409).json({
                success: false,
                error: 'Video is currently being processed and cannot be removed from queue',
                is_processing: true
            });
        }

        // Remove video from queue
        const removed = processingManager.removeFromQueue(video_uuid);

        res.json({
            success: true,
            removed: removed,
            message: removed ? 'Video removed from queue successfully' : 'Video was not in queue'
        });
    } catch (error) {
        console.error('Error removing video from queue:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Function to process the next video in queue
function processNextVideoInQueue() {
    try {
        const nextVideo = processingManager.processNextInQueue();
        if (nextVideo) {
            // Send status update immediately to mark as downloading
            sendCallback(nextVideo.callbackUrl, {
                video_uuid: nextVideo.video_uuid,
                status: 'downloading',
                message: 'Starting video processing'
            }).catch(error => console.error('Failed to send initial status update:', error));

            processVideoInBackground(nextVideo.videoUrl, nextVideo.video_uuid, nextVideo.callbackUrl, nextVideo.videoData)
                .then(() => {
                    processingManager.completeProcessing(nextVideo.video_uuid);
                    // Process next video in queue
                    processNextVideoInQueue();
                })
                .catch(error => {
                    console.error(`Error processing next video in queue ${nextVideo.video_uuid}:`, error);
                    processingManager.completeProcessing(nextVideo.video_uuid);
                    sendCallback(nextVideo.callbackUrl, { video_uuid: nextVideo.video_uuid, status: 'failed', error: error.message })
                        .catch(callbackError => console.error('Error sending failure callback:', callbackError));
                    // Continue with next video in queue despite error
                    processNextVideoInQueue();
                });
        }
    } catch (error) {
        console.error('Error processing next video in queue:', error);
        // Wait a bit and try again to prevent rapid failure loops
        setTimeout(processNextVideoInQueue, 5000);
    }
}

// Add new endpoint for video deletion
app.post('/api/delete', authenticateAPIKey, async (req, res) => {
    try {
        const { video_uuid } = req.body;

        if (!video_uuid) {
            return res.status(400).json({ success: false, error: 'Video UUID is required' });
        }

        // If video is in queue, remove it
        processingManager.removeFromQueue(video_uuid);

        // If video is currently processing, we can't delete it from S3 yet
        if (processingManager.getProcessingVideo() === video_uuid) {
            return res.status(409).json({
                success: false,
                error: 'Video is currently being processed and cannot be deleted',
                is_processing: true
            });
        }

        const s3FolderPath = `videos/${video_uuid}`;

        // List all objects in the video folder
        const listParams = {
            Bucket: process.env.S3_BUCKET,
            Prefix: s3FolderPath
        };

        const objects = await s3.listObjectsV2(listParams).promise();

        if (objects.Contents.length === 0) {
            return res.status(404).json({ success: false, error: 'Video not found in storage' });
        }

        // Delete all objects in the folder
        const deleteParams = {
            Bucket: process.env.S3_BUCKET,
            Delete: {
                Objects: objects.Contents.map(obj => ({ Key: obj.Key }))
            }
        };

        await s3.deleteObjects(deleteParams).promise();

        res.json({ success: true, message: 'Video deleted successfully' });
    } catch (error) {
        console.error('Error deleting video:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Process Video in Background
async function processVideoInBackground(video_url, video_uuid, callback_url, videoData) {
    const tempFilePath = path.join(TEMP_DIR, `video_${video_uuid}.mp4`);
    const outputDir = path.join(TEMP_DIR, `hls_${video_uuid}`);

    const updateStatus = async (status, error = null, conversion_url = null, duration = null, original_url = null) => {
        try {
            const payload = {
                video_uuid,
                status,
                ...(error && { error: error.message }),
                ...(conversion_url && { conversion_url }),
                ...(duration !== null && { duration }),
                ...(original_url && { original_url })
            };
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

        // Upload original video to S3 before conversion
        await updateStatus('uploading-original', null, null);
        let originalUrl = null;
        try {
            const s3FolderPath = `videos/${video_uuid}/original`;

            // Extract original filename from url
            let originalFilename = 'original.mp4';
            try {
                // Try to get the original filename from the URL
                const parsedUrl = new URL(video_url);
                const urlPath = decodeURIComponent(parsedUrl.pathname);
                const urlFilename = path.basename(urlPath);

                if (urlFilename && urlFilename.length > 0 && urlFilename !== '/' && urlFilename.includes('.')) {
                    const fileExt = path.extname(urlFilename);
                    const filenameWithoutExt = path.basename(urlFilename, fileExt);
                    const cleanedFilename = filenameWithoutExt.replace(/\s+/g, '-');
                    originalFilename = `${cleanedFilename}_${video_uuid}${fileExt}`;
                } else {
                    originalFilename = `video_${video_uuid}.mp4`;
                }
            } catch (error) {
                console.log('Could not extract filename from URL, using default name');
                originalFilename = `video_${video_uuid}.mp4`;
            }

            const s3Path = `${s3FolderPath}/${originalFilename}`;
            console.log(`Uploading original video to ${s3Path}`);

            const fileExt = path.extname(originalFilename).toLowerCase();
            let contentType = 'video/mp4';

            // Map common video extensions to MIME types
            const videoMimeTypes = {
                '.mp4': 'video/mp4',
                '.mkv': 'video/x-matroska',
                '.avi': 'video/x-msvideo',
                '.mov': 'video/quicktime',
                '.wmv': 'video/x-ms-wmv',
                '.flv': 'video/x-flv',
                '.webm': 'video/webm',
                '.mpeg': 'video/mpeg',
                '.mpg': 'video/mpeg',
                '.m4v': 'video/x-m4v',
                '.3gp': 'video/3gpp'
            };

            if (videoMimeTypes[fileExt]) {
                contentType = videoMimeTypes[fileExt];
            }

            const fileStream = fs.createReadStream(tempFilePath);
            const params = {
                Bucket: process.env.S3_BUCKET,
                Key: s3Path,
                Body: fileStream,
                ContentType: contentType
            };
            await s3.upload(params).promise();

            const publicUrlBase = videoData?.public_url_base || process.env.S3_ENDPOINT;
            originalUrl = `${publicUrlBase}/${s3Path}`;
            console.log(`Successfully uploaded original video to ${s3Path}`);
            console.log('Original video URL:', originalUrl);
        } catch (uploadError) {
            console.error('Error uploading original video:', uploadError);
        }
        
        // Get video duration after download
        let videoDuration = 0;
        try {
            videoDuration = await getVideoDuration(tempFilePath);
            console.log(`Extracted video duration: ${videoDuration} seconds`);
            // Send the duration as early as possible
            await updateStatus('converting', null, null, videoDuration, originalUrl);
        } catch (durationError) {
            console.error('Error getting video duration:', durationError);
            // Continue with conversion even if duration extraction fails
            await updateStatus('converting');
        }

        await Promise.race([
            convertToHLS(tempFilePath, outputDir),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Conversion timeout after 240 minutes')), 14400000))
        ]);

        await updateStatus('uploading', null, null, videoDuration, originalUrl);
        const s3Url = await Promise.race([
            uploadToS3(outputDir, video_uuid, videoData),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Upload timeout after 60 minutes')), 3600000))
        ]);

        await updateStatus('completed', null, s3Url, videoDuration, originalUrl);
        await cleanup(tempFilePath, outputDir);
        return s3Url;
    } catch (error) {
        console.error(`Error processing video ${video_uuid}:`, error);
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
async function markVideoAsFailed(videoUuid) {
    try {
        const callbackUrl = processingManager.getCallbackUrl(videoUuid);
        if (!callbackUrl) {
            console.error(`No callback URL found for video ${videoUuid}`);
            return;
        }
        await sendCallback(callbackUrl, { video_uuid: videoUuid, status: 'failed', error: 'Server shutdown during processing' });
        console.log(`Successfully marked video ${videoUuid} as failed during shutdown`);
    } catch (error) {
        console.error(`Failed to mark video ${videoUuid} as failed:`, error);
    }
}

// Shutdown Server Gracefully
async function shutdownServer() {
    console.log('Shutting down server...');
    const currentVideoUuid = processingManager.getProcessingVideo();

    if (currentVideoUuid) {
        console.log(`Found video ${currentVideoUuid} still processing during shutdown`);
        try {
            await markVideoAsFailed(currentVideoUuid);
        } catch (error) {
            console.error('Error marking video as failed during shutdown:', error);
        }
    }

    // Mark all queued videos as failed
    const queueStatus = processingManager.getQueueStatus();
    for (const videoUuid of queueStatus.queueItems) {
        console.log(`Found video ${videoUuid} in queue during shutdown`);
        try {
            const nextVideo = processingManager.getNextFromQueue();
            if (nextVideo && nextVideo.video_uuid === videoUuid) {
                await markVideoAsFailed(videoUuid);
            }
        } catch (error) {
            console.error(`Error marking queued video ${videoUuid} as failed during shutdown:`, error);
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