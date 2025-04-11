class ProcessingManager {
    constructor() {
        this.currentlyProcessing = null;
        this.processingStartTime = null;
        this.callbackUrl = null;
        // Timeout after 2 hours to prevent stuck states
        this.maxProcessingTime = 2 * 60 * 60 * 1000;
        // Add queue for pending videos
        this.videoQueue = [];
    }

    isProcessing() {
        if (!this.currentlyProcessing) {
            return false;
        }
        // Check if processing has timed out
        if (this.processingStartTime &&
            Date.now() - this.processingStartTime > this.maxProcessingTime) {
            console.log('Processing timeout detected, resetting state');
            this.resetProcessing();
            return false;
        }
        return true;
    }

    // Add video to queue
    addToQueue(videoId, videoUrl, callbackUrl, videoData = {}) {
        // Check if this video is already in the queue
        const existingIndex = this.videoQueue.findIndex(item => item.videoId === videoId);

        if (existingIndex >= 0) {
            // Update existing entry instead of adding a duplicate
            this.videoQueue[existingIndex] = { videoId, videoUrl, callbackUrl, videoData };
            console.log(`Updated video ${videoId} in queue`);
            return { success: true, position: existingIndex + 1, message: 'Video updated in queue' };
        } else {
            // Add to queue
            this.videoQueue.push({ videoId, videoUrl, callbackUrl, videoData });
            console.log(`Added video ${videoId} to queue at position ${this.videoQueue.length}`);
            return { success: true, position: this.videoQueue.length, message: 'Video added to queue' };
        }
    }

    // Get the queue status
    getQueueStatus() {
        return {
            currentlyProcessing: this.currentlyProcessing,
            queueLength: this.videoQueue.length,
            queueItems: this.videoQueue.map(item => item.videoId)
        };
    }

    // Get the next video from the queue
    getNextFromQueue() {
        if (this.videoQueue.length === 0) {
            return null;
        }
        return this.videoQueue[0];
    }

    // Process the next video in the queue
    processNextInQueue() {
        if (this.isProcessing() || this.videoQueue.length === 0) {
            return null;
        }

        const nextVideo = this.videoQueue.shift();
        this.startProcessing(nextVideo.videoId, nextVideo.callbackUrl);

        return {
            videoId: nextVideo.videoId,
            videoUrl: nextVideo.videoUrl,
            callbackUrl: nextVideo.callbackUrl,
            videoData: nextVideo.videoData
        };
    }

    startProcessing(videoId, callbackUrl) {
        if (this.isProcessing()) {
            return false;
        }
        this.currentlyProcessing = videoId;
        this.callbackUrl = callbackUrl;
        this.processingStartTime = Date.now();
        console.log(`Started processing video ${videoId} at ${new Date().toISOString()}`);
        return true;
    }

    getProcessingVideo() {
        return this.currentlyProcessing;
    }

    getCallbackUrl(videoId) {
        return this.currentlyProcessing === videoId ? this.callbackUrl : null;
    }

    resetProcessing() {
        this.currentlyProcessing = null;
        this.processingStartTime = null;
        this.callbackUrl = null;
    }

    completeProcessing(videoId) {
        if (this.currentlyProcessing === videoId) {
            this.resetProcessing();
            console.log(`Completed processing video ${videoId} at ${new Date().toISOString()}`);
            return true;
        }
        return false;
    }

    // Remove a specific video from the queue
    removeFromQueue(videoId) {
        const initialLength = this.videoQueue.length;
        this.videoQueue = this.videoQueue.filter(item => item.videoId !== videoId);

        const removed = initialLength > this.videoQueue.length;
        if (removed) {
            console.log(`Removed video ${videoId} from queue`);
        }

        return removed;
    }

} module.exports = new ProcessingManager();

