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
    addToQueue(video_uuid, videoUrl, callbackUrl, videoData = {}) {
        // Check if this video is already in the queue
        const existingIndex = this.videoQueue.findIndex(item => item.video_uuid === video_uuid);

        if (existingIndex >= 0) {
            // Update existing entry instead of adding a duplicate
            this.videoQueue[existingIndex] = { video_uuid, videoUrl, callbackUrl, videoData };
            console.log(`Updated video ${video_uuid} in queue`);
            return { success: true, position: existingIndex + 1, message: 'Video updated in queue' };
        } else {
            // Add to queue
            this.videoQueue.push({ video_uuid, videoUrl, callbackUrl, videoData });
            console.log(`Added video ${video_uuid} to queue at position ${this.videoQueue.length}`);
            return { success: true, position: this.videoQueue.length, message: 'Video added to queue' };
        }
    }

    // Get the queue status
    getQueueStatus() {
        return {
            currentlyProcessing: this.currentlyProcessing,
            queueLength: this.videoQueue.length,
            queueItems: this.videoQueue.map(item => item.video_uuid)
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
        this.startProcessing(nextVideo.video_uuid, nextVideo.callbackUrl);

        return {
            video_uuid: nextVideo.video_uuid,
            videoUrl: nextVideo.videoUrl,
            callbackUrl: nextVideo.callbackUrl,
            videoData: nextVideo.videoData
        };
    }

    startProcessing(video_uuid, callbackUrl) {
        if (this.isProcessing()) {
            return false;
        }
        this.currentlyProcessing = video_uuid;
        this.callbackUrl = callbackUrl;
        this.processingStartTime = Date.now();
        console.log(`Started processing video ${video_uuid} at ${new Date().toISOString()}`);
        return true;
    }

    getProcessingVideo() {
        return this.currentlyProcessing;
    }

    getCallbackUrl(video_uuid) {
        return this.currentlyProcessing === video_uuid ? this.callbackUrl : null;
    }

    resetProcessing() {
        this.currentlyProcessing = null;
        this.processingStartTime = null;
        this.callbackUrl = null;
    }

    completeProcessing(video_uuid) {
        if (this.currentlyProcessing === video_uuid) {
            this.resetProcessing();
            console.log(`Completed processing video ${video_uuid} at ${new Date().toISOString()}`);
            return true;
        }
        return false;
    }

    // Remove a specific video from the queue
    removeFromQueue(video_uuid) {
        const initialLength = this.videoQueue.length;
        this.videoQueue = this.videoQueue.filter(item => item.video_uuid !== video_uuid);

        const removed = initialLength > this.videoQueue.length;
        if (removed) {
            console.log(`Removed video ${video_uuid} from queue`);
        }

        return removed;
    }

} module.exports = new ProcessingManager();

