class ProcessingManager {
    constructor() {
        this.currentlyProcessing = null;
        this.processingStartTime = null;
        this.callbackUrl = null;
        // Timeout after 2 hours to prevent stuck states
        this.maxProcessingTime = 2 * 60 * 60 * 1000;
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
        }
    }

} module.exports = new ProcessingManager();

