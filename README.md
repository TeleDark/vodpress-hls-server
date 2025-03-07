# VODPress Conversion Server

This is a Node.js-based server designed to work with the [VODPress WordPress Plugin](#). It handles video downloading, conversion to HLS (HTTP Live Streaming) format with multiple quality levels (1080p, 720p, 480p), and uploads the results to an S3-compatible storage service.

## Features
- Downloads videos from provided URLs.
- Converts videos to HLS with adaptive bitrate streaming (1080p, 720p, 480p).
- Uploads HLS segments and playlists to S3-compatible storage.
- Sends status updates (downloading, converting, uploading, completed, failed) via callbacks.
- Secure API endpoint with API key hash authentication.
- Automatic cleanup of temporary files.

## Prerequisites
- Node.js 14.x or higher.
- FFMPEG installed on the server (see [FFMPEG Installation](#ffmpeg-installation)).
- An S3-compatible storage service (e.g., AWS S3, MinIO, Cloudflare R2).
- Environment variables configured via a `.env` file.

## Installation

### Clone the Repository
```bash
git clone https://github.com/yourusername/vodpress-conversion-server.git
cd vodpress-conversion-server
```

### Install Dependencies
```bash
npm install
```

### Configure Environment Variables
Create a `.env` file in the root directory with the following:
```ini
PORT=3000
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_ENDPOINT=https://your-s3-endpoint
S3_BUCKET=your-bucket-name
API_KEYS=your-api-key-here
FFMPEG_PATH=/path/to/ffmpeg (optional)
```
Replace values with your own credentials and paths.

### FFMPEG Installation
Install FFMPEG on your server:

- **Ubuntu/Debian:** `sudo apt-get install ffmpeg`
- **CentOS/RHEL:** `sudo yum install ffmpeg`
- **MacOS:** `brew install ffmpeg`

Verify installation:
```bash
ffmpeg -version
```

### Start the Server
```bash
npm start
```
The server will run on the specified `PORT` (default: 3000).

## Usage
The server exposes a single endpoint: `POST /api/convert`.

### Request Body:
```json
{
  "video_url": "https://example.com/video.mp4",
  "video_id": 123,
  "callback_url": "https://your-wordpress-site/wp-json/vodpress/v1/callback",
  "site_url": "https://your-wordpress-site"
}
```

### Headers:
```http
X-API-Key-Hash: SHA256 hash of your API key.
```
The server processes one video at a time and rejects additional requests with a `429` status if busy.

## Processing Workflow
1. Downloads the video to a temporary directory (`temp/`).
2. Converts it to HLS with multiple quality streams using FFMPEG.
3. Uploads the HLS files to S3.
4. Sends status updates to the provided `callback_url`.
5. Cleans up temporary files after completion or failure.

## Configuration
- **S3 Settings:** Configured via `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT`, and `S3_BUCKET`.
- **API Keys:** Stored in `API_KEYS` environment variable (comma-separated for multiple keys).
- **FFMPEG:** Automatically detects FFMPEG path or uses `FFMPEG_PATH`.

## Development
- **Dependencies:** See `package.json` for full list (e.g., `fluent-ffmpeg`, `aws-sdk`, `axios`).
- **Modules:**
  - `api-key-manager.js`: Manages API key generation and validation.
  - `processing-manager.js`: Ensures single-video processing with timeout protection.

## Contributing
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

## License
This project is licensed under the MIT License - see the `LICENSE` file for details.

## Author
**Morteza Mohammadnezhad**

## Support
For issues or feature requests, please open an issue on this repository.