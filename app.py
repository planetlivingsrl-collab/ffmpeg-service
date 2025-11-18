from flask import Flask, request, jsonify
import os
import io
import boto3
import subprocess
import tempfile
import logging
from urllib.parse import urlparse, unquote
from botocore.config import Config
from botocore.exceptions import ClientError

# Setup logging PRIMA di tutto
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Config
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")
R2_REGION = os.environ.get("R2_REGION", "us-east-1")

def normalize_region(region):
    if not region or region == "auto":
        return "us-east-1"
    return region

def make_r2_s3_client(endpoint, access_key, secret_key, region="us-east-1"):
    region = normalize_region(region)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}
        ),
    )

# Client globale
try:
    s3 = make_r2_s3_client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_REGION) if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY else None
    logger.info("S3 client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {e}")
    s3 = None

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/process", methods=["POST"])
def process_video():
    try:
        raw_data = request.get_json()
        logger.info("=== INCOMING REQUEST ===")
        logger.info(f"Raw data type: {type(raw_data)}")
        
        # Parse data
        if isinstance(raw_data, dict) and "body" in raw_data:
            data = raw_data["body"]
            logger.info("Extracted from body wrapper")
        else:
            data = raw_data

        s3_config = data.get("s3_config")
        segments = data.get("segments")
        
        logger.info(f"S3 Config present: {s3_config is not None}")
        logger.info(f"Segments count: {len(segments) if segments else 0}")

        if not segments:
            return jsonify({"error": "Missing segments"}), 400

        if not s3_config:
            return jsonify({"error": "Missing s3_config"}), 400

        # Build S3 client
        region = normalize_region(s3_config.get("region"))
        logger.info(f"Endpoint: {s3_config.get('endpoint')}")
        logger.info(f"Bucket: {s3_config.get('bucket')}")
        logger.info(f"Key: {s3_config.get('key')}")
        logger.info(f"Region: {region}")
        
        s3_client = make_r2_s3_client(
            endpoint=s3_config["endpoint"],
            access_key=s3_config["accessKeyId"],
            secret_key=s3_config["secretAccessKey"],
            region=region,
        )
        
        input_bucket = s3_config["bucket"]
        output_bucket = s3_config.get("output_bucket", "shortconsottotitoli")
        video_key = s3_config["key"]

        # HEAD check
        logger.info("Attempting HEAD object...")
        try:
            head_resp = s3_client.head_object(Bucket=input_bucket, Key=video_key)
            logger.info(f"HEAD success: {head_resp.get('ContentLength')} bytes")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            logger.error(f"HEAD failed: {error_code} - {error_msg}")
            return jsonify({
                "error": f"File not found: {error_code} - {error_msg}",
                "bucket": input_bucket,
                "key": video_key
            }), 400

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "input.mp4")
            
            # Download
            logger.info("Downloading video...")
            try:
                s3_client.download_file(input_bucket, video_key, video_path)
                file_size = os.path.getsize(video_path)
                logger.info(f"Download success: {file_size} bytes")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_msg = e.response.get("Error", {}).get("Message", str(e))
                logger.error(f"Download failed: {error_code} - {error_msg}")
                return jsonify({
                    "error": f"Download error: {error_code} - {error_msg}"
                }), 500

            # Process segments
            for idx, segment in enumerate(segments):
                logger.info(f"Processing segment {idx}...")
                start = segment["start"]
                end = segment["end"]
                duration = end - start

                segment_path = os.path.join(tmpdir, f"segment_{idx}.mp4")
                
                # Cut
                cut_cmd = [
                    "ffmpeg", "-y", "-i", video_path,
                    "-ss", str(start), "-t", str(duration),
                    "-c", "copy", segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)

                # Upload
                output_key = f"segment_{idx}_{os.path.basename(video_key)}"
                s3_client.upload_file(segment_path, output_bucket, output_key)
                
                logger.info(f"Segment {idx} uploaded: {output_key}")

                results.append({
                    "segment": idx,
                    "url": f"{s3_config['endpoint']}/{output_bucket}/{output_key}",
                    "duration": duration
                })

        logger.info(f"Success! Processed {len(results)} segments")
        return jsonify({"success": True, "results": results}), 200

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
```

---

## VERIFICA REQUIREMENTS.TXT

Assicurati che `requirements.txt` contenga:
```
Flask==3.0.0
boto3==1.34.0
gunicorn==21.2.0
