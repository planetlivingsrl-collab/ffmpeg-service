from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile
import logging
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY")

def normalize_region(region):
    if not region or region == "auto":
        return "us-east-1"
    return region

def make_r2_client(endpoint, access_key, secret_key, region="us-east-1"):
    region = normalize_region(region)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/process", methods=["POST"])
def process_video():
    try:
        raw = request.get_json()
        logger.info("Request received")
        
        data = raw.get("body", raw) if isinstance(raw, dict) else raw
        s3_config = data.get("s3_config")
        segments = data.get("segments")
        
        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        if not s3_config:
            return jsonify({"error": "Missing s3_config"}), 400
        
        logger.info(f"Endpoint: {s3_config.get('endpoint')}")
        logger.info(f"Bucket: {s3_config.get('bucket')}")
        logger.info(f"Key: {s3_config.get('key')}")
        
        region = normalize_region(s3_config.get("region"))
        s3_client = make_r2_client(
            s3_config["endpoint"],
            s3_config["accessKeyId"],
            s3_config["secretAccessKey"],
            region
        )
        
        input_bucket = s3_config["bucket"]
        output_bucket = s3_config.get("output_bucket", "shortconsottotitoli")
        video_key = s3_config["key"]
        
        logger.info("Checking file exists...")
        try:
            head = s3_client.head_object(Bucket=input_bucket, Key=video_key)
            logger.info(f"File found: {head.get('ContentLength')} bytes")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            msg = e.response.get("Error", {}).get("Message", str(e))
            logger.error(f"HEAD failed: {code} - {msg}")
            return jsonify({"error": f"File not found: {code} - {msg}"}), 400
        
        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "input.mp4")
            
            logger.info("Downloading video...")
            s3_client.download_file(input_bucket, video_key, video_path)
            logger.info(f"Downloaded: {os.path.getsize(video_path)} bytes")
            
            for idx, segment in enumerate(segments):
                logger.info(f"Processing segment {idx}")
                start = segment["start"]
                end = segment["end"]
                duration = end - start
                
                segment_path = os.path.join(tmpdir, f"segment_{idx}.mp4")
                
                cmd = [
                    "ffmpeg", "-y", "-i", video_path,
                    "-ss", str(start), "-t", str(duration),
                    "-c", "copy", segment_path
                ]
                subprocess.run(cmd, check=True, capture_output=True)
                
                output_key = f"segment_{idx}_{os.path.basename(video_key)}"
                s3_client.upload_file(segment_path, output_bucket, output_key)
                
                results.append({
                    "segment": idx,
                    "url": f"{s3_config['endpoint']}/{output_bucket}/{output_key}",
                    "duration": duration
                })
        
        logger.info(f"Success: {len(results)} segments")
        return jsonify({"success": True, "results": results}), 200
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting on port {port}")
    app.run(host="0.0.0.0", port=port)
```

---

## VERIFICA requirements.txt

Deve contenere:
```
Flask==3.0.0
boto3==1.34.0
gunicorn==21.2.0
