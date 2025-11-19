from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile
import logging
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

s3 = (
    make_r2_s3_client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_REGION)
    if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY
    else None
)

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/process")
def process_video():
    try:
        raw_data = request.get_json()
        logger.info(f"Received request")
        
        if isinstance(raw_data, dict):
            data = raw_data.get("body", raw_data)
        else:
            data = raw_data

        video_url = data.get("video_url")
        segments = data.get("segments")
        subtitles_data = data.get("subtitles")
        output_bucket = data.get("output_bucket", "shortconsottotitoli")

        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        
        if not video_url:
            return jsonify({"error": "Missing video_url"}), 400

        if not s3:
            return jsonify({"error": "S3 client not configured"}), 500

        logger.info(f"Processing video: {video_url}")

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "input.mp4")
            
            logger.info("Downloading video from public URL")
            try:
                response = requests.get(video_url, stream=True, timeout=300)
                response.raise_for_status()
                
                with open(video_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Download complete: {os.path.getsize(video_path)} bytes")
            except Exception as e:
                logger.error(f"Download failed: {str(e)}")
                return jsonify({"error": f"Download error: {str(e)}", "video_url": video_url}), 500

            for idx, segment in enumerate(segments):
                start = segment["start"]
                end = segment["end"]
                duration = end - start

                segment_subtitles = None
                if subtitles_data:
                    for sub_entry in subtitles_data:
                        if sub_entry.get("segment_index") == idx:
                            segment_subtitles = sub_entry.get("subtitle_srt")
                            break

                segment_path = os.path.join(tmpdir, f"segment_{idx}.mp4")
                output_path = os.path.join(tmpdir, f"output_{idx}.mp4")

                cut_cmd = [
                    "ffmpeg", "-y", "-i", video_path,
                    "-ss", str(start), "-t", str(duration),
                    "-c", "copy", segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)

                if segment_subtitles:
                    srt_path = os.path.join(tmpdir, f"segment_{idx}.srt")
                    with open(srt_path, "w", encoding="utf-8") as f:
                        f.write(segment_subtitles)

                    filter_str = (
                        f"subtitles={srt_path}:"
                        "force_style='FontSize=24,PrimaryColour=&HFFFFFF,"
                        "OutlineColour=&H000000,BorderStyle=3,Outline=2,Shadow=1,MarginV=20'"
                    )
                    subtitle_cmd = [
                        "ffmpeg", "-y", "-i", segment_path,
                        "-vf", filter_str, "-c:a", "copy", output_path
                    ]
                    subprocess.run(subtitle_cmd, check=True, capture_output=True)
                else:
                    output_path = segment_path

                filename = video_url.split('/')[-1]
                output_key = f"segment_{idx}_{filename}"
                s3.upload_file(output_path, output_bucket, output_key)

                results.append({
                    "segment": idx,
                    "url": f"https://cdn.vvcontent.com/{output_key}",
                    "duration": duration
                })

        return jsonify({"success": True, "results": results}), 200

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
