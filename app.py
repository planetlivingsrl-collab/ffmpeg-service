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
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}
        ),
    )

s3 = (
    make_r2_s3_client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_REGION)
    if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY
    else None
)

@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.post("/generate-presigned-url")
def generate_presigned_url():
    try:
        data = request.get_json()
        bucket_name = data.get('bucket')
        file_key = data.get('key')
        
        if not bucket_name or not file_key:
            return jsonify({"error": "Missing bucket or key"}), 400
        
        logger.info(f"Generating presigned URL for {bucket_name}/{file_key}")
        
        s3_client = make_r2_s3_client(
            R2_ENDPOINT,
            R2_ACCESS_KEY,
            R2_SECRET_KEY,
            R2_REGION
        )
        
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': file_key},
            ExpiresIn=7200
        )
        
        logger.info("Presigned URL generated successfully")
        return jsonify({'presigned_url': presigned_url}), 200
        
    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.post("/debug/head")
def debug_head():
    try:
        payload = request.get_json() or {}
        s3c = payload.get("s3_config", payload)

        required = ["endpoint", "bucket", "key", "accessKeyId", "secretAccessKey"]
        missing = [k for k in required if not s3c.get(k)]
        if missing:
            return jsonify({"ok": False, "error": f"Missing fields: {missing}"}), 400

        region = normalize_region(s3c.get("region"))
        s3_cli = make_r2_s3_client(
            endpoint=s3c["endpoint"],
            access_key=s3c["accessKeyId"],
            secret_key=s3c["secretAccessKey"],
            region=region,
        )
        r = s3_cli.head_object(Bucket=s3c["bucket"], Key=s3c["key"])
        return jsonify(
            {"ok": True, "content_length": r.get("ContentLength"), "etag": r.get("ETag")}
        ), 200

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        return jsonify({
            "ok": False,
            "error": f"S3 error ({error_code}): {error_message}",
            "bucket": s3c.get("bucket"),
            "key": s3c.get("key")
        }), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/process")
def process_video():
    try:
        raw_data = request.get_json()
        logger.info(f"=== RAW PAYLOAD ===")
        logger.info(f"Type: {type(raw_data)}")
        logger.info(f"Content: {raw_data}")
        
        if isinstance(raw_data, dict):
            if "body" in raw_data:
                logger.info("Found 'body' wrapper, extracting...")
                data = raw_data["body"]
            else:
                data = raw_data
        else:
            data = raw_data

        logger.info(f"=== PARSED DATA ===")
        logger.info(f"Type: {type(data)}")
        logger.info(f"Content: {data}")

        s3_config = data.get("s3_config")
        video_url = data.get("video_url")
        segments = data.get("segments")
        subtitles_data = data.get("subtitles")

        if not segments:
            return jsonify({"error": "Missing segments"}), 400

        logger.info(f"=== S3 CONFIG ===")
        logger.info(f"Endpoint: {s3_config.get('endpoint') if s3_config else 'N/A'}")
        logger.info(f"Bucket: {s3_config.get('bucket') if s3_config else 'N/A'}")
        logger.info(f"Key: {s3_config.get('key') if s3_config else 'N/A'}")
        logger.info(f"Region: {s3_config.get('region') if s3_config else 'N/A'}")

        if s3_config:
            region = normalize_region(s3_config.get("region"))
            s3_client = make_r2_s3_client(
                endpoint=s3_config["endpoint"],
                access_key=s3_config["accessKeyId"],
                secret_key=s3_config["secretAccessKey"],
                region=region,
            )
            input_bucket = s3_config["bucket"]
            output_bucket = s3_config.get("output_bucket", "shortconsottotitoli")
            video_key = s3_config["key"]
        elif video_url:
            if not s3:
                return jsonify({"error": "S3 client not configured"}), 500
            s3_client = s3
            input_bucket = os.environ.get("R2_INPUT_BUCKET", "videoliving")
            output_bucket = os.environ.get("R2_OUTPUT_BUCKET", "shortconsottotitoli")
            path = urlparse(video_url).path
            video_key = unquote(path.lstrip("/").split("/")[-1])
        else:
            return jsonify({"error": "Missing video_url or s3_config"}), 400

        logger.info(f"=== ATTEMPTING HEAD OBJECT ===")
        logger.info(f"Bucket: {input_bucket}")
        logger.info(f"Key: {video_key}")
        
        try:
            head_response = s3_client.head_object(Bucket=input_bucket, Key=video_key)
            logger.info(f"HEAD SUCCESS: {head_response.get('ContentLength')} bytes")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))
            logger.error(f"HEAD FAILED: {error_code} - {error_message}")
            logger.error(f"Full error response: {e.response}")
            return jsonify({
                "error": f"File non trovato o accesso negato: {error_code} - {error_message}",
                "bucket": input_bucket,
                "key": video_key,
                "endpoint": s3_config.get("endpoint") if s3_config else "N/A"
            }), 400

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "input.mp4")
            
            logger.info(f"=== DOWNLOADING VIDEO ===")
            try:
                s3_client.download_file(input_bucket, video_key, video_path)
                logger.info(f"Download SUCCESS: {os.path.getsize(video_path)} bytes")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", str(e))
                logger.error(f"Download FAILED: {error_code} - {error_message}")
                return jsonify({
                    "error": f"Errore download video: {error_code} - {error_message}",
                    "bucket": input_bucket,
                    "key": video_key
                }), 500

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

                output_key = f"segment_{idx}_{os.path.basename(video_key)}"
                s3_client.upload_file(output_path, output_bucket, output_key)

                base = s3_config["endpoint"] if s3_config else R2_ENDPOINT
                results.append({
                    "segment": idx,
                    "url": f"{base}/{output_bucket}/{output_key}",
                    "duration": duration
                })

        return jsonify({"success": True, "results": results}), 200

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))
        logger.error(f"S3 ERROR: {error_code} - {error_message}")
        return jsonify({
            "error": f"S3 error ({error_code}): {error_message}"
        }), 500
    except subprocess.CalledProcessError as e:
        logger.error(f"FFMPEG ERROR: {e.stderr.decode() if e.stderr else str(e)}")
        return jsonify({
            "error": f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}"
        }), 500
    except Exception as e:
        logger.error(f"GENERAL ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
