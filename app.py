from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile
import logging
import urllib.request
import time
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

def format_ass_time(seconds):
    """Format seconds as H:MM:SS.cc for ASS"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    centis = int((seconds % 1) * 100)
    return f"{hours}:{minutes:02d}:{secs:02d}.{centis:02d}"

def format_srt_time(milliseconds):
    """Converti millisecondi in formato SRT (HH:MM:SS,mmm)"""
    seconds = milliseconds / 1000
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

def create_karaoke_ass(words, segment_start, output_path):
    """Create ASS subtitle file with karaoke effect (word highlighting)"""
    
    # ASS header with styling
    ass_content = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,48,&H00FFFFFF,&H00FFFF00,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,3,2,2,10,10,80,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    
    # Group words into chunks of 3-4 for readability
    chunk_size = 4
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk_words = words[i:i + chunk_size]
        chunks.append(chunk_words)
    
    # Create dialogue lines with karaoke effect
    for chunk in chunks:
        if not chunk:
            continue
            
        # Calculate timing for this chunk
        start_time = (chunk[0]['start'] / 1000.0) - segment_start
        end_time = (chunk[-1]['end'] / 1000.0) - segment_start
        
        # Format times for ASS (H:MM:SS.cc)
        start_ass = format_ass_time(max(0, start_time))
        end_ass = format_ass_time(max(0, end_time))
        
        # Build karaoke text with timing for each word
        karaoke_text = ""
        for word in chunk:
            word_start = (word['start'] / 1000.0) - segment_start
            word_duration = ((word['end'] - word['start']) / 1000.0)
            
            # Convert duration to centiseconds for ASS karaoke effect
            duration_cs = int(word_duration * 100)
            
            # Add karaoke effect: \k<duration> highlights the word
            karaoke_text += f"{{\\k{duration_cs}}}{word['text']} "
        
        # Add dialogue line
        ass_content += f"Dialogue: 0,{start_ass},{end_ass},Default,,0,0,0,,{karaoke_text.strip()}\n"
    
    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(ass_content)

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
        
        logger.info(f"RECEIVED DATA: {data}")
        segment_idx = data.get("segment_index", 0)
        
        # Converti segment_idx in intero se è stringa
        if isinstance(segment_idx, str):
            segment_idx = int(segment_idx)
        
        logger.info(f"EXTRACTED segment_index: {segment_idx} (type: {type(segment_idx)})")
        
        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        
        if not video_url:
            return jsonify({"error": "Missing video_url"}), 400

        if not s3:
            return jsonify({"error": "S3 client not configured"}), 500

        # Processa solo il segmento specifico richiesto
        if segment_idx >= len(segments):
            return jsonify({"error": f"segment_index {segment_idx} out of range"}), 400
        
        target_segment = segments[segment_idx]
        segments_to_process = [target_segment]

        logger.info(f"Processing video: {video_url}")
        logger.info(f"Processing segment index: {segment_idx}")

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, "input.mp4")
            
            logger.info("Downloading video from public URL")
            try:
                urllib.request.urlretrieve(video_url, video_path)
                logger.info(f"Download complete: {os.path.getsize(video_path)} bytes")
            except Exception as e:
                logger.error(f"Download failed: {str(e)}")
                return jsonify({"error": f"Download error: {str(e)}", "video_url": video_url}), 500

            all_words = data.get("words", [])

            for segment in segments_to_process:
                logger.info(f"Processing segment {segment_idx}")
                start = segment["start"]
                end = segment["end"]
                duration = end - start

                # Get words for this segment
                segment_words = [w for w in all_words if (w['start']/1000.0) >= start and (w['end']/1000.0) <= end]
                logger.info(f"Total words received: {len(all_words)}")
                logger.info(f"Segment start: {start}, end: {end}")
                logger.info(f"Filtered segment_words: {len(segment_words)}")
                if len(segment_words) > 0:
                    logger.info(f"First word: start={segment_words[0]['start']}, end={segment_words[0]['end']}")
                
                segment_path = os.path.join(tmpdir, f"segment_{segment_idx}.mp4")
                output_path = os.path.join(tmpdir, f"output_{segment_idx}.mp4")

                # Cut segment
                cut_cmd = [
                    "ffmpeg", "-y", "-i", video_path,
                    "-ss", str(start), "-t", str(duration),
                    "-c", "copy", segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)

                if segment_words:
                    # Create karaoke ASS file
                    ass_path = os.path.join(tmpdir, f"segment_{segment_idx}.ass")
                    create_karaoke_ass(segment_words, start, ass_path)

                    # Apply subtitles with ASS
                    subtitle_cmd = [
                        "ffmpeg", "-y", "-i", segment_path,
                        "-vf", f"ass={ass_path}",
                        "-c:a", "copy", output_path
                    ]
                    subprocess.run(subtitle_cmd, check=True, capture_output=True)
                else:
                    output_path = segment_path

                filename = video_url.split('/')[-1]
                output_key = f"segment_{segment_idx}_{filename}"
                
                logger.info(f"Uploading segment {segment_idx} to R2 as {output_key}")
                s3.upload_file(output_path, output_bucket, output_key)

                results.append({
                    "segment": segment_idx,
                    "url": f"https://cdn.vvcontent.com/{output_key}",
                    "duration": duration
                })
                
                logger.info(f"Segment {segment_idx} completed")

        logger.info("All segments processed successfully")
        return jsonify({"success": True, "results": results}), 200

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/generate_srt")
def generate_srt():
    try:
        raw_data = request.get_json()
        logger.info(f"Received SRT generation request")
        
        if isinstance(raw_data, dict):
            data = raw_data.get("body", raw_data)
        else:
            data = raw_data

        words = data.get("words", [])
        video_url = data.get("video_url", None)
        output_bucket = data.get("output_bucket", "shortconsottotitoli")
        
        if not words:
            return jsonify({"error": "Missing words"}), 400

        if not s3:
            return jsonify({"error": "S3 client not configured"}), 500

        # Genera il file SRT
        srt_content = ""
        chunk_size = 4  # Parole per sottotitolo
        subtitle_index = 1
        
        for i in range(0, len(words), chunk_size):
            chunk = words[i:i + chunk_size]
            if not chunk:
                continue
            
            start_ms = chunk[0]['start']
            end_ms = chunk[-1]['end']
            
            # Converti millisecondi in formato SRT (HH:MM:SS,mmm)
            start_time = format_srt_time(start_ms)
            end_time = format_srt_time(end_ms)
            
            # Testo del sottotitolo
            text = " ".join([w['text'] for w in chunk])
            
            srt_content += f"{subtitle_index}\n"
            srt_content += f"{start_time} --> {end_time}\n"
            srt_content += f"{text}\n\n"
            
            subtitle_index += 1
        
        # Genera nome file da video_url se presente, altrimenti usa timestamp
        if video_url:
            filename = video_url.split('/')[-1].replace('.mp4', '.srt')
        else:
            filename = f"subtitles_{int(time.time())}.srt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.srt', delete=False, encoding='utf-8') as tmp:
            tmp.write(srt_content)
            tmp_path = tmp.name
        
        try:
            s3.upload_file(tmp_path, output_bucket, filename)
            logger.info(f"SRT uploaded to R2 as {filename}")
        finally:
            os.unlink(tmp_path)
        
        return jsonify({
            "success": True,
            "srt_url": f"https://cdn.vvcontent.com/{filename}",
            "filename": filename
        }), 200

    except Exception as e:
        logger.error(f"Error generating SRT: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
