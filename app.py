from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile
import logging
import urllib.request
import time
import re
import json
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

# Memory optimization settings
FFMPEG_THREADS = "2"  # Limit threads to reduce memory
FFMPEG_PRESET = "ultrafast"  # Uses less memory than fast/medium
FFMPEG_CRF = "23"  # Slightly lower quality but much less memory

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
    return jsonify({"status": "ok", "version": "3.7-memory-optimized"}), 200

def format_ass_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    centis = round((seconds % 1) * 100)
    if centis >= 100:
        centis = 99
    return f"{hours}:{minutes:02d}:{secs:02d}.{centis:02d}"

def format_srt_time(milliseconds):
    seconds = milliseconds / 1000
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

def get_stream_start_time(video_path, stream_type="a"):
    """Ottieni lo start_time dello stream audio o video"""
    try:
        stream_selector = "a:0" if stream_type == "a" else "v:0"
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-select_streams", stream_selector,
            "-show_entries", "stream=start_time",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ], capture_output=True, text=True)
        output = result.stdout.strip()
        if output and output != "N/A":
            return float(output)
        return None
    except Exception as e:
        logger.error(f"Error getting stream start_time: {e}")
        return None

def get_first_packet_pts(video_path, stream_type="a"):
    """Ottieni il PTS del primo pacchetto leggendo solo i primi pacchetti"""
    try:
        stream_selector = "a:0" if stream_type == "a" else "v:0"
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-select_streams", stream_selector,
            "-show_entries", "packet=pts_time",
            "-of", "csv=p=0",
            "-read_intervals", "%+#10",
            video_path
        ], capture_output=True, text=True)
        lines = result.stdout.strip().split('\n')
        for line in lines:
            line = line.strip()
            if line and line != "N/A":
                try:
                    return float(line)
                except ValueError:
                    continue
        return None
    except Exception as e:
        logger.error(f"Error getting first packet PTS: {e}")
        return None

def get_actual_start_time(video_path):
    """Ottieni il tempo di inizio effettivo del video usando metodi multipli"""
    
    audio_stream_start = get_stream_start_time(video_path, "a")
    logger.info(f"Audio stream start_time: {audio_stream_start}")
    
    if audio_stream_start is not None and audio_stream_start > 0:
        return audio_stream_start
    
    audio_packet_pts = get_first_packet_pts(video_path, "a")
    logger.info(f"Audio first packet PTS: {audio_packet_pts}")
    
    if audio_packet_pts is not None and audio_packet_pts > 0:
        return audio_packet_pts
    
    video_stream_start = get_stream_start_time(video_path, "v")
    logger.info(f"Video stream start_time: {video_stream_start}")
    
    if video_stream_start is not None and video_stream_start > 0:
        return video_stream_start
    
    return 0.0

def create_copernicus_ass(words, segment_start, output_path, keywords=None):
    if keywords is None:
        keywords = []
    
    keywords_clean = []
    for kw in keywords:
        if isinstance(kw, str):
            keywords_clean.append(kw.lower().strip())
    
    logger.info(f"Keywords for styling: {keywords_clean}")
    logger.info(f"Creating ASS with segment_start: {segment_start}")
    
    ass_content = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial Black,60,&H00FFAA00,&H00FFFFFF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,4,0,2,60,60,200,1
Style: Keyword,Arial Black,60,&H000000FF,&H00FFFFFF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,4,0,2,60,60,200,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    
    chunk_size = 4
    max_chars_per_line = 24
    
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk_words = words[i:i + chunk_size]
        chunks.append(chunk_words)
    
    keyword_count = 0
    
    for chunk in chunks:
        if not chunk:
            continue
        
        start_time = chunk[0]['start'] - segment_start
        end_time = chunk[-1]['end'] - segment_start
        
        start_ass = format_ass_time(max(0, start_time))
        end_ass = format_ass_time(max(0, end_time))
        
        words_in_chunk = []
        for word in chunk:
            word_text = word['text'].strip().upper()
            word_lower = word['text'].strip().lower()
            word_clean = ''.join(c for c in word_lower if c.isalnum())
            
            word_duration_secs = word['end'] - word['start']
            word_duration_centis = max(1, round(word_duration_secs * 100))
            
            is_keyword = False
            for kw in keywords_clean:
                kw_clean = ''.join(c for c in kw if c.isalnum())
                if word_clean == kw_clean:
                    is_keyword = True
                    break
            
            if is_keyword:
                words_in_chunk.append({
                    'text': word_text,
                    'styled': f"{{\\rKeyword\\kf{word_duration_centis}}}{word_text}{{\\rDefault}}",
                    'is_keyword': True
                })
                keyword_count += 1
            else:
                words_in_chunk.append({
                    'text': word_text,
                    'styled': f"{{\\kf{word_duration_centis}}}{word_text}",
                    'is_keyword': False
                })
        
        total_text = ' '.join([w['text'] for w in words_in_chunk])
        
        if len(total_text) > max_chars_per_line and len(words_in_chunk) > 1:
            mid = len(words_in_chunk) // 2
            line1 = ' '.join([w['styled'] for w in words_in_chunk[:mid]])
            line2 = ' '.join([w['styled'] for w in words_in_chunk[mid:]])
            styled_text = f"{line1}\\N{line2}"
        else:
            styled_text = ' '.join([w['styled'] for w in words_in_chunk])
        
        dialogue_line = f"Dialogue: 0,{start_ass},{end_ass},Default,,0,0,0,,{styled_text}\n"
        ass_content += dialogue_line
    
    logger.info(f"Total keywords highlighted: {keyword_count}")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(ass_content)

@app.post("/identify_keywords")
def identify_keywords():
    try:
        raw_data = request.get_json()
        logger.info("Received keyword identification request")
        
        if isinstance(raw_data, dict):
            data = raw_data.get("body", raw_data)
        else:
            data = raw_data
        
        full_text = data.get("full_text", "")
        
        if not full_text:
            return jsonify({"error": "Missing full_text"}), 400
        
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 500
        
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        
        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 500,
            "messages": [{
                "role": "user",
                "content": f"Analizza questo testo in italiano e identifica le PAROLE CHIAVE più importanti e attraenti per chi guarda (ad esempio nomi propri, numeri come prezzi, luoghi, termini tecnici). NON includere verbi comuni, articoli, congiunzioni. Rispondi SOLO con un array JSON, tutto minuscolo.\n\nTesto: {full_text}\n\nFormato: [\"parola1\", \"parola2\"]"
            }]
        }
        
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Anthropic API error: {response.status_code}")
            return jsonify({"error": f"API error: {response.status_code}"}), 500
        
        response_data = response.json()
        response_text = response_data["content"][0]["text"]
        
        keywords_match = re.search(r'\[.*?\]', response_text, re.DOTALL)
        keywords = []
        
        if keywords_match:
            keywords = json.loads(keywords_match.group(0))
        
        logger.info(f"Identified {len(keywords)} keywords: {keywords}")
        
        return jsonify({
            "success": True,
            "keywords": keywords
        }), 200
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/process")
def process_video():
    try:
        raw_data = request.get_json()
        logger.info("Received process request")
        
        if isinstance(raw_data, dict):
            data = raw_data.get("body", raw_data)
        else:
            data = raw_data

        video_url = data.get("video_url")
        segments = data.get("segments")
        output_bucket = data.get("output_bucket", "shortconsottotitoli")
        keywords = data.get("keywords", [])
        
        logger.info(f"Keywords received: {keywords}")
        
        segment_idx = data.get("segment_index", 0)
        output_idx = data.get("output_index", segment_idx)
        
        if isinstance(segment_idx, str):
            segment_idx = int(segment_idx)
        if isinstance(output_idx, str):
            output_idx = int(output_idx)
        
        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        
        if not video_url:
            return jsonify({"error": "Missing video_url"}), 400

        if not s3:
            return jsonify({"error": "S3 client not configured"}), 500

        if segment_idx >= len(segments):
            return jsonify({"error": f"segment_index {segment_idx} out of range"}), 400
        
        target_segment = segments[segment_idx]

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            # OPTIMIZATION: Download only the segment we need using FFmpeg streaming
            # Instead of downloading the full video, we let FFmpeg handle the range
            
            all_words = data.get("words", [])
            
            start = target_segment["start"]
            end = target_segment["end"]
            duration = end - start

            segment_words = [w for w in all_words if w['start'] >= start and w['start'] < end]
            logger.info(f"Segment {output_idx}: {len(segment_words)} words, requested start={start}, end={end}")
            
            if segment_words:
                first_word_time = segment_words[0]['start']
                logger.info(f"First word timestamp: {first_word_time}")
            
            temp_segment_path = os.path.join(tmpdir, f"temp_segment_{output_idx}.mp4")
            output_path = os.path.join(tmpdir, f"output_{output_idx}.mp4")

            # OPTIMIZED: Single pass cut with streaming from URL
            # FFmpeg reads directly from URL, seeking to start position
            # Uses limited threads and memory-efficient settings
            cut_cmd = [
                "ffmpeg", "-y",
                "-threads", FFMPEG_THREADS,
                "-ss", str(start),  # Seek BEFORE input for faster seeking
                "-i", video_url,    # Read directly from URL
                "-t", str(duration),
                "-c:v", "libx264",
                "-crf", FFMPEG_CRF,
                "-preset", FFMPEG_PRESET,
                "-c:a", "aac",
                "-b:a", "128k",  # Reduced audio bitrate
                "-threads", FFMPEG_THREADS,
                "-max_muxing_queue_size", "1024",  # Limit memory for muxing
                temp_segment_path
            ]
            
            logger.info(f"Cutting segment directly from URL: start={start}, duration={duration}")
            result = subprocess.run(cut_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"FFmpeg cut error: {result.stderr}")
                # Fallback: try downloading first if streaming fails
                logger.info("Streaming failed, falling back to download method")
                video_path = os.path.join(tmpdir, "input.mp4")
                urllib.request.urlretrieve(video_url, video_path)
                logger.info(f"Download complete: {os.path.getsize(video_path)} bytes")
                
                cut_cmd_fallback = [
                    "ffmpeg", "-y",
                    "-threads", FFMPEG_THREADS,
                    "-ss", str(start),
                    "-i", video_path,
                    "-t", str(duration),
                    "-c:v", "libx264",
                    "-crf", FFMPEG_CRF,
                    "-preset", FFMPEG_PRESET,
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-threads", FFMPEG_THREADS,
                    temp_segment_path
                ]
                result = subprocess.run(cut_cmd_fallback, capture_output=True, text=True)
                if result.returncode != 0:
                    logger.error(f"FFmpeg fallback cut error: {result.stderr}")
                    return jsonify({"error": f"FFmpeg error: {result.stderr[:500]}"}), 500
                
                # Clean up downloaded file immediately to free disk space
                try:
                    os.remove(video_path)
                    logger.info("Cleaned up downloaded video file")
                except:
                    pass
            
            logger.info(f"Cut complete")
            
            # For subtitle timing, use the requested start time
            # Since we're using -ss before -i, the output starts at 0
            actual_start = start
            logger.info(f"Using start time for subtitles: {actual_start}")

            # OPTIMIZED: Apply subtitles in single pass (no separate PTS reset needed)
            if segment_words:
                ass_path = os.path.join(tmpdir, f"segment_{output_idx}.ass")
                create_copernicus_ass(segment_words, actual_start, ass_path, keywords)

                subtitle_cmd = [
                    "ffmpeg", "-y",
                    "-threads", FFMPEG_THREADS,
                    "-i", temp_segment_path,
                    "-vf", f"ass={ass_path}",
                    "-c:v", "libx264",
                    "-crf", FFMPEG_CRF,
                    "-preset", FFMPEG_PRESET,
                    "-c:a", "copy",  # Copy audio without re-encoding
                    "-threads", FFMPEG_THREADS,
                    "-max_muxing_queue_size", "1024",
                    output_path
                ]
                result = subprocess.run(subtitle_cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    logger.error(f"FFmpeg subtitle error: {result.stderr}")
                logger.info(f"Subtitles applied")
                
                # Clean up temp segment to free memory
                try:
                    os.remove(temp_segment_path)
                except:
                    pass
            else:
                output_path = temp_segment_path
                logger.info(f"No words for segment, skipping subtitles")

            filename = video_url.split('/')[-1]
            output_key = f"segment_{output_idx}_{filename}"
            
            s3.upload_file(output_path, output_bucket, output_key)

            results.append({
                "segment": output_idx,
                "url": f"https://cdn.vvcontent.com/{output_key}",
                "dropbox_path": None,
                "duration": duration,
                "actual_start": actual_start,
                "requested_start": start,
                "pts_difference": 0
            })

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

        srt_content = ""
        chunk_size = 4
        subtitle_index = 1
        
        for i in range(0, len(words), chunk_size):
            chunk = words[i:i + chunk_size]
            if not chunk:
                continue
            
            start_ms = chunk[0]['start']
            end_ms = chunk[-1]['end']
            
            start_time = format_srt_time(start_ms)
            end_time = format_srt_time(end_ms)
            
            text = " ".join([w['text'] for w in chunk])
            
            srt_content += f"{subtitle_index}\n"
            srt_content += f"{start_time} --> {end_time}\n"
            srt_content += f"{text}\n\n"
            
            subtitle_index += 1
        
        if video_url:
            filename = video_url.split('/')[-1].replace('.mp4', '_subtitles.srt')
        else:
            filename = f"subtitles_{int(time.time())}.srt"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.srt', delete=False, encoding='utf-8') as tmp:
            tmp.write(srt_content)
            tmp_path = tmp.name
        
        try:
            s3.upload_file(tmp_path, output_bucket, filename)
        finally:
            os.unlink(tmp_path)
        
        return jsonify({
            "success": True,
            "srt_url": f"https://cdn.vvcontent.com/{filename}",
            "dropbox_path": None,
            "filename": filename
        }), 200

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
