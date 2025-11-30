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
    return jsonify({"status": "ok", "version": "2.1-color-fix"}), 200

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

def create_copernicus_ass(words, segment_start, output_path, keywords=None):
    """Create ASS subtitle file with colored keywords - optimized for 1080x1920 vertical video"""
    
    if keywords is None:
        keywords = []
    
    keywords_lower = [k.lower().strip() for k in keywords]
    
    logger.info(f"=== ASS CREATION DEBUG ===")
    logger.info(f"Keywords received: {keywords_lower}")
    logger.info(f"Number of words: {len(words)}")
    
    # Stile default: bianco
    ass_content = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial Black,60,&H00FFFFFF,&H00FFFFFF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,4,0,2,60,60,200,1

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
    normal_count = 0
    
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
            
            is_keyword = word_clean in keywords_lower or word_lower in keywords_lower
            
            if is_keyword:
                # Keyword: rosso (BGR: 0000FF = rosso), poi reset
                words_in_chunk.append({
                    'text': word_text,
                    'styled': f"{{\\1c&H0000FF&}}{word_text}{{\\r}}",
                    'is_keyword': True
                })
                keyword_count += 1
                logger.info(f"KEYWORD: '{word_text}' (matched: '{word_clean}')")
            else:
                # Parola normale: nessun tag, usa stile default (bianco)
                words_in_chunk.append({
                    'text': word_text,
                    'styled': word_text,
                    'is_keyword': False
                })
                normal_count += 1
        
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
    
    logger.info(f"=== ASS STATS ===")
    logger.info(f"Keywords styled: {keyword_count}")
    logger.info(f"Normal words: {normal_count}")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(ass_content)
    
    logger.info(f"ASS file created: {output_path}")

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
                "content": f"Analizza questo testo in italiano e identifica SOLO le 10-15 PAROLE CHIAVE più importanti (nomi propri, numeri importanti, verbi d'azione forti, concetti chiave). NON includere articoli, congiunzioni, preposizioni o parole comuni come 'ciao', 'benvenuto', 'sono', 'questo'. Rispondi SOLO con un array JSON di parole, tutto minuscolo, senza punteggiatura.\n\nTesto: {full_text}\n\nRispondi nel formato: [\"parola1\", \"parola2\", \"parola3\"]"
            }]
        }
        
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Anthropic API error: {response.status_code} - {response.text}")
            return jsonify({"error": f"API error: {response.status_code}"}), 500
        
        response_data = response.json()
        response_text = response_data["content"][0]["text"]
        logger.info(f"AI Response: {response_text}")
        
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
        logger.error(f"Error identifying keywords: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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
        keywords = data.get("keywords", [])
        
        logger.info(f"=== PROCESS VIDEO DEBUG ===")
        logger.info(f"Keywords received: {keywords}")
        logger.info(f"Number of keywords: {len(keywords)}")
        
        segment_idx = data.get("segment_index", 0)
        output_idx = data.get("output_index", segment_idx)
        
        if isinstance(segment_idx, str):
            segment_idx = int(segment_idx)
        if isinstance(output_idx, str):
            output_idx = int(output_idx)
        
        logger.info(f"EXTRACTED segment_index: {segment_idx}, output_index: {output_idx}")
        
        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        
        if not video_url:
            return jsonify({"error": "Missing video_url"}), 400

        if not s3:
            return jsonify({"error": "S3 client not configured"}), 500

        if segment_idx >= len(segments):
            return jsonify({"error": f"segment_index {segment_idx} out of range"}), 400
        
        target_segment = segments[segment_idx]
        segments_to_process = [target_segment]

        logger.info(f"Processing video: {video_url}")

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
            logger.info(f"Received {len(all_words)} words")

            for segment in segments_to_process:
                logger.info(f"Processing segment {output_idx}")
                start = segment["start"]
                end = segment["end"]
                duration = end - start

                segment_words = [w for w in all_words if w['start'] >= start and w['end'] <= end]
                logger.info(f"Filtered segment_words: {len(segment_words)}")
                
                segment_path = os.path.join(tmpdir, f"segment_{output_idx}.mp4")
                output_path = os.path.join(tmpdir, f"output_{output_idx}.mp4")

                cut_cmd = [
                    "ffmpeg", "-y", "-i", video_path,
                    "-ss", str(start), "-t", str(duration),
                    "-c", "copy", segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)

                if segment_words:
                    ass_path = os.path.join(tmpdir, f"segment_{output_idx}.ass")
                    create_copernicus_ass(segment_words, start, ass_path, keywords)
                    
                    # Log del contenuto ASS per debug
                    with open(ass_path, 'r') as f:
                        ass_content = f.read()
                    logger.info(f"=== ASS FILE CONTENT (first 500 chars) ===")
                    logger.info(ass_content[:500])

                    subtitle_cmd = [
                        "ffmpeg", "-y", "-i", segment_path,
                        "-vf", f"ass={ass_path}",
                        "-c:a", "copy", output_path
                    ]
                    subprocess.run(subtitle_cmd, check=True, capture_output=True)
                else:
                    output_path = segment_path

                filename = video_url.split('/')[-1]
                output_key = f"segment_{output_idx}_{filename}"
                
                logger.info(f"Uploading segment {output_idx} to R2 as {output_key}")
                s3.upload_file(output_path, output_bucket, output_key)

                results.append({
                    "segment": output_idx,
                    "url": f"https://cdn.vvcontent.com/{output_key}",
                    "dropbox_path": None,
                    "duration": duration
                })
                
                logger.info(f"Segment {output_idx} completed")

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
            logger.info(f"SRT uploaded to R2 as {filename}")
        finally:
            os.unlink(tmp_path)
        
        return jsonify({
            "success": True,
            "srt_url": f"https://cdn.vvcontent.com/{filename}",
            "dropbox_path": None,
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
