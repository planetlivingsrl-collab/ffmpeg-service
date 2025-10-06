from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile

app = Flask(__name__)

R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY')
R2_SECRET_KEY = os.environ.get('R2_SECRET_KEY')
R2_INPUT_BUCKET = os.environ.get('R2_INPUT_BUCKET')  # bucket video sorgente
R2_OUTPUT_BUCKET = os.environ.get('R2_OUTPUT_BUCKET')  # bucket video processati

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

@app.route('/process', methods=['POST'])
def process_video():
    try:
        data = request.json
        video_url = data.get('video_url')
        segments = data.get('segments')
        
        if not video_url or not segments:
            return jsonify({"error": "Missing video_url or segments"}), 400
        
        s3 = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY
        )
        
        results = []
        
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, 'input.mp4')
            s3.download_file(R2_INPUT_BUCKET, video_url, video_path)
            
            for idx, segment in enumerate(segments):
                start = segment['start']
                end = segment['end']
                duration = end - start
                subtitles = segment.get('subtitles', [])
                
                segment_path = os.path.join(tmpdir, f'segment_{idx}.mp4')
                srt_path = os.path.join(tmpdir, f'segment_{idx}.srt')
                output_path = os.path.join(tmpdir, f'output_{idx}.mp4')
                
                cut_cmd = [
                    'ffmpeg', '-i', video_path,
                    '-ss', str(start),
                    '-t', str(duration),
                    '-c', 'copy',
                    segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)
                
                generate_srt(subtitles, srt_path)
                
                subtitle_cmd = [
                    'ffmpeg', '-i', segment_path,
                    '-vf', f"subtitles={srt_path}:force_style='FontSize=24,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BorderStyle=3,Outline=2,Shadow=1,MarginV=20'",
                    '-c:a', 'copy',
                    output_path
                ]
                subprocess.run(subtitle_cmd, check=True, capture_output=True)
                
                output_key = f'processed/segment_{idx}_{os.path.basename(video_url)}'
                s3.upload_file(output_path, R2_OUTPUT_BUCKET, output_key)
                
                output_url = f"{R2_ENDPOINT}/{R2_OUTPUT_BUCKET}/{output_key}"
                
                results.append({
                    "segment": idx,
                    "url": output_url,
                    "duration": duration
                })
        
        return jsonify({"success": True, "results": results}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def generate_srt(subtitles, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        for idx, sub in enumerate(subtitles, 1):
            start_time = format_time(sub['start'])
            end_time = format_time(sub['end'])
            text = sub['text']
            
            f.write(f"{idx}\n")
            f.write(f"{start_time} --> {end_time}\n")
            f.write(f"{text}\n\n")

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
