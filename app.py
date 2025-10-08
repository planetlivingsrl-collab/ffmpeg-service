from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile

app = Flask(__name__)

# Configurazione R2 da variabili d'ambiente
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY')
R2_SECRET_KEY = os.environ.get('R2_SECRET_KEY')

# Client S3 globale per retrocompatibilità
s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY
) if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY else None

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

@app.route('/process', methods=['POST'])
def process_video():
    try:
        # Supporta sia body wrapper che payload diretto
        raw_data = request.json
        data = raw_data.get('body', raw_data) if isinstance(raw_data, dict) else raw_data
        
        # Supporta sia s3_config che video_url
        s3_config = data.get('s3_config')
        video_url = data.get('video_url')
        segments = data.get('segments')
        subtitles_data = data.get('subtitles')
        
        if not segments:
            return jsonify({"error": "Missing segments"}), 400
        
        # Determina quale client S3 usare
        if s3_config:
            s3_client = boto3.client(
                's3',
                endpoint_url=s3_config['endpoint'],
                aws_access_key_id=s3_config['accessKeyId'],
                aws_secret_access_key=s3_config['secretAccessKey'],
                region_name=s3_config.get('region', 'auto')
            )
            input_bucket = s3_config['bucket']
            output_bucket = s3_config.get('output_bucket', 'shortconsottotitoli')
            video_key = s3_config['key']
        elif video_url:
            if not s3:
                return jsonify({"error": "S3 client not configured"}), 500
            s3_client = s3
            input_bucket = os.environ.get('R2_INPUT_BUCKET', 'videoliving')
            output_bucket = os.environ.get('R2_OUTPUT_BUCKET', 'shortconsottotitoli')
            video_key = video_url.split('/')[-1]
        else:
            return jsonify({"error": "Missing video_url or s3_config"}), 400
        
        results = []
        
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, 'input.mp4')
            s3_client.download_file(input_bucket, video_key, video_path)
            
            for idx, segment in enumerate(segments):
                start = segment['start']
                end = segment['end']
                duration = end - start
                
                segment_subtitles = None
                if subtitles_data:
                    for sub_entry in subtitles_data:
                        if sub_entry.get('segment_index') == idx:
                            segment_subtitles = sub_entry.get('subtitle_srt')
                            break
                
                segment_path = os.path.join(tmpdir, f'segment_{idx}.mp4')
                output_path = os.path.join(tmpdir, f'output_{idx}.mp4')
                
                cut_cmd = [
                    'ffmpeg', '-y', '-i', video_path,
                    '-ss', str(start),
                    '-t', str(duration),
                    '-c', 'copy',
                    segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)
                
                if segment_subtitles:
                    srt_path = os.path.join(tmpdir, f'segment_{idx}.srt')
                    with open(srt_path, 'w', encoding='utf-8') as f:
                        f.write(segment_subtitles)
                    
                    subtitle_cmd = [
                        'ffmpeg', '-y', '-i', segment_path,
                        '-vf', f"subtitles={srt_path}:force_style='FontSize=24,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BorderStyle=3,Outline=2,Shadow=1,MarginV=20'",
                        '-c:a', 'copy',
                        output_path
                    ]
                    subprocess.run(subtitle_cmd, check=True, capture_output=True)
                else:
                    output_path = segment_path
                
                output_key = f'segment_{idx}_{os.path.basename(video_key)}'
                s3_client.upload_file(output_path, output_bucket, output_key)
                
                if s3_config:
                    output_url = f"{s3_config['endpoint']}/{output_bucket}/{output_key}"
                else:
                    output_url = f"{R2_ENDPOINT}/{output_bucket}/{output_key}"
                
                results.append({
                    "segment": idx,
                    "url": output_url,
                    "duration": duration
                })
        
        return jsonify({"success": True, "results": results}), 200
        
    except subprocess.CalledProcessError as e:
        return jsonify({"error": f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
