from flask import Flask, request, jsonify
import os
import boto3
import subprocess
import tempfile
from botocore.config import Config   # <— AGGIUNTO

app = Flask(__name__)

# Configurazione R2 da variabili d'ambiente
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_ACCESS_KEY = os.environ.get('R2_ACCESS_KEY')
R2_SECRET_KEY = os.environ.get('R2_SECRET_KEY')
R2_REGION     = os.environ.get('R2_REGION', 'us-east-1')  # <— AGGIUNTO (default corretto)

def make_r2_s3_client(endpoint, access_key, secret_key, region='us-east-1'):
    """Client S3 compatibile Cloudflare R2 (path-style + SigV4)."""
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}   # <— OBBLIGATORIO con R2
        )
    )

# Client S3 globale per retrocompatibilità
s3 = make_r2_s3_client(R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_REGION) \
     if R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY else None

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"}), 200

@app.route('/process', methods=['POST'])
def process_video():
    try:
        raw_data = request.json
        data = raw_data.get('body', raw_data) if isinstance(raw_data, dict) else raw_data

        s3_config       = data.get('s3_config')
        video_url       = data.get('video_url')
        segments        = data.get('segments')
        subtitles_data  = data.get('subtitles')

        if not segments:
            return jsonify({"error": "Missing segments"}), 400

        # Determina quale client S3 usare
        if s3_config:
            # Usa SEMPRE una region reale; se non arriva dal client, default us-east-1
            region = s3_config.get('region') or 'us-east-1'
            s3_client = make_r2_s3_client(
                endpoint=s3_config['endpoint'],
                access_key=s3_config['accessKeyId'],
                secret_key=s3_config['secretAccessKey'],
                region=region
            )
            input_bucket  = s3_config['bucket']
            output_bucket = s3_config.get('output_bucket', 'shortconsottotitoli')
            video_key     = s3_config['key']
        elif video_url:
            if not s3:
                return jsonify({"error": "S3 client not configured"}), 500
            s3_client     = s3
            input_bucket  = os.environ.get('R2_INPUT_BUCKET', 'videoliving')
            output_bucket = os.environ.get('R2_OUTPUT_BUCKET', 'shortconsottotitoli')
            # estrai la key in modo robusto
            from urllib.parse import urlparse, unquote
            path = urlparse(video_url).path
            video_key = unquote(path.lstrip('/').split('/')[-1])
        else:
            return jsonify({"error": "Missing video_url or s3_config"}), 400

        # Pre-check esplicito: fallisce subito se key/bucket sono errati (evita timeout lunghi)
        s3_client.head_object(Bucket=input_bucket, Key=video_key)

        results = []
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = os.path.join(tmpdir, 'input.mp4')
            s3_client.download_file(input_bucket, video_key, video_path)

            for idx, segment in enumerate(segments):
                start = segment['start']
                end   = segment['end']
                duration = end - start

                # Sottotitoli per segmento (se presenti)
                segment_subtitles = None
                if subtitles_data:
                    for sub_entry in subtitles_data:
                        if sub_entry.get('segment_index') == idx:
                            segment_subtitles = sub_entry.get('subtitle_srt')
                            break

                segment_path = os.path.join(tmpdir, f'segment_{idx}.mp4')
                output_path  = os.path.join(tmpdir, f'output_{idx}.mp4')

                cut_cmd = [
                    'ffmpeg', '-y', '-i', video_path,
                    '-ss', str(start), '-t', str(duration),
                    '-c', 'copy', segment_path
                ]
                subprocess.run(cut_cmd, check=True, capture_output=True)

                if segment_subtitles:
                    srt_path = os.path.join(tmpdir, f'segment_{idx}.srt')
                    with open(srt_path, 'w', encoding='utf-8') as f:
                        f.write(segment_subtitles)

                    # Nota: path con spazi va quotato
                    subtitle_cmd = [
                        'ffmpeg', '-y', '-i', segment_path,
                        '-vf', f"subtitles='{srt_path}':force_style='FontSize=24,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,BorderStyle=3,Outline=2,Shadow=1,MarginV=20'",
                        '-c:a', 'copy', output_path
                    ]
                    subprocess.run(subtitle_cmd, check=True, capture_output=True)
                else:
                    output_path = segment_path

                output_key = f'segment_{idx}_{os.path.basename(video_key)}'
                s3_client.upload_file(output_path, output_bucket, output_key)

                # URL di comodo (non firmato). Va bene se il bucket/oggetto è pubblico;
                # per uso interno puoi rispondere con bucket/key.
                base = s3_config['endpoint'] if s3_config else R2_ENDPOINT
                results.append({
                    "segment": idx,
                    "url": f"{base}/{output_bucket}/{output_key}",
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
