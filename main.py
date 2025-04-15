import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import json
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from flask import Flask
from waitress import serve
from urllib.parse import urlparse
import threading
import signal
import sys

app = Flask(__name__)

# Configuración logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración
CONFIG = {
    "MEDIOS_URL": "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json",
    "CACHE_DIR": os.path.abspath("./radio_cache"),
    "STREAM_DURATION": 8 * 3600,  # 8 horas en segundos
    "RETRY_DELAY": 300,  # 5 minutos entre reintentos
    "YOUTUBE_CREDS": {
        'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
        'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
        'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
    },
    "FFMPEG_PARAMS": {
        "video_codec": "libx264",
        "audio_codec": "aac",
        "video_bitrate": "3000k",
        "audio_bitrate": "192k",
        "resolution": "1280x720",
        "fps": "30",
        "preset": "ultrafast",
        "tune": "stillimage"
    }
}

class ContentManager:
    def __init__(self):
        os.makedirs(CONFIG['CACHE_DIR'], exist_ok=True)
        self.media_data = self._load_media_data()
        self._verify_content()
    
    def _load_media_data(self):
        try:
            response = requests.get(CONFIG["MEDIOS_URL"], timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}
    
    def _verify_content(self):
        if not self.media_data.get('imagenes'):
            logging.error("No se encontraron imágenes en el JSON")
        if not self.media_data.get('musica'):
            logging.error("No se encontraron pistas de audio en el JSON")
    
    def _process_google_drive_url(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            return url
        except:
            return url
    
    def _download_file(self, url, is_image=False):
        try:
            url = self._process_google_drive_url(url)
            file_hash = hashlib.md5(url.encode()).hexdigest()
            extension = ".jpg" if is_image else ".mp3"
            local_path = os.path.join(CONFIG['CACHE_DIR'], f"{file_hash}{extension}")
            
            if os.path.exists(local_path):
                return local_path
            
            logging.info(f"Descargando {'imagen' if is_image else 'audio'}: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            return self._optimize_image(local_path) if is_image else local_path
        
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None
    
    def _optimize_image(self, image_path):
        try:
            optimized_path = f"{image_path}_optimized.jpg"
            subprocess.run([
                "ffmpeg", "-y", "-i", image_path,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2", "-compression_level", "6",
                optimized_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return optimized_path if os.path.exists(optimized_path) else image_path
        except Exception as e:
            logging.error(f"Error optimizando imagen: {str(e)}")
            return image_path
    
    def get_random_content(self):
        try:
            image = random.choice([
                img for img in self.media_data['imagenes']
                if self._download_file(img['url'], is_image=True)
            ])
            music = random.sample([
                track for track in self.media_data['musica']
                if self._download_file(track['url'])
            ], min(50, len(self.media_data['musica'])))
            
            return {
                "image": {
                    "name": image['name'],
                    "path": self._download_file(image['url'], is_image=True)
                },
                "music": [{
                    "name": track['name'],
                    "path": self._download_file(track['url'])
                } for track in music]
            }
        except Exception as e:
            logging.error(f"Error seleccionando contenido: {str(e)}")
            return None

class YouTubeStreamer:
    def __init__(self):
        self.youtube = self._authenticate()
    
    def _authenticate(self):
        try:
            creds = Credentials(
                token="",
                refresh_token=CONFIG['YOUTUBE_CREDS']['refresh_token'],
                client_id=CONFIG['YOUTUBE_CREDS']['client_id'],
                client_secret=CONFIG['YOUTUBE_CREDS']['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"Error autenticando YouTube: {str(e)}")
            return None
    
    def create_stream(self, title, image_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=2)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": title,
                        "description": "🎵 24/7 Relax Station Radio • Música Continua\n\nDisfruta de nuestra programación musical las 24 horas\n\n🔔 Activa las notificaciones\n\n#MusicaContinua #RadioOnline #Relax",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "enableArchive": True
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Main Radio Stream"},
                    "cdn": {
                        "ingestionType": "rtmp",
                        "resolution": "1080p",
                        "frameRate": "30fps"
                    }
                }
            ).execute()

            self.youtube.liveBroadcasts().bind(
                part="id,contentDetails",
                id=broadcast['id'],
                streamId=stream['id']
            ).execute()

            if image_path and os.path.exists(image_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=image_path
                ).execute()

            return {
                "rtmp_url": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "start_time": scheduled_start
            }
        except Exception as e:
            logging.error(f"Error creando stream: {str(e)}")
            return None
    
    def transition_stream(self, broadcast_id, status):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=status,
                id=broadcast_id,
                part="status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error cambiando estado a {status}: {str(e)}")
            return False
    
    def end_stream(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id,
                part="status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error finalizando stream: {str(e)}")
            return False

class RadioEngine:
    def __init__(self):
        self.content_manager = ContentManager()
        self.youtube = YouTubeStreamer()
        self.current_stream = None
        self.ffmpeg_process = None
    
    def _generate_playlist(self, tracks):
        playlist_path = os.path.join(CONFIG['CACHE_DIR'], "playlist.m3u")
        with open(playlist_path, "w") as f:
            f.write("#EXTM3U\n")
            for track in tracks:
                f.write(f"#EXTINF:-1,{track['name']}\n{track['path']}\n")
        return playlist_path
    
    def _build_ffmpeg_command(self, image_path, playlist_path, rtmp_url):
        return [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", image_path,
            "-f", "concat",
            "-safe", "0",
            "-i", playlist_path,
            "-vf", f"scale={CONFIG['FFMPEG_PARAMS']['resolution']},setsar=1",
            "-c:v", CONFIG['FFMPEG_PARAMS']['video_codec'],
            "-preset", CONFIG['FFMPEG_PARAMS']['preset'],
            "-tune", CONFIG['FFMPEG_PARAMS']['tune'],
            "-b:v", CONFIG['FFMPEG_PARAMS']['video_bitrate'],
            "-r", CONFIG['FFMPEG_PARAMS']['fps'],
            "-g", "60",
            "-c:a", CONFIG['FFMPEG_PARAMS']['audio_codec'],
            "-b:a", CONFIG['FFMPEG_PARAMS']['audio_bitrate'],
            "-f", "flv",
            rtmp_url
        ]
    
    def _start_stream(self, content):
        try:
            playlist_path = self._generate_playlist(content['music'])
            stream_info = self.youtube.create_stream(
                f"🎧 {content['image']['name']} • Música Continua • 24/7",
                content['image']['path']
            )
            
            if not stream_info:
                return False

            cmd = self._build_ffmpeg_command(
                content['image']['path'],
                playlist_path,
                stream_info['rtmp_url']
            )
            
            self.ffmpeg_process = subprocess.Popen(cmd)
            logging.info("Iniciando FFmpeg...")
            
            # Esperar conexión estable
            time.sleep(30)
            
            if self.youtube.transition_stream(stream_info['broadcast_id'], 'live'):
                logging.info("Transmisión LIVE activada")
                self.current_stream = {
                    "start_time": datetime.now(),
                    "broadcast_id": stream_info['broadcast_id'],
                    "process": self.ffmpeg_process
                }
                return True
            return False
        except Exception as e:
            logging.error(f"Error iniciando stream: {str(e)}")
            return False
    
    def _monitor_stream(self):
        while self.ffmpeg_process and self.ffmpeg_process.poll() is None:
            if (datetime.now() - self.current_stream['start_time']).total_seconds() > CONFIG['STREAM_DURATION']:
                logging.info("Tiempo de transmisión completado")
                self.stop_stream()
                break
            time.sleep(30)
    
    def start_radio(self):
        while True:
            try:
                content = self.content_manager.get_random_content()
                if not content:
                    logging.error("No se pudo obtener contenido, reintentando...")
                    time.sleep(CONFIG['RETRY_DELAY'])
                    continue
                
                if self._start_stream(content):
                    self._monitor_stream()
                else:
                    time.sleep(CONFIG['RETRY_DELAY'])
                
            except Exception as e:
                logging.error(f"Error crítico: {str(e)}")
                time.sleep(CONFIG['RETRY_DELAY'])
    
    def stop_stream(self):
        if self.ffmpeg_process:
            self.ffmpeg_process.terminate()
            try:
                self.ffmpeg_process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.ffmpeg_process.kill()
            self.ffmpeg_process = None
        
        if self.current_stream:
            self.youtube.end_stream(self.current_stream['broadcast_id'])
            self.current_stream = None

def signal_handler(sig, frame):
    logging.info("Deteniendo radio...")
    radio.stop_stream()
    sys.exit(0)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    radio = RadioEngine()
    
    logging.info("Iniciando servidor de radio 24/7...")
    radio_thread = threading.Thread(target=radio.start_radio, daemon=True)
    radio_thread.start()
    
    serve(app, host='0.0.0.0', port=10000)
