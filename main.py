import os
import random
import subprocess
import logging
import time
import requests
import hashlib
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from flask import Flask
from waitress import serve
from urllib.parse import urlparse
import threading

app = Flask(__name__)

# Configuración logging optimizada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Configuración
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/RelaxStationmedios/master/mediosmusic.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

PALABRAS_CLAVE = {
    'relax': ['relax', 'calm', 'peaceful'],
    'instrumental': ['instrumental', 'piano', 'guitar'],
    'ambient': ['ambient', 'atmospheric', 'space'],
    'jazz': ['jazz', 'smooth', 'blues'],
    'classical': ['classical', 'orchestra', 'symphony']
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def obtener_extension_segura(self, url):
        try:
            parsed = urlparse(url)
            return os.path.splitext(parsed.path)[1].lower() or '.mp4'
        except:
            return '.mp4'

    def descargar_video(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = self.obtener_extension_segura(url)
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando video: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando video: {str(e)}")
            return None

    def descargar_audio(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.mp3")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando audio: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando audio: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            logging.info("📡 Obteniendo lista de medios...")
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Descargar videos
            for video in datos['videos']:
                video['local_path'] = self.descargar_video(video['url'])
            
            # Descargar música
            for cancion in datos['musica']:
                cancion['local_path'] = self.descargar_audio(cancion['url'])
            
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"videos": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = None
        self.autenticar()
    
    def autenticar(self):
        try:
            creds = Credentials(
                token=None,
                refresh_token=YOUTUBE_CREDS['refresh_token'],
                client_id=YOUTUBE_CREDS['client_id'],
                client_secret=YOUTUBE_CREDS['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            self.youtube = build('youtube', 'v3', credentials=creds)
            return True
        except Exception as e:
            logging.error(f"Error autenticación YouTube: {str(e)}")
            return False
    
    def generar_miniatura(self, video_path):
        try:
            output_path = "/tmp/miniatura.jpg"
            subprocess.run([
                "ffmpeg",
                "-y", "-ss", "00:00:10",
                "-i", video_path,
                "-vframes", "1",
                "-vf", "scale=1280:720",
                output_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return output_path
        except:
            return None
    
    def crear_transmision(self, titulo, video_path):
        if not self.youtube:
            return None
            
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                    "title": titulo,
                    "scheduledStartTime": scheduled_start.isoformat() + "Z"
                  },
                  "status": {
                    "privacyStatus": "public",
                    "enableAutoStart": True,
                    "enableAutoStop": True
                  }
                }
            ).execute()
            
            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Stream principal"},
                    "cdn": {
                        "ingestionType": "rtmp",
                        "resolution": "1080p",
                        "frameRate": "24fps"
                    }
                }
            ).execute()
            
            self.youtube.liveBroadcasts().bind(
                id=broadcast['id'],
                streamId=stream['id']
            ).execute()
            
            rtmp_url = stream['cdn']['ingestionInfo']['ingestionAddress']
            stream_name = stream['cdn']['ingestionInfo']['streamName']
            
            thumbnail_path = self.generar_miniatura(video_path)
            if thumbnail_path:
                try:
                    self.youtube.thumbnails().set(
                        videoId=broadcast['id'],
                        media_body=thumbnail_path
                    ).execute()
                except:
                    pass
            
            return {
                "rtmp": f"{rtmp_url}/{stream_name}",
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def obtener_estado_stream(self, stream_id):
        if not self.youtube:
            return None
            
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            if response.get('items'):
                return response['items'][0]['status']['streamStatus']
            return None
        except:
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        if not self.youtube:
            return False
            
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id
            ).execute()
            return True
        except:
            return False

    def finalizar_transmision(self, broadcast_id):
        if not self.youtube:
            return False
            
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id
            ).execute()
            return True
        except:
            return False

def determinar_categoria(nombre_musica):
    nombre = nombre_musica.lower()
    for categoria, palabras in PALABRAS_CLAVE.items():
        for palabra in palabras:
            if palabra in nombre:
                return categoria
    return random.choice(list(PALABRAS_CLAVE.keys()))

def seleccionar_musica_aleatoria(gestor):
    canciones = [m for m in gestor.medios['musica'] if m['local_path']]
    return random.choice(canciones) if canciones else None

def generar_titulo_musica(nombre_musica, categoria):
    actividades = [
        ('Relajarse', '😌'), ('Estudiar', '📚'), 
        ('Meditar', '🧘♂️'), ('Dormir', '🌙')
    ]
    
    actividad, emoji_act = random.choice(actividades)
    return f"Lofi Chill {categoria.capitalize()} • Ideal para {actividad} {emoji_act}"

def crear_lista_reproduccion(gestor, duracion_horas=8):
    canciones = [m for m in gestor.medios['musica'] if m['local_path']]
    if not canciones:
        return []
    
    random.shuffle(canciones)
    canciones_necesarias = int((duracion_horas * 60) / 4)
    return (canciones * (canciones_necesarias // len(canciones) + 1))[:canciones_necesarias]

def manejar_transmision(stream_data, youtube):
    try:
        tiempo_inicio_ffmpeg = stream_data['start_time'] - timedelta(minutes=1)
        espera_ffmpeg = max(0, (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds())
        time.sleep(espera_ffmpeg)
        
        lista_archivo = "/tmp/playlist.txt"
        with open(lista_archivo, 'w') as f:
            for cancion in stream_data['playlist']:
                f.write(f"file '{cancion['local_path']}'\n")
        
        # Comando FFmpeg optimizado para 1080p con menor carga
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-f", "concat",
            "-safe", "0",
            "-i", lista_archivo,
            "-stream_loop", "-1",
            "-i", stream_data['video']['local_path'],
            "-map", "0:a:0",
            "-map", "1:v:0",
            "-c:v", "libx264",
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-b:v", "2500k",
            "-maxrate", "3000k",
            "-bufsize", "5000k",
            "-r", "24",
            "-g", "48",
            "-c:a", "aac",
            "-b:a", "96k",
            "-ar", "44100",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Verificar estado del stream
        for _ in range(5):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                youtube.transicionar_estado(stream_data['broadcast_id'], 'testing')
                break
            time.sleep(5)
        
        tiempo_restante = max(0, (stream_data['start_time'] - datetime.utcnow()).total_seconds())
        time.sleep(tiempo_restante)
        
        youtube.transicionar_estado(stream_data['broadcast_id'], 'live')
        logging.info("🎥 TRANSMISIÓN INICIADA")
        
        # Mantener el stream activo por 8 horas
        time.sleep(8 * 3600)
        
        proceso.terminate()
        youtube.finalizar_transmision(stream_data['broadcast_id'])

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            video = random.choice([v for v in gestor.medios['videos'] if v['local_path']])
            playlist = crear_lista_reproduccion(gestor)
            
            if not playlist:
                time.sleep(60)
                continue
                
            primera_cancion = playlist[0]
            categoria = determinar_categoria(primera_cancion['name'])
            titulo = generar_titulo_musica(primera_cancion['name'], categoria)
            
            stream_info = youtube.crear_transmision(titulo, video['local_path'])
            if not stream_info:
                time.sleep(60)
                continue
                
            logging.info(f"🎥 Video seleccionado: {video['name']}")
            logging.info(f"🎵 Primera canción: {primera_cancion['name']} ({categoria})")
            
            stream_data = {
                "rtmp": stream_info['rtmp'],
                "start_time": stream_info['scheduled_start'],
                "video": video,
                "playlist": playlist,
                "broadcast_id": stream_info['broadcast_id'],
                "stream_id": stream_info['stream_id']
            }

            threading.Thread(
                target=manejar_transmision,
                args=(stream_data, youtube),
                daemon=True
            ).start()
            
            # Esperar hasta que termine esta transmisión + margen
            time.sleep(8 * 3600 + 300)
            
        except Exception as e:
            logging.error(f"Error en ciclo: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
