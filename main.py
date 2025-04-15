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

# Configuración logging
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

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
        self.playlist_path = os.path.join(self.media_cache_dir, "playlist.txt")

    def generar_playlist(self):
        canciones = [m for m in self.medios['musica'] if m.get('local_path')]
        random.shuffle(canciones)
        
        with open(self.playlist_path, "w") as f:
            for cancion in canciones:
                f.write(f"file '{cancion['local_path']}'\n")
        return self.playlist_path

    def procesar_imagen(self, ruta_original):
        try:
            nombre_hash = hashlib.md5(ruta_original.encode()).hexdigest()
            ruta_procesada = os.path.join(self.media_cache_dir, f"{nombre_hash}_opt.jpg")
            
            subprocess.run([
                "ffmpeg", "-y",
                "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=increase",
                "-q:v", "2",
                "-compression_level", "6",
                ruta_procesada
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            return ruta_procesada if os.path.exists(ruta_procesada) else None
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def descargar_imagen(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")
            
            if os.path.exists(ruta_local):
                return self.procesar_imagen(ruta_local)

            logging.info(f"⬇️ Descargando imagen: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            return self.procesar_imagen(ruta_local)
        except Exception as e:
            logging.error(f"Error descargando imagen: {str(e)}")
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

            logging.info(f"⬇️ Descargando canción: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando audio: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar música
            datos['musica'] = [{"name": m['name'], "url": m['url'], "local_path": self.descargar_audio(m['url'])} 
                              for m in datos.get('musica', [])]
            
            # Procesar imágenes
            datos['imagenes'] = []
            for img in datos.get('imagenes', []):
                processed_path = self.descargar_imagen(img['url'])
                if processed_path:
                    datos['imagenes'].append({"name": img['name'], "local_path": processed_path})
                else:
                    logging.warning(f"Imagen no disponible: {img['name']}")
            
            if not datos['imagenes']:
                logging.error("❌ No hay imágenes válidas disponibles")
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"musica": [], "imagenes": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
    def autenticar(self):
        try:
            creds = Credentials(
                token="",
                refresh_token=YOUTUBE_CREDS['refresh_token'],
                client_id=YOUTUBE_CREDS['client_id'],
                client_secret=YOUTUBE_CREDS['client_secret'],
                token_uri="https://oauth2.googleapis.com/token",
                scopes=['https://www.googleapis.com/auth/youtube']
            )
            creds.refresh(Request())
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"Error autenticación YouTube: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                  "title": titulo,
                  "description": "🎵 Relax Station Radio • Música Continua 24/7\n\nDisfruta de nuestra selección musical las 24 horas del día\n\n🎧 Nueva playlist cada 8 horas\n\n🔔 Síguenos en redes:\nhttps://instagram.com/turadio\nhttps://facebook.com/turadio",
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
                    "snippet": {
                        "title": "Stream de Radio"
                    },
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
            
            # Subir miniatura
            if imagen_path and os.path.exists(imagen_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=imagen_path,
                    media_mime_type='image/jpeg'
                ).execute()
            
            rtmp_url = stream['cdn']['ingestionInfo']['ingestionAddress']
            stream_name = stream['cdn']['ingestionInfo']['streamName']
            
            return {
                "rtmp": f"{rtmp_url}/{stream_name}",
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None

def generar_titulo():
    temas = ['Lofi', 'Chill', 'Relax', 'Estudio', 'Noche']
    return f"🎧 Música {random.choice(temas)} • Live 24/7 • {datetime.now().strftime('%d/%m')}"

def manejar_transmision(stream_data, youtube):
    try:
        gestor = GestorContenido()
        playlist = gestor.generar_playlist()
        imagen = random.choice([img for img in gestor.medios['imagenes'] if img['local_path']])
        
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "concat",
            "-safe", "0",
            "-i", playlist,
            "-vf", "scale=1280:720,setsar=1",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "2500k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-ar", "44100",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(cmd)
        logging.info(f"🟢 Transmitiendo con imagen: {imagen['name']}")
        
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                playlist = gestor.generar_playlist()
                proceso = subprocess.Popen(cmd)
            time.sleep(15)
        
        proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada")

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes']:
                logging.error("🚨 No hay imágenes disponibles en el JSON")
                time.sleep(60)
                continue
                
            titulo = generar_titulo()
            imagen = random.choice(gestor.medios['imagenes'])
            
            stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
            if not stream_info:
                raise Exception("Error al crear transmisión")
            
            threading.Thread(
                target=manejar_transmision,
                args=(stream_info, youtube),
                daemon=True
            ).start()
            
            time.sleep(8 * 3600)  # Esperar 8 horas
            
        except Exception as e:
            logging.error(f"Error: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("📻 Iniciando radio 24/7...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
