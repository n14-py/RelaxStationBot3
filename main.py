import os
import random
import subprocess
import logging
import time
import requests
import hashlib
import mimetypes
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
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/relaxstationmedios/master/mediosmusic.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

# Plantillas de títulos dinámicos
TITULOS = [
    "🎧 {keywords} • Música Continua 24/7",
    "🎶 {keywords} • Mix Relajante Sin Interrupciones",
    "🔥 Lo Mejor de {keywords} • Stream Ininterrumpido",
    "🌌 {keywords} • Ambiente Musical Perfecto",
    "⚡ {keywords} • Energía Musical Continua",
    "🧘♂️ {keywords} • Música para Relajarse",
    "🌿 {keywords} • Sonidos para Concentrarse",
    "🎹 {keywords} • Selección Premium Musical",
    "🌙 {keywords} • Música Nocturna Continua",
    "🚀 {keywords} • Mix de Productividad"
]

PALABRAS_CLAVE = {
    'naturaleza': ['Bosque', 'Lluvia', 'Relax Natural', 'Paisajes Sonoros'],
    'ciudad': ['Urbano', 'Ciudad', 'Vida Moderna', 'Metrópolis'],
    'abstracto': ['Arte', 'Geometría', 'Creatividad', 'Diseño'],
    'espacio': ['Galaxia', 'Estrellas', 'Universo', 'Cósmicos'],
    'vintage': ['Retro', 'Clásico', 'Nostalgia', 'Analógico']
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def procesar_imagen(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Procesando imagen: {url}")
            temp_path = os.path.join(self.media_cache_dir, f"temp_{nombre_hash}")
            
            # Descargar y convertir imagen
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:-1:-1",
                "-q:v", "2",
                "-pix_fmt", "yuv420p",
                ruta_local
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            
            os.remove(temp_path)
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def descargar_musica(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.mp3")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando música: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return ruta_local
        except Exception as e:
            logging.error(f"Error descargando música: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            # Procesar imágenes
            for img in datos['imagenes']:
                img['local_path'] = self.procesar_imagen(img['url'])
            
            # Procesar música
            for musica in datos['musica']:
                musica['local_path'] = self.descargar_musica(musica['url'])
            
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = build('youtube', 'v3', credentials=self.autenticar())
    
    def autenticar(self):
        creds = Credentials(
            token="",
            refresh_token=YOUTUBE_CREDS['refresh_token'],
            client_id=YOUTUBE_CREDS['client_id'],
            client_secret=YOUTUBE_CREDS['client_secret'],
            token_uri="https://oauth2.googleapis.com/token",
            scopes=['https://www.googleapis.com/auth/youtube']
        )
        creds.refresh(Request())
        return creds
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            # Crear broadcast
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música continua 24/7 • Mezcla profesional de los mejores temas\n\n🔔 Activa las notificaciones\n👍 Déjanos tu like\n💬 Sugiere canciones en los comentarios\n\n#MusicaContinua #LiveStream #MusicaSinParar",
                        "scheduledStartTime": (datetime.utcnow() + timedelta(seconds=30)).isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True
                    }
                }
            ).execute()

            # Crear stream
            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {
                        "title": "Stream de música continua"
                    },
                    "cdn": {
                        "format": "1080p",
                        "ingestionType": "rtmp",
                        "resolution": "1080p",
                        "frameRate": "30fps"
                    }
                }
            ).execute()

            # Vincular broadcast y stream
            self.youtube.liveBroadcasts().bind(
                part="id,contentDetails",
                id=broadcast['id'],
                streamId=stream['id']
            ).execute()

            # Subir miniatura
            self.youtube.thumbnails().set(
                videoId=broadcast['id'],
                media_body=imagen_path,
                media_mime_type='image/jpeg'
            ).execute()

            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None

    def finalizar_transmision(self, broadcast_id):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus="complete",
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error finalizando transmisión: {str(e)}")
            return False

def generar_titulo(nombre_imagen):
    # Extraer palabras clave
    nombre = nombre_imagen.lower()
    categoria = 'abstracto'
    for key, words in PALABRAS_CLAVE.items():
        if any(word.lower() in nombre for word in words):
            categoria = key
            break
    
    keywords = random.sample(PALABRAS_CLAVE[categoria], 2)
    plantilla = random.choice(TITULOS)
    return plantilla.format(keywords=' • '.join(keywords))

def manejar_transmision(gestor, youtube, imagen):
    ffmpeg_process = None
    stream_info = None
    try:
        # Generar título
        titulo = generar_titulo(imagen['name'])
        logging.info(f"📝 Título generado: {titulo}")
        
        # Crear transmisión
        stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
        if not stream_info:
            return False

        # Configurar FIFO
        fifo_path = os.path.join(gestor.media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg optimizado
        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", imagen['local_path'],
            "-f", "mp3",
            "-i", fifo_path,
            "-vf", "format=yuv420p,scale=1280:720",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "3000k",
            "-maxrate", "3000k",
            "-bufsize", "6000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-f", "flv",
            stream_info['rtmp']
        ]

        ffmpeg_process = subprocess.Popen(ffmpeg_cmd)
        logging.info("🟢 Transmisión activa - Reproduciendo música...")

        # Transmitir música continuamente
        while True:
            musica = random.choice([m for m in gestor.medios['musica'] if m['local_path']])
            if not musica['local_path']:
                continue
            
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            try:
                with open(musica['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
            except Exception as e:
                logging.error(f"Error enviando audio: {str(e)}")

    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        return False
    finally:
        if ffmpeg_process:
            ffmpeg_process.terminate()
            try:
                ffmpeg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                ffmpeg_process.kill()
        if stream_info:
            youtube.finalizar_transmision(stream_info['broadcast_id'])
    return True

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            # Seleccionar nueva imagen cada 8 horas
            imagen = random.choice([i for i in gestor.medios['imagenes'] if i['local_path']])
            logging.info(f"🖼️ Nueva transmisión con imagen: {imagen['name']}")
            
            start_time = time.time()
            while time.time() - start_time < 28800:  # 8 horas
                if not manejar_transmision(gestor, youtube, imagen):
                    time.sleep(30)
                    break
            
        except Exception as e:
            logging.error(f"Error crítico: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando transmisor de música continua...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
