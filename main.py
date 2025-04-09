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

PALABRAS_CLAVE = {
    'naturaleza': ['bosque', 'lluvia', 'jungla', 'montaña', 'río'],
    'urbano': ['ciudad', 'moderno', 'loft', 'ático', 'urbano'],
    'abstracto': ['arte', 'geometría', 'color', 'diseño', 'creativo'],
    'vintage': ['retro', 'clásico', 'nostalgia', 'analógico', 'vintage']
}

PLANTILLAS_TITULOS = [
    "🎧 {keywords} • Música Continua 24/7",
    "🌌 {keywords} • Ambiente Relajante",
    "🔥 {keywords} • Mix Premium",
    "🧘♂️ {keywords} • Música para Meditar",
    "🌿 {keywords} • Sonidos Naturales",
    "🚀 {keywords} • Energía Positiva",
    "🌙 {keywords} • Noche Relajante",
    "🎨 {keywords} • Creatividad Musical"
]

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
            
            if not all(key in datos for key in ["imagenes", "musica"]):
                raise ValueError("Estructura JSON inválida")
            
            # Procesar imágenes
            for img in datos['imagenes']:
                img['local_path'] = self.procesar_imagen(img['url'])
            
            # Procesar música
            for musica in datos['musica']:
                musica['local_path'] = self.descargar_musica(musica['url'])
            
            # Filtrar elementos no descargados
            datos['imagenes'] = [i for i in datos['imagenes'] if i['local_path']]
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            if not datos['imagenes'] or not datos['musica']:
                raise ValueError("No hay medios válidos para transmitir")
            
            logging.info("✅ Medios verificados y listos")
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
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "Disfruta de nuestra selección musical continua con ambientación visual relajante. Ideal para trabajar, estudiar o meditar.\n\n🔔 Activa las notificaciones\n👍 Déjanos tu like\n💬 Sugiere canciones en los comentarios",
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
                "scheduled_start": scheduled_start,
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id']
            }
        except Exception as e:
            logging.error(f"Error creando transmisión: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error transicionando a {estado}: {str(e)}")
            return False
    
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
    nombre = nombre_imagen.lower()
    categoria = 'abstracto'
    
    for key, palabras in PALABRAS_CLAVE.items():
        if any(p in nombre for p in palabras):
            categoria = key
            break
    
    keywords = random.sample(PALABRAS_CLAVE[categoria], 2)
    return random.choice(PLANTILLAS_TITULOS).format(keywords=' • '.join(keywords).title())

def manejar_transmision(stream_data, youtube):
    try:
        tiempo_inicio_ffmpeg = stream_data['scheduled_start'] - timedelta(minutes=1)
        espera_ffmpeg = (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds()
        
        if espera_ffmpeg > 0:
            logging.info(f"⏳ Esperando {espera_ffmpeg:.0f}s para preparar transmisión...")
            time.sleep(espera_ffmpeg)
        
        fifo_path = os.path.join(stream_data['gestor'].media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", stream_data['imagen']['local_path'],
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
            stream_data['rtmp']
        ]
        proceso = subprocess.Popen(cmd)
        logging.info("🟢 FFmpeg iniciado - Preparando stream...")
        
        # Esperar activación
        for _ in range(10):
            estado = youtube.transicionar_estado(stream_data['broadcast_id'], 'testing')
            if estado:
                logging.info("🎬 Transmisión en vista previa")
                break
            time.sleep(5)
        else:
            raise Exception("No se pudo activar la vista previa")
        
        # Esperar hasta el inicio programado
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        if not youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            raise Exception("Error al iniciar transmisión LIVE")
        
        logging.info("🎥 Transmisión LIVE iniciada")
        
        # Reproducir música
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            musica = random.choice(stream_data['gestor'].medios['musica'])
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            
            try:
                with open(musica['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
            except Exception as e:
                logging.error(f"Error reproduciendo música: {str(e)}")
            
        proceso.terminate()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada correctamente")
    
    except Exception as e:
        logging.error(f"Error en transmisión: {str(e)}")
        if 'proceso' in locals():
            proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                    logging.error("No hay medios disponibles, reintentando...")
                    time.sleep(60)
                    gestor = GestorContenido()
                    continue
                
                imagen = random.choice(gestor.medios['imagenes'])
                logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")
                
                titulo = generar_titulo(imagen['name'])
                logging.info(f"📝 Título generado: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
                if not stream_info:
                    raise Exception("Error al crear transmisión")
                
                current_stream = {
                    "rtmp": stream_info['rtmp'],
                    "scheduled_start": stream_info['scheduled_start'],
                    "imagen": imagen,
                    "broadcast_id": stream_info['broadcast_id'],
                    "stream_id": stream_info['stream_id'],
                    "gestor": gestor
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, youtube),
                    daemon=True
                ).start()
                
                next_stream_time = stream_info['scheduled_start'] + timedelta(hours=8, minutes=5)
            
            if datetime.utcnow() >= next_stream_time:
                logging.info("🔄 Preparando nueva transmisión...")
                current_stream = None
                gestor = GestorContenido()  # Recargar medios
            
            time.sleep(15)
        
        except Exception as e:
            logging.error(f"🔥 Error crítico: {str(e)}")
            current_stream = None
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando transmisor de música continua...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
