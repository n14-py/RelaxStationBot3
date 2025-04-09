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
from urllib.parse import urlparse, parse_qs
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
    'imagen': {
        'naturaleza': ['bosque', 'lluvia', 'montaña', 'río', 'jungla'],
        'urbano': ['ciudad', 'loft', 'ático', 'moderno', 'urbano'],
        'abstracto': ['arte', 'geometría', 'color', 'diseño', 'creativo'],
        'vintage': ['retro', 'nostalgia', 'analógico', 'vintage']
    },
    'musica': {
        'lofi': ['lofi', 'chill', 'study', 'relax'],
        'jazz': ['jazz', 'blues', 'saxo', 'instrumental'],
        'naturaleza': ['lluvia', 'bosque', 'viento', 'oceano'],
        'clásica': ['piano', 'clásica', 'violín', 'orquesta']
    }
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            if "drive.google.com" in url:
                file_id = parse_qs(urlparse(url).query.get('id', [None])[0])
                if file_id:
                    url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = os.path.splitext(urlparse(url).path)[1].lower() if es_imagen else '.mp3'
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando {'imagen' if es_imagen else 'música'}: {url}")
            
            session = requests.Session()
            response = session.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(ruta_local, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return ruta_local
        except Exception as e:
            logging.error(f"Error descargando archivo: {str(e)}")
            return None

    def procesar_imagen(self, url):
        try:
            ruta_original = self.descargar_archivo(url, es_imagen=True)
            if not ruta_original:
                return None

            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_procesada = os.path.join(self.media_cache_dir, f"{nombre_hash}.jpg")

            subprocess.run([
                "ffmpeg", "-y", "-i", ruta_original,
                "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:-1:-1",
                "-q:v", "2", "-pix_fmt", "yuv420p", ruta_procesada
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            return ruta_procesada
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
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
                musica['local_path'] = self.descargar_archivo(musica['url'])
            
            # Filtrar elementos inválidos
            datos['imagenes'] = [i for i in datos['imagenes'] if i['local_path']]
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            if not datos['imagenes'] or not datos['musica']:
                raise ValueError("No hay contenido válido para transmitir")
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
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
        return build('youtube', 'v3', credentials=creds)
    
    def crear_transmision(self, titulo, imagen_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "Disfruta de nuestra selección musical continua con ambientación visual profesional.\n\n🔔 Activa las notificaciones\n👍 Déjanos tu like\n💬 Comenta tus sugerencias",
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
                    "snippet": {"title": "Stream Continuo"},
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
    
    def obtener_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            if response.get('items'):
                return response['items'][0]['status']['streamStatus']
            return None
        except Exception as e:
            logging.error(f"Error obteniendo estado del stream: {str(e)}")
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

def generar_titulo():
    EMOJIS = ['📚', '🌙', '🎧', '✨', '☕', '🌿', '🌀', '🕯️', '🌌', '🎹', '🛋️', '📖', '🌧️', '🍵']
    DESCRIPTORES = [
        "instrumental", "beats relajantes", "mix premium", "ritmos suaves",
        "melodías nocturnas", "sonidos cálidos", "vibraciones armoniosas"
    ]
    ACTIVIDADES = [
        "estudiá", "relajate", "concentrate", "meditá", "trabajá",
        "creá", "dormí", "soñá", "desconectá"
    ]
    MODIFICADORES = [
        "con estas melodías suaves", "con beats para el alma",
        "en tu refugio sonoro", "con nuestra mezcla única"
    ]
    HORARIOS = ["24/7", "sin interrupciones", "non-stop"]

    actividad = random.choice(ACTIVIDADES)
    descriptor = random.choice(DESCRIPTORES)

    componente = {
        'emoji': random.choice(EMOJIS),
        'descriptor': descriptor,
        'descriptor_cap': descriptor.capitalize(),
        'actividad': actividad,
        'actividad_cap': actividad.capitalize(),
        'modificador': random.choice(MODIFICADORES),
        'horario': random.choice(HORARIOS)
    }

    plantillas = [
        "{emoji} Lofi {descriptor} {horario} – {actividad_cap} {modificador}",
        "{emoji} Lofi para {actividad} – {descriptor} {horario}",
        "{emoji} {descriptor_cap} – {actividad_cap} {modificador}",
        "{emoji} Lofi {horario} – {descriptor} para {actividad}",
        "{emoji} {actividad_cap} con Lofi – {descriptor} {modificador}"
    ]

    return random.choice(plantillas).format(**componente)

def manejar_transmision(stream_data, youtube):
    proceso = None
    try:
        tiempo_inicio_ffmpeg = stream_data['scheduled_start'] - timedelta(minutes=1)
        espera_ffmpeg = (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds()
        
        if espera_ffmpeg > 0:
            logging.info(f"⏳ Esperando {espera_ffmpeg:.0f} segundos para iniciar FFmpeg...")
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
        logging.info("🟢 FFmpeg iniciado - Estableciendo conexión RTMP...")
        
        max_checks = 15
        stream_activo = False
        for _ in range(max_checks):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                logging.info("✅ Stream activo - Transicionando a testing")
                if youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
                    logging.info("🎬 Transmisión en VISTA PREVIA")
                    stream_activo = True
                break
            time.sleep(5)
        
        if not stream_activo:
            logging.error("❌ Stream no se activó a tiempo")
            proceso.kill()
            return
        
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        if youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            logging.info("🎥 Transmisión LIVE iniciada")
        else:
            raise Exception("No se pudo iniciar la transmisión")
        
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                proceso = subprocess.Popen(cmd)
            
            musica = random.choice(stream_data['gestor'].medios['musica'])
            logging.info(f"🎵 Reproduciendo: {musica['name']}")
            
            try:
                with open(musica['local_path'], 'rb') as audio_file:
                    with open(fifo_path, 'wb') as fifo:
                        fifo.write(audio_file.read())
                time.sleep(get_duration(musica['local_path']))
            except Exception as e:
                logging.error(f"Error enviando audio: {str(e)}")
                time.sleep(5)

    except Exception as e:
        logging.error(f"ERROR: {str(e)}")
    finally:
        if proceso:
            proceso.kill()
        try:
            youtube.finalizar_transmision(stream_data['broadcast_id'])
            logging.info("🛑 Transmisión finalizada y archivada correctamente")
        except Exception as e:
            logging.error(f"Error al finalizar: {str(e)}")

def get_duration(file_path):
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            file_path
        ], capture_output=True, text=True)
        return float(result.stdout.strip())
    except:
        return 300

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                imagen = random.choice(gestor.medios['imagenes'])
                titulo = generar_titulo()
                logging.info(f"📝 Título generado: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
                if not stream_info:
                    raise Exception("Error creación transmisión")
                
                current_stream = {
                    "rtmp": stream_info['rtmp'],
                    "scheduled_start": stream_info['scheduled_start'],
                    "imagen": imagen,
                    "gestor": gestor,
                    "broadcast_id": stream_info['broadcast_id'],
                    "stream_id": stream_info['stream_id'],
                    "end_time": stream_info['scheduled_start'] + timedelta(hours=8)
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, youtube),
                    daemon=True
                ).start()
                
                next_stream_time = current_stream['end_time'] + timedelta(minutes=5)
            
            else:
                if datetime.utcnow() >= next_stream_time:
                    current_stream = None
                    logging.info("🔄 Preparando nueva transmisión...")
                
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
