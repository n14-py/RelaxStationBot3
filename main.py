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
 }   }

PLANTILLAS_TITULOS = [
    "📚 {musica} | Música suave para {imagen}",
    "🌧️ {musica} + {imagen} – Relajate, estudia o soñá",
    "✨ {musica} – Para tu mente, alma y creatividad",
    "☕ Noche {imagen} con {musica} | Transmisión 24/7",
    "📼 Retro {musica} | Desde RelaxStation",
    "🧘 Música {musica} para {imagen}",
    "📀 {musica} para enfocar, relajar o disfrutar",
    "🌌 {musica} para soñar despierto – {imagen}",
    "🔥 {musica} para noches {imagen}"
]

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def descargar_archivo(self, url, es_imagen=False):
        try:
            if "drive.google.com" in url:
                file_id = parse_qs(urlparse(url).query).get('id', [None])[0]
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
    
    def verificar_stream_activo(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            return response['items'][0]['status']['streamStatus'] == 'active'
        except:
            return False
    
    def control_estado(self, broadcast_id, estado):
        try:
            self.youtube.liveBroadcasts().transition(
                broadcastStatus=estado,
                id=broadcast_id,
                part="id,status"
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Error cambiando a estado {estado}: {str(e)}")
            return False

def generar_titulo():
    # Listas de componentes para generar títulos
    EMOJIS = ['📚', '🌙', '🎧', '✨', '☕', '🌿', '🌀', '🕯️', '🌌', '🎹', '🛋️', '📖', '🌧️', '🍵']
    DESCRIPTORES = [
        "instrumental", "beats relajantes", "mix premium", "ritmos suaves",
        "melodías nocturnas", "sonidos cálidos", "vibraciones armoniosas",
        "compilación especial", "selección exclusiva", "flow continuo"
    ]
    ACTIVIDADES = [
        "estudiá", "relajate", "concentrate", "meditá", "trabajá",
        "creá", "dormí", "soñá", "desconectá", "fluí"
    ]
    MODIFICADORES = [
        "con estas melodías suaves", "con beats para el alma",
        "en tu refugio sonoro", "con nuestra mezcla única",
        "en tu zona de paz", "con energía renovadora"
    ]
    HORARIOS = ["24/7", "sin interrupciones", "non-stop", "en loop infinito"]

    # Seleccionar componentes aleatorios
    componente = {
        'emoji': random.choice(EMOJIS),
        'descriptor': random.choice(DESCRIPTORES),
        'actividad': random.choice(ACTIVIDADES),
        'modificador': random.choice(MODIFICADORES),
        'horario': random.choice(HORARIOS)
    }

    # Plantillas de títulos
    plantillas = [
        "{emoji} Lofi {descriptor} {horario} – {actividad.capitalize()} {modificador}",
        "{emoji} Lofi para {actividad} – {descriptor} {horario}",
        "{emoji} {descriptor.capitalize()} – {actividad.capitalize()} {modificador}",
        "{emoji} Lofi {horario} – {descriptor} para {actividad}",
        "{emoji} {actividad.capitalize()} con Lofi – {descriptor} {modificador}"
    ]

    # Generar y retornar título
    return random.choice(plantillas).format(**componente)

def manejar_transmision(stream_data, youtube):
    proceso = None
    try:
        # Esperar inicio programado
        tiempo_espera = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_espera > 0:
            logging.info(f"⏳ Esperando {tiempo_espera:.0f}s para preparar transmisión...")
            time.sleep(tiempo_espera)
        
        # Configurar FIFO
        fifo_path = os.path.join(stream_data['gestor'].media_cache_dir, "audio_fifo")
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg optimizado
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

        # Esperar hasta que el stream esté activo
        stream_activo = False
        for _ in range(10):
            estado = youtube.verificar_stream_activo(stream_data['stream_id'])
            if estado == 'active':
                stream_activo = True
                break
            time.sleep(5)
        
        if not stream_activo:
            raise Exception("❌ Stream no se activó a tiempo")

        # Transición a testing
        if youtube.control_estado(stream_data['broadcast_id'], 'testing'):
            logging.info("🎬 Transmisión en VISTA PREVIA")
        
        # Esperar hasta el inicio programado
        tiempo_restante = (stream_data['scheduled_start'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        # Transición a live
        if youtube.control_estado(stream_data['broadcast_id'], 'live'):
            logging.info("🎥 Transmisión LIVE iniciada")

        # Mantener transmisión por 8 horas
        tiempo_inicio = datetime.utcnow()
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.kill()
                proceso = subprocess.Popen(cmd)
            
            # Reproducir música
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
            youtube.control_estado(stream_data['broadcast_id'], 'complete')
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
        return 300  # Duración por defecto 3 minutos

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    while True:
        try:
            if not gestor.medios['imagenes'] or not gestor.medios['musica']:
                logging.error("No hay medios válidos, reintentando...")
                time.sleep(60)
                gestor = GestorContenido()
                continue
            
            imagen = random.choice(gestor.medios['imagenes'])
            musica = random.choice(gestor.medios['musica'])
            
            titulo = generar_titulo()
            logging.info(f"📝 Título generado: {titulo}")
            
            stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
            if not stream_info:
                raise Exception("Error al crear transmisión")
            
            stream_data = {
                **stream_info,
                "imagen": imagen,
                "gestor": gestor
            }

            hilo = threading.Thread(
                target=manejar_transmision,
                args=(stream_data, youtube),
                daemon=True
            )
            hilo.start()
            
            # Esperar 8 horas y 5 minutos para nueva transmisión
            time.sleep(28800 + 300)

        except Exception as e:
            logging.error(f"ERROR GRAVE: {str(e)}")
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando transmisor de música continua...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
