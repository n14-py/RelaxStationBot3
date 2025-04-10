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
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/relaxstationmedios/master/mediosmusic.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

PALABRAS_CLAVE = {
    'chill': ['chill', 'relax', 'calm'],
    'naturaleza': ['nature', 'bosque', 'playa'],
    'ciudad': ['city', 'urban', 'night'],
    'abstracto': ['abstract', 'art', 'digital']
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

            logging.info(f"⬇️ Descargando imagen: {url}")
            temp_path = os.path.join(self.media_cache_dir, f"temp_{nombre_hash}")
            
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-vf", "scale=1280:720",
                "-q:v", "2",
                ruta_local
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
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
            
            medios_validos = {"imagenes": [], "musica": []}
            for img in datos['imagenes']:
                if 'url' in img and 'name' in img:
                    path = self.procesar_imagen(img['url'])
                    if path:
                        medios_validos['imagenes'].append({
                            'name': img['name'],
                            'local_path': path
                        })
            
            for musica in datos['musica']:
                if 'url' in musica and 'name' in musica:
                    path = self.descargar_musica(musica['url'])
                    if path:
                        medios_validos['musica'].append({
                            'name': musica['name'],
                            'local_path': path
                        })
            
            logging.info("✅ Medios verificados y listos")
            return medios_validos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

class YouTubeManager:
    def __init__(self):
        self.youtube = self.autenticar()
    
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
            logging.info("🔑 Autenticación exitosa con YouTube")
            return build('youtube', 'v3', credentials=creds)
        except Exception as e:
            logging.error(f"🚨 Error de autenticación: {str(e)}")
            return None

    def verificar_estado_stream(self, stream_id):
        try:
            response = self.youtube.liveStreams().list(
                part="status",
                id=stream_id
            ).execute()
            
            if not response.get('items'):
                return None
                
            return response['items'][0]['status']['streamStatus']
        except Exception as e:
            logging.error(f"Error verificando estado del stream: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, imagen_path):
        if not self.youtube:
            return None
            
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=3)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": titulo,
                        "description": "🎵 Música Continua 24/7 • Ambiente Relajante\n🔔 Activa las notificaciones\n👍 Déjanos tu like",
                        "scheduledStartTime": scheduled_start.isoformat() + "Z"
                    },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "lifeCycleStatus": "created"
                    }
                }
            ).execute()

            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {"title": "Stream Automático"},
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

            try:
                subprocess.run([
                    "ffmpeg", "-y", "-i", imagen_path,
                    "-vframes", "1", "-q:v", "2",
                    "-vf", "scale=1280:720",
                    "/tmp/miniatura.jpg"
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body="/tmp/miniatura.jpg"
                ).execute()
                os.remove("/tmp/miniatura.jpg")
            except Exception as e:
                logging.error(f"⚠️ Error miniatura: {str(e)}")

            return {
                "rtmp": f"{stream['cdn']['ingestionInfo']['ingestionAddress']}/{stream['cdn']['ingestionInfo']['streamName']}",
                "broadcast_id": broadcast['id'],
                "stream_id": stream['id'],
                "start_time": scheduled_start
            }
        except Exception as e:
            logging.error(f"🔥 Error creando transmisión: {str(e)}")
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
            logging.error(f"🚫 Error transición a {estado}: {str(e)}")
            return False

def determinar_categoria(nombre_imagen):
    nombre = nombre_imagen.lower()
    for categoria, palabras in PALABRAS_CLAVE.items():
        if any(palabra in nombre for palabra in palabras):
            return categoria
    return random.choice(list(PALABRAS_CLAVE.keys()))

def seleccionar_musica_compatible(gestor, categoria):
    try:
        return random.choice([
            m for m in gestor.medios['musica'] 
            if m['local_path'] and any(p in m['name'].lower() for p in PALABRAS_CLAVE[categoria])
        ])
    except:
        return random.choice([m for m in gestor.medios['musica'] if m['local_path']])

def generar_titulo(nombre_imagen, categoria):
    temas = {
        'chill': ['Lounge Relax', 'Zona de Paz', 'Espacio Zen'],
        'naturaleza': ['Bosque Encantado', 'Playa Serena', 'Jardín Secreto'],
        'ciudad': ['Metrópolis Nocturna', 'Skyline Urbano', 'Horizonte Moderno'],
        'abstracto': ['Arte Digital', 'Geometría Sagrada', 'Universo Abstracto']
    }
    return f"{random.choice(temas[categoria])} • {nombre_imagen}"

def manejar_transmision(stream_data, youtube):
    proceso = None
    fifo_path = "/tmp/audio_fifo"
    
    try:
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        os.mkfifo(fifo_path)

        # Comando FFmpeg mejorado
        cmd = [
            "ffmpeg",
            "-loglevel", "verbose",
            "-re",
            "-loop", "1",
            "-i", stream_data['imagen']['local_path'],
            "-f", "mp3",
            "-i", fifo_path,
            "-vf", "format=yuv420p,scale=1280:720",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "stillimage",
            "-b:v", "4500k",
            "-maxrate", "4500k",
            "-bufsize", "9000k",
            "-g", "60",
            "-c:a", "aac",
            "-b:a", "192k",
            "-ar", "44100",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(cmd, stderr=subprocess.PIPE)
        logging.info("🟢 FFmpeg iniciado - Transmitiendo...")

        # Esperar activación del stream
        timeout_activacion = time.time() + 120  # 2 minutos máximo
        while time.time() < timeout_activacion:
            estado = youtube.verificar_estado_stream(stream_data['stream_id'])
            if estado == "active":
                logging.info("🎬 Stream activo en YouTube")
                break
            logging.warning(f"⏳ Esperando activación del stream... Estado actual: {estado}")
            time.sleep(10)
        else:
            raise Exception("Tiempo de espera agotado para activación del stream")

        # Transición a testing con verificación de estado
        for i in range(3):
            estado_actual = youtube.verificar_estado_stream(stream_data['stream_id'])
            if estado_actual != "active":
                logging.warning(f"Estado stream antes de testing: {estado_actual}")
                continue
                
            if youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
                logging.info("🎬 Transmisión en VISTA PREVIA")
                break
            logging.warning(f"⚠️ Reintentando testing ({i+1}/3)")
            time.sleep(10)
        else:
            raise Exception("No se pudo forzar vista previa")

        # Esperar tiempo programado con margen
        tiempo_restante = (stream_data['start_time'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante + 15)

        # Forzar transición a LIVE con verificaciones
        for i in range(5):
            estado_actual = youtube.verificar_estado_stream(stream_data['stream_id'])
            if estado_actual != "active":
                logging.warning(f"Estado stream antes de LIVE: {estado_actual}")
                time.sleep(5)
                continue
                
            if youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
                logging.info("🎥 Transmisión LIVE activada")
                break
            logging.warning(f"⚠️ Reintentando LIVE ({i+1}/5)")
            time.sleep(15)
        else:
            raise Exception("No se pudo forzar LIVE")

        # Reproducción continua mejorada
        inicio = time.time()
        while (time.time() - inicio) < 28800:  # 8 horas
            try:
                with open(stream_data['musica']['local_path'], 'rb') as f:
                    while True:
                        data = f.read(1024*1024)  # Leer en bloques de 1MB
                        if not data:
                            f.seek(0)  # Reiniciar al final del archivo
                            continue
                        with open(fifo_path, 'wb') as fifo:
                            fifo.write(data)
                            fifo.flush()
                        time.sleep(0.1)
            except Exception as e:
                logging.error(f"⚠️ Error reproducción: {str(e)}")
                time.sleep(1)

        logging.info("🕒 Transmisión completada")
        return True

    except Exception as e:
        logging.error(f"🔥 Error crítico: {str(e)}")
        return False
    finally:
        if proceso:
            proceso.terminate()
        if os.path.exists(fifo_path):
            os.remove(fifo_path)
        try:
            youtube.transicionar_estado(stream_data['broadcast_id'], 'complete')
        except Exception as e:
            logging.error(f"🚫 Error finalizando: {str(e)}")

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    
    if not youtube.youtube:
        logging.error("🚨 Conexión YouTube fallida")
        return

    current_stream = None
    
    while True:
        try:
            if not current_stream:
                imagen = random.choice(gestor.medios['imagenes'])
                logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")
                
                categoria = determinar_categoria(imagen['name'])
                logging.info(f"🏷️ Categoría: {categoria}")
                
                musica = seleccionar_musica_compatible(gestor, categoria)
                logging.info(f"🎵 Música seleccionada: {musica['name']}")
                
                titulo = generar_titulo(imagen['name'], categoria)
                logging.info(f"📢 Título: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
                if not stream_info:
                    raise Exception("Error creación transmisión")
                
                current_stream = {
                    **stream_info,
                    "imagen": imagen,
                    "musica": musica,
                    "end_time": stream_info['start_time'] + timedelta(hours=8)
                }

                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, youtube),
                    daemon=True
                ).start()

            else:
                if datetime.utcnow() >= current_stream['end_time'] + timedelta(minutes=5):
                    current_stream = None
                    logging.info("🔄 Reiniciando ciclo...")
                
                time.sleep(15)

        except Exception as e:
            logging.error(f"💥 Error en ciclo: {str(e)}")
            current_stream = None
            time.sleep(60)

@app.route('/health')
def health_check():
    return "OK", 200

if __name__ == "__main__":
    logging.info("🎧 Iniciando sistema de streaming...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
