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
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/relaxmusicmedios/master/mediosmusic.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

PALABRAS_CLAVE = {
    'chill': ['chill', 'relax', 'calm'],
    'jazz': ['jazz', 'blues', 'saxo'],
    'ambiente': ['ambiente', 'ambiental', 'meditación'],
    'clásica': ['clásica', 'piano', 'orquesta'],
    'naturaleza': ['naturaleza', 'bosque', 'lluvia']
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def obtener_extension_segura(self, url):
        try:
            parsed = urlparse(url)
            return os.path.splitext(parsed.path)[1].lower() or '.jpg'
        except:
            return '.jpg'

    def descargar_imagen(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            extension = self.obtener_extension_segura(url)
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}{extension}")
            
            if os.path.exists(ruta_local):
                return ruta_local

            logging.info(f"⬇️ Descargando imagen: {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(ruta_local, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando imagen: {str(e)}")
            return None

    def descargar_audio(self, url):
        try:
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.mp3")
            
            if os.path.exists(ruta_local):
                return ruta_local
                
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
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            if not all(key in datos for key in ["imagenes", "musica"]):
                raise ValueError("Estructura JSON inválida")
            
            # Descargar imágenes
            for imagen in datos['imagenes']:
                imagen['local_path'] = self.descargar_imagen(imagen['url'])
            
            # Descargar música
            for audio in datos['musica']:
                audio['local_path'] = self.descargar_audio(audio['url'])
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"imagenes": [], "musica": []}

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
    
    def generar_miniatura(self, image_path):
        try:
            output_path = "/tmp/miniatura_nueva.jpg"
            subprocess.run([
                "ffmpeg",
                "-y",
                "-i", image_path,
                "-vframes", "1",
                "-q:v", "2",
                "-vf", "scale=1280:720,setsar=1",
                output_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return output_path
        except Exception as e:
            logging.error(f"Error generando miniatura: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, image_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                  "title": titulo,
                  "description": "Sumérgete en un ambiente de relajación perfecto con nuestra selección de música ambiental y chill. Ideal para trabajar, estudiar, meditar o simplemente relajarte. Disfruta de una mezcla única de sonidos suaves y melodías armoniosas que te ayudarán a mejorar tu concentración y reducir el estrés.\n\n🎵 Lista de reproducción continua\n🌿 Mezcla de géneros relajantes\n🕒 24/7 Música para cada momento\n\n🔔 Síguenos para más actualizaciones\n#MusicaRelajante #ChillMusic #Estudiar #Trabajar #Relajación",
                  "scheduledStartTime": scheduled_start.isoformat() + "Z"
                     },
                    "status": {
                        "privacyStatus": "public",
                        "selfDeclaredMadeForKids": False,
                        "enableAutoStart": True,
                        "enableAutoStop": True,
                        "enableArchive": True,
                        "lifeCycleStatus": "created"
                    }
                }
            ).execute()
            
            stream = self.youtube.liveStreams().insert(
                part="snippet,cdn",
                body={
                    "snippet": {
                        "title": "Stream de música ambiental"
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
            
            rtmp_url = stream['cdn']['ingestionInfo']['ingestionAddress']
            stream_name = stream['cdn']['ingestionInfo']['streamName']
            
            thumbnail_path = self.generar_miniatura(image_path)
            if thumbnail_path and os.path.exists(thumbnail_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=thumbnail_path
                ).execute()
            
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

def determinar_categoria(nombre_imagen):
    nombre = nombre_imagen.lower()
    contador = {categoria: 0 for categoria in PALABRAS_CLAVE}
    
    for palabra in nombre.split():
        for categoria, palabras in PALABRAS_CLAVE.items():
            if palabra in palabras:
                contador[categoria] += 1
                
    max_categoria = max(contador, key=contador.get)
    return max_categoria if contador[max_categoria] > 0 else random.choice(list(PALABRAS_CLAVE.keys()))

def seleccionar_musica_compatible(gestor, categoria_imagen):
    musica_compatible = [
        track for track in gestor.medios['musica']
        if track['local_path'] and 
        any(palabra in track['name'].lower() 
        for palabra in PALABRAS_CLAVE[categoria_imagen])
    ]
    
    if not musica_compatible:
        musica_compatible = [t for t in gestor.medios['musica'] if t['local_path']]
    
    return random.choice(musica_compatible)

def generar_titulo(nombre_imagen, categoria):
    ambientes = {
        'chill': ['Chill Vibes', 'Relax Total', 'Mood Relax'],
        'jazz': ['Jazz Lounge', 'Smooth Jazz', 'Blues Night'],
        'ambiente': ['Ambiente Zen', 'Espacio Meditativo', 'Paz Interior'],
        'clásica': ['Clásicos Eternos', 'Música Clásica', 'Piano Clásico'],
        'naturaleza': ['Sonidos Naturales', 'Armonía Natural', 'Conexión Natural']
    }
    
    actividades = [
        ('Trabajar', '💼'), ('Estudiar', '📚'), ('Meditar', '🧘♂️'), 
        ('Dormir', '🌙'), ('Relajarse', '😌'), ('Yoga', '🧘♀️')
    ]
    
    beneficios = [
        'Mejorar la Concentración', 'Reducir el Estrés', 'Aumentar la Productividad',
        'Equilibrar la Mente', 'Estimular la Creatividad', 'Armonía Interior'
    ]

    ambiente = random.choice(ambientes[categoria])
    actividad, emoji_act = random.choice(actividades)
    beneficio = random.choice(beneficios)

    plantillas = [
        f"{ambiente} • Música {categoria.capitalize()} para {actividad} {emoji_act} | {beneficio}",
        f"Música {categoria.capitalize()} • {actividad} {emoji_act} • {beneficio}",
        f"{beneficio} con Música {categoria.capitalize()} • {ambiente} {emoji_act}",
        f"{actividad} {emoji_act} • {ambiente} • Música {categoria.capitalize()} para {beneficio}"
    ]
    
    return random.choice(plantillas)

def manejar_transmision(stream_data, youtube):
    try:
        tiempo_inicio_ffmpeg = stream_data['start_time'] - timedelta(minutes=1)
        espera_ffmpeg = (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds()
        
        if espera_ffmpeg > 0:
            logging.info(f"⏳ Esperando {espera_ffmpeg:.0f} segundos para iniciar FFmpeg...")
            time.sleep(espera_ffmpeg)
        
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-loop", "1",
            "-i", stream_data['imagen']['local_path'],
            "-stream_loop", "-1",
            "-i", stream_data['musica']['local_path'],
            "-c:v", "libx264",
            "-tune", "stillimage",
            "-preset", "ultrafast",
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:-1:-1,setsar=1",
            "-c:a", "aac",
            "-b:a", "192k",
            "-ar", "44100",
            "-shortest",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        proceso = subprocess.Popen(cmd)
        logging.info("🟢 FFmpeg iniciado - Estableciendo conexión RTMP...")
        
        max_checks = 10
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
        
        tiempo_restante = (stream_data['start_time'] - datetime.utcnow()).total_seconds()
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
            time.sleep(15)
        
        proceso.kill()
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada y archivada correctamente")

    except Exception as e:
        logging.error(f"Error en hilo de transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                imagen = random.choice(gestor.medios['imagenes'])
                logging.info(f"🖼️ Imagen seleccionada: {imagen['name']}")
                
                categoria = determinar_categoria(imagen['name'])
                logging.info(f"🏷️ Categoría detectada: {categoria}")
                
                musica = seleccionar_musica_compatible(gestor, categoria)
                logging.info(f"🎵 Música seleccionada: {musica['name']}")
                
                titulo = generar_titulo(imagen['name'], categoria)
                logging.info(f"📝 Título generado: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, imagen['local_path'])
                if not stream_info:
                    raise Exception("Error creación transmisión")
                
                current_stream = {
                    "rtmp": stream_info['rtmp'],
                    "start_time": stream_info['scheduled_start'],
                    "imagen": imagen,
                    "musica": musica,
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
    logging.info("🎶 Iniciando servicio de música ambiental...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
