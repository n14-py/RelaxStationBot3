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
MEDIOS_URL = "https://raw.githubusercontent.com/n14-py/RelaxStationBot/master/medios.json"
YOUTUBE_CREDS = {
    'client_id': os.getenv("YOUTUBE_CLIENT_ID"),
    'client_secret': os.getenv("YOUTUBE_CLIENT_SECRET"),
    'refresh_token': os.getenv("YOUTUBE_REFRESH_TOKEN")
}

PALABRAS_CLAVE = {
    'lluvia': ['lluvia', 'rain', 'storm'],
    'fuego': ['fuego', 'fire', 'chimenea'],
    'bosque': ['bosque', 'jungla', 'forest'],
    'rio': ['rio', 'river', 'cascada'],
    'noche': ['noche', 'night', 'luna']
}

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
    
    def obtener_extension_segura(self, url):
        try:
            parsed = urlparse(url)
            return os.path.splitext(parsed.path)[1].lower() or '.mp3'
        except:
            return '.mp3'

    def descargar_audio(self, url):
        try:
            if "drive.google.com" in url:
                file_id = url.split('id=')[-1].split('&')[0]
                url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
            
            nombre_hash = hashlib.md5(url.encode()).hexdigest()
            ruta_local = os.path.join(self.media_cache_dir, f"{nombre_hash}.wav")
            
            if os.path.exists(ruta_local):
                return ruta_local
                
            temp_path = os.path.join(self.media_cache_dir, f"temp_{nombre_hash}.mp3")
            
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            subprocess.run([
                "ffmpeg", "-y", "-i", temp_path,
                "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2",
                ruta_local
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            os.remove(temp_path)
            return ruta_local
        except Exception as e:
            logging.error(f"Error procesando audio: {str(e)}")
            return None

    def cargar_medios(self):
        try:
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            if not all(key in datos for key in ["videos", "musica", "sonidos_naturaleza"]):
                raise ValueError("Estructura JSON inválida")
            
            for medio in datos['sonidos_naturaleza']:
                medio['local_path'] = self.descargar_audio(medio['url'])
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"videos": [], "musica": [], "sonidos_naturaleza": []}

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
    
    def generar_miniatura(self, video_url):
        try:
            output_path = "/tmp/miniatura_nueva.jpg"
            subprocess.run([
                "ffmpeg",
                "-y", "-ss", "00:00:10",
                "-i", video_url,
                "-vframes", "1",
                "-q:v", "2",
                "-vf", "scale=1280:720,setsar=1",
                output_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return output_path
        except Exception as e:
            logging.error(f"Error generando miniatura: {str(e)}")
            return None
    
    def crear_transmision(self, titulo, video_url):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                  "title": titulo,
                  "description": "Déjate llevar por la serenidad de la naturaleza con nuestro video \"Relax Station\". Los relajantes sonidos de la lluvia te transportarán a un lugar de paz y tranquilidad, ideal para dormir, meditar o concentrarte. Perfecto para desconectar y encontrar tu equilibrio interior. ¡Relájate y disfruta!                                                                                                   IGNORAR TAGS                                                   relax, relajación, lluvia, sonidos de lluvia, calma, dormir, meditar, concentración, sonidos de la naturaleza, ambiente relajante, tranquilidad, lluvia para dormir, lluvia relajante, lluvia y calma, sonidos para relajación, ASMR, sonidos ASMR, lluvia nocturna, estudio, sonidos relajantes, ruido blanco, concentración mental, paz interior, alivio del estrés, lluvia natural, lluvia suave, descanso, ambiente de lluvia, dormir rápido, lluvia profunda, día lluvioso, lluvia para meditar, bienestar, paz, naturaleza, mindfulness, relajación profunda, yoga, pilates, meditación guiada, ondas cerebrales, sonidos curativos, música para estudiar, música para concentración, descanso mental, serenidad, zen, armonía, equilibrio, espiritualidad, relajación total, energía positiva, lluvia tibia, tormenta suave, lluvia con truenos, descanso absoluto, terapia de sonido, bienestar emocional, salud mental, terapia de relajación, descanso nocturno, paz mental, sonidos de la selva, sonidos de bosque, mindfulness y relajación, mejor sueño, descanso profundo, liberación de estrés, antiestrés, antiansiedad, dormir mejor, sueño reparador, relajación sensorial, relajación auditiva, calma mental, música relajante, relajación para ansiedad, terapia de paz, sonido blanco para dormir, relax absoluto, serenidad de la naturaleza, sonidos calmantes, música tranquila para dormir, estado zen, enfoque mental, concentración absoluta, claridad mental, noche lluviosa, sonido de la lluvia, sonido de lluvia para dormir, tranquilidad nocturna, música chill, descanso consciente, relajación instantánea, serenidad para el alma, limpieza mental, vibraciones relajantes, energía relajante, conexión con la naturaleza, descanso espiritual, introspección, desconexión del estrés, flujo de energía positiva, alivio de tensiones, sonidos puros, alivio de fatiga, contemplación, vibraciones positivas, terapia sonora, sonidos calmantes para niños, calma en la tormenta, dormir sin interrupciones, música de fondo tranquila, ambiente natural, relax, relaxation, rain, rain sounds, calm, sleep, meditate, focus, nature sounds, relaxing ambiance, tranquility, rain for sleep, relaxing rain, rain and calm, sounds for relaxation, ASMR, ASMR sounds, nighttime rain, study, relaxing sounds, white noise, mental focus, inner peace, stress relief, natural rain, soft rain, rest, rain ambiance, deep rain, rainy day, rain for meditation, wellness, peace, stress, nature, mindfulness, deep relaxation, yoga, pilates, guided meditation, brain waves, healing sounds, music for studying, music for concentration, mental rest, serenity, zen, harmony, balance, spirituality, total relaxation, positive energy, warm rain, gentle storm, rain with thunder, absolute rest, sound therapy, emotional well-being, mental health, relaxation therapy, nighttime rest, jungle sounds, forest sounds, baby sounds, pet sounds, mindfulness and relaxation, relaxation before sleep, better sleep, deep rest, stress relief, anti-stress, anti-anxiety, sleep better, restorative sleep, sensory relaxation, mental calm, relaxing music, background relaxing rain, relaxing background music, natural sounds, mental harmonization, relaxing noise, natural relaxing sounds, deep relaxation music, relaxed mind, relaxation for anxiety, peace therapy, absolute rest, sound well-being, relaxed concentration, mental balance, white noise for sleeping, absolute relax, calm mind, total serenity, secured rest, rain audio, rain sounds with music, rainy night, nature serenity, calming sounds, quiet music for sleeping, zen state, energetic balance, meditation and focus, mental sharpness, absolute concentration, improved concentration, mental clarity, music and rain, harmony and balance, sound of rain, nighttime tranquility, chill music, mindful rest, instant relaxation, soul serenity, mental cleansing, soft music, relaxing energy, connection with nature, relaxation frequency, brain rest, sound peace, introspection, stress disconnection, positive energy flow, tension relief, mental detox, pure sounds, fatigue relief, full serenity, contemplation, positive vibes, sound therapy, calming sounds for kids, uninterrupted sleep, quiet background music, natural ambiance..",
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
                        "title": "Stream de ingesta principal"
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
            
            thumbnail_path = self.generar_miniatura(video_url)
            if thumbnail_path and os.path.exists(thumbnail_path):
                self.youtube.thumbnails().set(
                    videoId=broadcast['id'],
                    media_body=thumbnail_path
                ).execute()
                os.remove(thumbnail_path)
            
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

def determinar_categoria(nombre_video):
    nombre = nombre_video.lower()
    contador = {categoria: 0 for categoria in PALABRAS_CLAVE}
    
    for palabra in nombre.split():
        for categoria, palabras in PALABRAS_CLAVE.items():
            if palabra in palabras:
                contador[categoria] += 1
                
    max_categoria = max(contador, key=contador.get)
    return max_categoria if contador[max_categoria] > 0 else random.choice(list(PALABRAS_CLAVE.keys()))

def seleccionar_audio_compatible(gestor, categoria_video):
    audios_compatibles = [
        audio for audio in gestor.medios['sonidos_naturaleza']
        if audio['local_path'] and 
        any(palabra in audio['name'].lower() 
        for palabra in PALABRAS_CLAVE[categoria_video])
    ]
    
    if not audios_compatibles:
        audios_compatibles = [a for a in gestor.medios['sonidos_naturaleza'] if a['local_path']]
    
    return random.choice(audios_compatibles)

def generar_titulo(nombre_video, categoria):
    ubicaciones = {
        'departamento': ['Departamento Acogedor', 'Loft Moderno', 'Ático con Vista', 'Estudio Minimalista'],
        'cabaña': ['Cabaña en el Bosque', 'Refugio Montañoso', 'Chalet de Madera', 'Cabaña junto al Lago'],
        'cueva': ['Cueva Acogedor', 'Gruta Acogedora', 'Cueva con Chimenea', 'Casa Cueva Moderna'],
        'selva': ['Cabaña en la Selva', 'Refugio Tropical', 'Habitación en la Jungla', 'Casa del Árbol'],
        'default': ['Entorno Relajante', 'Espacio Zen', 'Lugar de Paz', 'Refugio Natural']
    }
    
    ubicacion_keys = {
        'departamento': ['departamento', 'loft', 'ático', 'estudio', 'apartamento'],
        'cabaña': ['cabaña', 'chalet', 'madera', 'bosque', 'lago'],
        'cueva': ['cueva', 'gruta', 'caverna', 'roca'],
        'selva': ['selva', 'jungla', 'tropical', 'palmeras']
    }
    
    actividades = [
        ('Dormir', '🌙'), ('Estudiar', '📚'), ('Meditar', '🧘♂️'), 
        ('Trabajar', '💻'), ('Desestresarse', '😌'), ('Concentrarse', '🎯')
    ]
    
    beneficios = [
        'Aliviar el Insomnio', 'Reducir la Ansiedad', 'Mejorar la Concentración',
        'Relajación Profunda', 'Conexión con la Naturaleza', 'Sueño Reparador',
        'Calma Interior'
    ]

    ubicacion_tipo = 'default'
    nombre = nombre_video.lower()
    for key, words in ubicacion_keys.items():
        if any(palabra in nombre for palabra in words):
            ubicacion_tipo = key
            break
            
    ubicacion = random.choice(ubicaciones.get(ubicacion_tipo, ubicaciones['default']))
    actividad, emoji_act = random.choice(actividades)
    beneficio = random.choice(beneficios)
    
    plantillas = [
        f"{ubicacion} • Sonidos de {categoria.capitalize()} para {actividad} {emoji_act} | {beneficio}",
        f"{actividad} {emoji_act} con Sonidos de {categoria.capitalize()} en {ubicacion} | {beneficio}",
        f"{beneficio} • {ubicacion} con Ambiente de {categoria.capitalize()} {emoji_act}",
        f"Relájate en {ubicacion} • {categoria.capitalize()} para {actividad} {emoji_act} | {beneficio}"
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
            "-rtbufsize", "100M",
            "-re",
            "-stream_loop", "-1",
            "-i", stream_data['video']['url'],
            "-stream_loop", "-1",
            "-i", stream_data['audio']['local_path'],
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:-1:-1,setsar=1",
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-x264-params", "keyint=48:min-keyint=48",
            "-b:v", "3000k",
            "-maxrate", "3000k",
            "-bufsize", "6000k",
            "-r", "24",
            "-g", "48",
            "-threads", "1",
            "-flush_packets", "1",
            "-c:a", "aac",
            "-b:a", "96k",
            "-ar", "44100",
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
                video = random.choice(gestor.medios['videos'])
                logging.info(f"🎥 Video seleccionado: {video['name']}")
                
                categoria = determinar_categoria(video['name'])
                logging.info(f"🏷️ Categoría detectada: {categoria}")
                
                audio = seleccionar_audio_compatible(gestor, categoria)
                logging.info(f"🔊 Audio seleccionado: {audio['name']}")
                
                titulo = generar_titulo(video['name'], categoria)
                logging.info(f"📝 Título generado: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, video['url'])
                if not stream_info:
                    raise Exception("Error creación transmisión")
                
                current_stream = {
                    "rtmp": stream_info['rtmp'],
                    "start_time": stream_info['scheduled_start'],
                    "video": video,
                    "audio": audio,
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
    logging.info("🎬 Iniciando servicio de streaming...")
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    serve(app, host='0.0.0.0', port=10000)
