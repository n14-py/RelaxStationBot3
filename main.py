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
            
            if not all(key in datos for key in ["videos", "musica"]):
                raise ValueError("Estructura JSON inválida")
            
            # Descargar videos
            for video in datos['videos']:
                video['local_path'] = self.descargar_video(video['url'])
            
            # Descargar música
            for cancion in datos['musica']:
                cancion['local_path'] = self.descargar_audio(cancion['url'])
            
            logging.info("✅ Medios verificados y listos")
            return datos
        except Exception as e:
            logging.error(f"Error cargando medios: {str(e)}")
            return {"videos": [], "musica": []}

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
    
    def crear_transmision(self, titulo, video_path):
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                  "title": titulo,
                  "description": "Disfruta de nuestra selección musical las 24 horas del día. Música relajante, instrumental y ambiental para trabajar, estudiar, meditar o simplemente disfrutar. 🎵🎶\n\n📲 Síguenos: \n\nhttp://instagram.com/@desderelaxstation \n\nFacebook: https://www.facebook.com/people/Desde-Relax-Station/61574709615178/ \n\nTikTok: https://www.tiktok.com/@desderelaxstation\n\n🚫IGNORAR TAGS DesdeRelaxStation, música relajante, música instrumental, música para trabajar, música para estudiar, música para dormir, música ambiental, música chill, música suave, música tranquila, música de fondo, música para concentrarse, música para meditar, música para yoga, música clásica, música de piano, música de guitarra, música sin copyright, música libre de derechos, música para streaming, música para videos, música positiva, música zen, música para aliviar el estrés, música antiestrés, música para ansiedad, música calmante, música para relajarse, música para leer, música para creatividad, música para productividad, música para concentración, música para oficina, música para café, música para lluvia, música para noche, música para día, música para mañana, música para tarde, música para atardecer, música para amanecer, música para estudio, música para escritura, música para pintar, música para dibujar, música para diseñar, música para programar, música para trabajar remoto, música para home office, música para teletrabajo, música para mindfulness, música para bienestar, música para salud mental, música para terapia, música para masajes, música para spa, música para descansar, música para soñar, música para viajar, música para volar, música para pensar, música para reflexionar, música para inspirarse, música para motivación, música para energía positiva, música para armonía, música para equilibrio, música para paz interior, música para alma, música para corazón, música para espíritu, música para vibraciones positivas, música para frecuencia, música para ondas cerebrales, música para alpha, música para theta, música para delta, música para gamma, música para beta, música para meditación profunda, música para sanación, música para chakras, música para reiki, música para energía, música para vibración, música para frecuencia 432hz, música para frecuencia 528hz, música para solfeggio, música para cuencos tibetanos, música para cuencos de cristal, música para naturaleza, música para bosque, música para montaña, música para playa, música para océano, música para río, música para lago, música para selva, música para desierto, música para espacio, música para estrellas, música para luna, música para sol, música para planetas, música para universo, música para cosmos, música para galaxias, música para nebulosas, música para aurora boreal, música para atardeceres, música para amaneceres, música para estaciones, música para primavera, música para verano, música para otoño, música para invierno, música para días lluviosos, música para días soleados, música para días nublados, música para días ventosos, música para días nevados, música para días fríos, música para días cálidos, música para días templados, música para todas las ocasiones, música para todos los estados de ánimo, música para todos los momentos, relax music, instrumental music, study music, work music, sleep music, meditation music, yoga music, background music, focus music, concentration music, chill music, soft music, calm music, peaceful music, ambient music, atmospheric music, classical music, piano music, guitar music, no copyright music, royalty free music, streaming music, video music, positive music, zen music, stress relief music, anti-stress music, anxiety relief music, calming music, relaxation music, reading music, creativity music, productivity music, office music, coffee music, rain music, night music, day music, morning music, afternoon music, sunset music, sunrise music, study music, writing music, painting music, drawing music, design music, programming music, remote work music, home office music, telework music, mindfulness music, wellness music, mental health music, therapy music, massage music, spa music, rest music, dream music, travel music, fly music, think music, reflection music, inspiration music, motivation music, positive energy music, harmony music, balance music, inner peace music, soul music, heart music, spirit music, positive vibrations music, frequency music, brain waves music, alpha music, theta music, delta music, gamma music, beta music, deep meditation music, healing music, chakras music, reiki music, energy music, vibration music, 432hz music, 528hz music, solfeggio music, tibetan bowls music, crystal bowls music, nature music, forest music, mountain music, beach music, ocean music, river music, lake music, jungle music, desert music, space music, stars music, moon music, sun music, planets music, universe music, cosmos music, galaxies music, nebulas music, aurora music, sunsets music, sunrises music, seasons music, spring music, summer music, autumn music, winter music, rainy days music, sunny days music, cloudy days music, windy days music, snowy days music, cold days music, warm days music, mild days music, music for all occasions, music for all moods, music for all moments.",
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
            
            thumbnail_path = self.generar_miniatura(video_path)
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

def determinar_categoria(nombre_musica):
    nombre = nombre_musica.lower()
    contador = {categoria: 0 for categoria in PALABRAS_CLAVE}
    
    for palabra in nombre.split():
        for categoria, palabras in PALABRAS_CLAVE.items():
            if palabra in palabras:
                contador[categoria] += 1
                
    max_categoria = max(contador, key=contador.get)
    return max_categoria if contador[max_categoria] > 0 else random.choice(list(PALABRAS_CLAVE.keys()))

def seleccionar_musica_aleatoria(gestor):
    canciones_disponibles = [m for m in gestor.medios['musica'] if m['local_path']]
    if not canciones_disponibles:
        raise Exception("No hay música disponible para transmitir")
    return random.choice(canciones_disponibles)

def generar_titulo_musica(nombre_musica, categoria):
    actividades = [
        ('Relajarse', '😌'), ('Estudiar', '📚'), ('Trabajar', '💻'), 
        ('Meditar', '🧘♂️'), ('Dormir', '🌙'), ('Concentrarse', '🎯'),
        ('Leer', '📖'), ('Crear', '🎨'), ('Programar', '💻')
    ]
    
    beneficios = [
        'Mejorar la Concentración', 'Reducir el Estrés', 'Aumentar la Productividad',
        'Favorecer la Relajación', 'Inducir al Sueño', 'Estimular la Creatividad',
        'Armonizar el Ambiente', 'Equilibrar las Emociones'
    ]

    actividad, emoji_act = random.choice(actividades)
    beneficio = random.choice(beneficios)
    
    plantillas = [
        f"Música de {categoria.capitalize()} • {nombre_musica} | Perfecta para {actividad} {emoji_act} | {beneficio}",
        f"{nombre_musica} • {categoria.capitalize()} para {actividad} {emoji_act} | {beneficio}",
        f"{beneficio} • {nombre_musica} | Música {categoria.capitalize()} {emoji_act}",
        f"Relájate con {nombre_musica} • {categoria.capitalize()} para {actividad} {emoji_act} | {beneficio}"
    ]
    
    return random.choice(plantillas)

def crear_lista_reproduccion(gestor, duracion_horas=8):
    """Crea una lista de reproducción aleatoria que durará aproximadamente duracion_horas"""
    canciones = [m for m in gestor.medios['musica'] if m['local_path']]
    if not canciones:
        raise Exception("No hay canciones disponibles")
    
    lista_reproduccion = []
    tiempo_total = timedelta()
    
    # Estimación promedio de duración de canción (4 minutos)
    duracion_estimada = timedelta(hours=duracion_horas)
    canciones_necesarias = int((duracion_estimada.total_seconds() / 60) / 4
    
    # Si no tenemos suficientes canciones, repetiremos algunas
    while len(lista_reproduccion) < canciones_necesarias:
        cancion = random.choice(canciones)
        lista_reproduccion.append(cancion)
    
    return lista_reproduccion

def manejar_transmision(stream_data, youtube):
    try:
        tiempo_inicio_ffmpeg = stream_data['start_time'] - timedelta(minutes=1)
        espera_ffmpeg = (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds()
        
        if espera_ffmpeg > 0:
            logging.info(f"⏳ Esperando {espera_ffmpeg:.0f} segundos para iniciar FFmpeg...")
            time.sleep(espera_ffmpeg)
        
        # Crear archivo de lista de reproducción para FFmpeg
        lista_archivo = os.path.join(stream_data['video']['local_path'] + ".txt")
        with open(lista_archivo, 'w') as f:
            for cancion in stream_data['playlist']:
                f.write(f"file '{cancion['local_path']}'\n")
        
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-rtbufsize", "100M",
            "-re",
            "-f", "concat",
            "-safe", "0",
            "-i", lista_archivo,
            "-i", stream_data['video']['local_path'],
            "-map", "0:a:0",
            "-map", "1:v:0",
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
            os.remove(lista_archivo)
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
        os.remove(lista_archivo)
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada y archivada correctamente")

    except Exception as e:
        logging.error(f"Error en hilo de transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        if 'lista_archivo' in locals() and os.path.exists(lista_archivo):
            os.remove(lista_archivo)

def ciclo_transmision():
    gestor = GestorContenido()
    youtube = YouTubeManager()
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                video = random.choice(gestor.medios['videos'])
                logging.info(f"🎥 Video seleccionado: {video['name']}")
                
                playlist = crear_lista_reproduccion(gestor)
                primera_cancion = playlist[0]
                categoria = determinar_categoria(primera_cancion['name'])
                logging.info(f"🎵 Playlist creada con {len(playlist)} canciones")
                
                titulo = generar_titulo_musica(primera_cancion['name'], categoria)
                logging.info(f"📝 Título generado: {titulo}")
                
                stream_info = youtube.crear_transmision(titulo, video['local_path'])
                if not stream_info:
                    raise Exception("Error creación transmisión")
                
                current_stream = {
                    "rtmp": stream_info['rtmp'],
                    "start_time": stream_info['scheduled_start'],
                    "video": video,
                    "playlist": playlist,
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
