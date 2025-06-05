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
import json

app = Flask(__name__)

# Configuración logging mejorada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('streaming.log')
    ]
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

# Tiempo de transmisión en horas (12 horas)
STREAM_DURATION_HOURS = 12

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
        self.videos_usados = []
        self.canciones_usadas = []
    
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
            logging.info("📡 Obteniendo lista de medios desde GitHub...")
            respuesta = requests.get(MEDIOS_URL, timeout=20)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            if not all(key in datos for key in ["videos", "musica"]):
                raise ValueError("Estructura JSON inválida")
            
            # Descargar videos primero
            logging.info("🎥 Iniciando descarga de videos...")
            for i, video in enumerate(datos['videos'], 1):
                logging.info(f"⬇️ Descargando video {i}/{len(datos['videos'])}: {video['name']}")
                video['local_path'] = self.descargar_video(video['url'])
                if not video['local_path']:
                    raise Exception(f"Fallo al descargar video: {video['name']}")
            
            # Luego descargar música
            logging.info("🎵 Iniciando descarga de música...")
            for j, cancion in enumerate(datos['musica'], 1):
                logging.info(f"⬇️ Descargando canción {j}/{len(datos['musica'])}: {cancion['name']}")
                cancion['local_path'] = self.descargar_audio(cancion['url'])
                if not cancion['local_path']:
                    raise Exception(f"Fallo al descargar canción: {cancion['name']}")
            
            logging.info("✅ Todos los medios descargados y verificados")
            return datos
        except Exception as e:
            logging.error(f"❌ Error cargando medios: {str(e)}")
            return {"videos": [], "musica": []}

    def seleccionar_video_no_usado(self):
        videos_disponibles = [
            v for v in self.medios['videos'] 
            if v['local_path'] and v['name'] not in self.videos_usados
        ]
        
        if not videos_disponibles:
            # Si todos los videos han sido usados, reiniciamos la lista
            self.videos_usados = []
            videos_disponibles = [v for v in self.medios['videos'] if v['local_path']]
        
        if not videos_disponibles:
            raise Exception("No hay videos disponibles para transmitir")
        
        video_seleccionado = random.choice(videos_disponibles)
        self.videos_usados.append(video_seleccionado['name'])
        return video_seleccionado

class YouTubeManager:
    def __init__(self):
        self.youtube = None
        self.autenticar()
    
    def autenticar(self):
        max_intentos = 3
        for intento in range(max_intentos):
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
                logging.info("🔑 Autenticación con YouTube exitosa")
                return
            except Exception as e:
                logging.error(f"🔴 Error autenticación YouTube (intento {intento+1}/{max_intentos}): {str(e)}")
                if intento == max_intentos - 1:
                    logging.error("❌ No se pudo autenticar con YouTube después de varios intentos")
                    self.youtube = None
                time.sleep(5)
    
    def generar_miniatura(self, video_path):
        try:
            output_path = "/tmp/miniatura_nueva.jpg"
            subprocess.run([
                "ffmpeg",
                "-y", "-ss", "00:00:10",
                "-i", video_path,
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
        if not self.youtube:
            logging.error("No hay conexión con YouTube")
            return None
            
        try:
            scheduled_start = datetime.utcnow() + timedelta(minutes=5)
            
            broadcast = self.youtube.liveBroadcasts().insert(
                part="snippet,status",
                body={
                  "snippet": {
                    "title": titulo,
                    "description": "Siguenos \n\n . 🎵🎶\n\n📲 Spotify: \n\n https://open.spotify.com/intl-es/artist/7J4Rf0Q97OcDjg3kmBXSRj Instagram: \n\nhttp://instagram.com/@desderelaxstation \n\nFacebook: https://www.facebook.com/people/Desde-Relax-Station/61574709615178/ \n\nTikTok: https://www.tiktok.com/@desderelaxstation   \n \n🚫IGNORAR TAGS DesdeRelaxStation,música relajante, música para estudiar, música para dormir, música instrumental, música chill, música lofi, lofi chill, chill out, música para meditar, sonidos de naturaleza, música para concentración, música suave, música tranquila, música relajación, ambient music, música de fondo, música para trabajar, focus music, concentración, estudio, relajación profunda, calma, tranquilidad, música para yoga, meditación guiada, mindfulness, desde relax station, lo-fi beats, música para leer, música para spa, música zen, dormir mejor, descanso, paz interior, música para bebés, música piano, guitarra instrumental, jazz suave, bossanova, música acústica, ambiente relajante, sonidos mar, lluvia relajante, ruido blanco, sonidos de bosque, naturaleza, música chillhop, música electrónica suave, música ambiental, down tempo, soft beats, desde relax station, música indie chill, ethereal sounds, smooth jazz, relaxing vibes, calming music, peaceful sounds, sound healing, bienestar, bienestar emocional, sonidos binaurales, ondas alfa, ondas theta, spa music, masaje relajante, energía positiva, detox mental, balance interior, serenidad, creatividad, concentración plena, productividad, música para oficina, música coworking, relajación cuerpo mente, tranquilidad mental, descanso profundo, sueño reparador, lofi español, desde relax station, música para el alma, paz mental, inspiración, música motivacional suave, trabajar en casa, home office, ambiente chill, café música, coffee shop music, chill café, chill estudio, estudiar tranquilo, estudiar relajado, concentración máxima, música para pintar, música para escribir, arte y música, creatividad fluida, noche tranquila, tarde de estudio, sonidos de lluvia, sonidos de olas, sonidos de viento, naturaleza instrumental, desde relax station, chill vibes, good vibes, positive energy, relax music, focus beats, focus lofi, dreamy lofi, night lofi, chill piano, chill guitar, chill sax, soft melodies, cozy music, calm music, background music, sleep sounds, deep sleep, restful sleep, sleep music, study music, work music, meditation music, yoga music, mindfulness music, focus music, chillhop, lo-fi hip hop, lo-fi chill, chill beats, ambient beats, smooth beats, dreamy beats, relaxing beats, mellow beats, peaceful music, peaceful vibes, healing music, from relax station, calm vibes, chill moments, peaceful moments, soft piano, soft guitar, soft jazz, acoustic chill, acoustic instrumental, ocean sounds, rain sounds, forest sounds, nature sounds, soundscapes, calm melodies, peaceful melodies, healing melodies, binaural beats, alpha waves, theta waves, deep focus, mental clarity, emotional balance, stress relief, relaxation music, spa sounds, massage music, positive vibes, mental detox, inner peace, body mind relaxation, mental calm, deep rest, restorative sleep, spanish lofi, relaxing spanish music, smooth beats spanish, studying beats, sleeping beats, relaxing loops, chill loops, chillout music, cozy vibes, home vibes, cozy atmosphere, inspiration music, motivational chill, work from home music, home office vibes, chill environment, cafe vibes, coffee shop vibes, relaxed studying, relaxed work, maximum concentration, latin chill, latin lofi, instrumental beats, painting music, writing music, art music, creative flow, quiet night, study evening, rain ambience, ocean ambience, wind ambience, instrumental nature, desde relax station, peaceful night, tranquil evening, cozy night, chill evening, sunset music, sunrise music, lofi sunset, lofi sunrise, morning chill, evening chill, cozy morning, cozy evening, mindfulness meditation, relaxation session, gentle music, calming beats, smooth sounds, cozy sounds, quiet sounds, healing sounds, from relax station, sleep aid, insomnia relief, bedtime music, nap music, chill dreams, peaceful dreams, cozy dreams, dreamy sleep, serene sounds, gentle sounds, soft sounds, subtle beats, atmospheric music, dreamy atmosphere, relaxing atmosphere, lofi atmosphere, calm environment, peaceful environment, chill environment, relaxing environment, music therapy, sound therapy, stress-free music, unplug music, digital detox music, serenity, calmness, peace, tranquility, mindfulness, meditative state, productive state, creative state, artistic vibes, musical escape, sound escape, ambient escape, tranquil beats, harmonic beats, harmonic sounds, melodic sounds, soft rhythms, tranquil rhythms, soothing rhythms, gentle rhythms, relaxing rhythms, from relax station",
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
                try:
                    self.youtube.thumbnails().set(
                        videoId=broadcast['id'],
                        media_body=thumbnail_path
                    ).execute()
                except Exception as e:
                    logging.error(f"Error subiendo miniatura: {str(e)}")
                finally:
                    os.remove(thumbnail_path)
            
            logging.info(f"📡 Transmisión programada para {scheduled_start}")
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
        except Exception as e:
            logging.error(f"Error obteniendo estado del stream: {str(e)}")
            return None
    
    def transicionar_estado(self, broadcast_id, estado):
        if not self.youtube:
            return False
            
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
        if not self.youtube:
            return False
            
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
    canciones_disponibles = [
        m for m in gestor.medios['musica'] 
        if m['local_path'] and m['name'] not in gestor.canciones_usadas
    ]
    
    if not canciones_disponibles:
        # Si todas las canciones han sido usadas, reiniciamos la lista
        gestor.canciones_usadas = []
        canciones_disponibles = [m for m in gestor.medios['musica'] if m['local_path']]
    
    if not canciones_disponibles:
        raise Exception("No hay música disponible para transmitir")
    
    cancion_seleccionada = random.choice(canciones_disponibles)
    gestor.canciones_usadas.append(cancion_seleccionada['name'])
    return cancion_seleccionada

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
        f"Lofi Chill {categoria.capitalize()} • Ideal para {actividad} {emoji_act} | {beneficio}",
        f"Música suave para {actividad} {emoji_act} • {beneficio} garantizado",
        f"Ambiente Lofi para {actividad} {emoji_act} • {beneficio} y más",
        f"🎵 Lofi Chill para {actividad} {emoji_act} • {beneficio}",
        f"Tus momentos de {actividad} {emoji_act} con música Lofi {categoria.capitalize()} • {beneficio}",
        f"Lofi Chill Daily • {actividad} {emoji_act} y {beneficio.lower()}",
        f"Música relajante tipo Lofi Chill • {beneficio} mientras {actividad.lower()}s {emoji_act}",
        f"🌙 Sesión de Lofi Chill para {actividad} {emoji_act} • {beneficio}",
        f"Lofi Vibes para {actividad} {emoji_act} • {beneficio} incluido"
    ]
    
    return random.choice(plantillas)

def crear_lista_reproduccion(gestor, duracion_horas=STREAM_DURATION_HOURS):
    """Crea una lista de reproducción aleatoria que durará aproximadamente duracion_horas"""
    canciones = [m for m in gestor.medios['musica'] if m['local_path']]
    if not canciones:
        raise Exception("No hay canciones disponibles")
    
    # Mezclar las canciones aleatoriamente
    random.shuffle(canciones)
    
    # Calcular cuántas canciones necesitamos (estimando 4 minutos por canción)
    canciones_necesarias = int((duracion_horas * 60) / 4)
    
    # Si no hay suficientes canciones, repetiremos algunas
    lista_reproduccion = []
    while len(lista_reproduccion) < canciones_necesarias:
        lista_reproduccion.extend(canciones)
    
    # Ajustar al número exacto necesario
    lista_reproduccion = lista_reproduccion[:canciones_necesarias]
    
    logging.info(f"🎶 Lista de reproducción creada con {len(lista_reproduccion)} canciones")
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
        
        # Comando FFmpeg optimizado para YouTube Live con mejor balance calidad/rendimiento
        cmd = [
            "ffmpeg",
            "-loglevel", "warning",  # Reducimos el nivel de log para evitar saturación
            "-rtbufsize", "150M",    # Buffer más pequeño para mejor respuesta
            "-re",
            "-f", "concat",
            "-safe", "0",
            "-i", lista_archivo,
            "-stream_loop", "-1",
            "-i", stream_data['video']['local_path'],
            "-map", "0:a:0",
            "-map", "1:v:0",
            "-ignore_unknown",
            "-c:v", "libx264",
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,format=yuv420p",
            "-preset", "veryfast",   # Más rápido que ultrafast con mejor calidad
            "-tune", "zerolatency",
            "-x264-params", "keyint=60:min-keyint=60:scenecut=0",
            "-b:v", "2000k",         # Bitrate reducido pero suficiente para 1080p
            "-maxrate", "2500k",
            "-bufsize", "5000k",
            "-r", "24",              # Frame rate más bajo para reducir carga
            "-g", "48",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "96k",
            "-ar", "44100",
            "-ac", "1",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        logging.info(f"🔧 Comando FFmpeg optimizado:\n{' '.join(cmd)}")
        
        proceso = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Hilo para leer la salida de FFmpeg en tiempo real
        def leer_salida():
            for linea in proceso.stdout:
                if "error" in linea.lower() or "fail" in linea.lower():
                    logging.error(f"FFMPEG ERROR: {linea.strip()}")
                elif "warning" in linea.lower():
                    logging.warning(f"FFMPEG WARNING: {linea.strip()}")
        
        threading.Thread(target=leer_salida, daemon=True).start()
        
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
            proceso.terminate()
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
        tiempo_fin = tiempo_inicio + timedelta(hours=STREAM_DURATION_HOURS)
        
        while datetime.utcnow() < tiempo_fin:
            if proceso.poll() is not None:
                logging.warning("⚡ Reconectando FFmpeg...")
                proceso.terminate()
                proceso = subprocess.Popen(cmd)
            
            # Verificar periodicamente el estado del stream
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado != 'active':
                logging.warning(f"⚠️ Estado del stream inesperado: {estado}")
                
            time.sleep(30)
        
        proceso.terminate()
        os.remove(lista_archivo)
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        logging.info("🛑 Transmisión finalizada y archivada correctamente")

    except Exception as e:
        logging.error(f"Error en hilo de transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])
        if 'lista_archivo' in locals() and os.path.exists(lista_archivo):
            os.remove(lista_archivo)

def ciclo_transmision():
    logging.info("🔄 Iniciando ciclo de transmisión...")
    
    # Primero cargar todos los medios
    gestor = GestorContenido()
    
    # Verificar que tenemos contenido
    if not gestor.medios['videos'] or not gestor.medios['musica']:
        logging.error("❌ No hay suficientes medios para transmitir")
        time.sleep(60)
        return
    
    # Luego autenticar con YouTube
    youtube = YouTubeManager()
    if not youtube.youtube:
        logging.error("❌ No se pudo autenticar con YouTube, reintentando en 1 minuto...")
        time.sleep(60)
        return
    
    current_stream = None
    
    while True:
        try:
            if not current_stream:
                # Seleccionar video no usado recientemente
                video = gestor.seleccionar_video_no_usado()
                logging.info(f"🎥 Video seleccionado: {video['name']}")
                
                # Crear playlist de música con canciones no usadas recientemente
                playlist = crear_lista_reproduccion(gestor)
                primera_cancion = playlist[0]
                categoria = determinar_categoria(primera_cancion['name'])
                logging.info(f"🎵 Primera canción: {primera_cancion['name']} ({categoria})")
                
                # Generar título atractivo
                titulo = generar_titulo_musica(primera_cancion['name'], categoria)
                logging.info(f"📝 Título generado: {titulo}")
                
                # Crear transmisión en YouTube
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
                    "end_time": stream_info['scheduled_start'] + timedelta(hours=STREAM_DURATION_HOURS)
                }

                # Iniciar transmisión en segundo plano
                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, youtube),
                    daemon=True
                ).start()
                
                next_stream_time = current_stream['end_time'] + timedelta(minutes=5)
            
            else:
                # Esperar hasta que sea hora de la próxima transmisión
                if datetime.utcnow() >= next_stream_time:
                    current_stream = None
                    logging.info("🔄 Preparando nueva transmisión...")
                    time.sleep(10)  # Pequeña pausa entre transmisiones
                else:
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
    
    # Iniciar ciclo de transmisión en segundo plano
    threading.Thread(target=ciclo_transmision, daemon=True).start()
    
    # Iniciar servidor web
    serve(app, host='0.0.0.0', port=10000)
