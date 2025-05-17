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

# Palabras clave mejoradas para categorización
PALABRAS_CLAVE = {
    'relax': ['relax', 'calm', 'peaceful', 'tranquil', 'serene'],
    'instrumental': ['instrumental', 'piano', 'guitar', 'violin', 'cello'],
    'ambient': ['ambient', 'atmospheric', 'space', 'nature', 'rain'],
    'jazz': ['jazz', 'smooth', 'blues', 'bossa', 'lounge'],
    'classical': ['classical', 'orchestra', 'symphony', 'mozart', 'beethoven'],
    'lofi': ['lofi', 'lowfi', 'chillhop', 'chillout', 'studybeats']
}

class MediaBuffer:
    def __init__(self, buffer_dir="./stream_buffer"):
        self.buffer_dir = os.path.abspath(buffer_dir)
        os.makedirs(self.buffer_dir, exist_ok=True)
        self.buffer_files = []
        self.current_index = 0
        self.buffer_size = 5  # Minutos de buffer
        self.segment_duration = 30  # Duración de cada segmento en segundos

    def create_buffer_segments(self, video_path, audio_paths):
        """Crea segmentos de video+audio para el buffer"""
        try:
            # Limpiar buffer anterior
            for f in os.listdir(self.buffer_dir):
                os.remove(os.path.join(self.buffer_dir, f))
            self.buffer_files = []
            
            # Crear lista de reproducción temporal para el audio
            audio_list = os.path.join(self.buffer_dir, "audio_list.txt")
            with open(audio_list, 'w') as f:
                for audio in audio_paths:
                    f.write(f"file '{audio}'\n")
            
            # Generar segmentos bufferizados
            for i in range(self.buffer_size * 2):  # 2 segmentos por minuto
                output_file = os.path.join(self.buffer_dir, f"buffer_{i}.flv")
                
                # Calcular punto de inicio aleatorio en el video (para variedad)
                start_time = random.randint(0, 30)
                
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-ss", str(start_time),
                    "-i", video_path,
                    "-f", "concat",
                    "-safe", "0",
                    "-i", audio_list,
                    "-t", str(self.segment_duration),
                    "-map", "0:v:0",
                    "-map", "1:a:0",
                    "-c:v", "libx264",
                    "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
                    "-preset", "ultrafast",
                    "-tune", "zerolatency",
                    "-x264-params", "keyint=60:min-keyint=60:scenecut=0",
                    "-b:v", "1500k",
                    "-maxrate", "1500k",
                    "-bufsize", "3000k",
                    "-r", "24",
                    "-g", "48",
                    "-pix_fmt", "yuv420p",
                    "-c:a", "aac",
                    "-b:a", "96k",
                    "-ar", "44100",
                    "-ac", "1",
                    "-f", "flv",
                    output_file
                ]
                
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.buffer_files.append(output_file)
                logging.info(f"🔄 Generado segmento de buffer {i+1}/{self.buffer_size*2}")
            
            os.remove(audio_list)
            return True
        except Exception as e:
            logging.error(f"Error creando segmentos de buffer: {str(e)}")
            return False

    def get_next_segment(self):
        """Obtiene el siguiente segmento del buffer (round-robin)"""
        if not self.buffer_files:
            return None
            
        segment = self.buffer_files[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.buffer_files)
        return segment

class GestorContenido:
    def __init__(self):
        self.media_cache_dir = os.path.abspath("./media_cache")
        os.makedirs(self.media_cache_dir, exist_ok=True)
        self.medios = self.cargar_medios()
        self.videos_usados = []
        self.reiniciar_videos_usados()
        self.media_buffer = MediaBuffer()
    
    def reiniciar_videos_usados(self):
        if len(self.videos_usados) >= len(self.medios['videos']):
            self.videos_usados = []
    
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
            
            # Filtrar videos válidos
            datos['videos'] = [v for v in datos['videos'] if v.get('url') and v.get('name')]
            
            # Descargar videos primero
            logging.info("🎥 Iniciando descarga de videos...")
            for i, video in enumerate(datos['videos'], 1):
                try:
                    logging.info(f"⬇️ Descargando video {i}/{len(datos['videos'])}: {video['name']}")
                    video['local_path'] = self.descargar_video(video['url'])
                    if not video['local_path']:
                        raise Exception(f"Fallo al descargar video: {video['name']}")
                except Exception as e:
                    logging.error(f"Error con video {video['name']}: {str(e)}")
                    video['local_path'] = None
            
            # Filtrar videos que no se pudieron descargar
            datos['videos'] = [v for v in datos['videos'] if v['local_path']]
            
            # Luego descargar música
            logging.info("🎵 Iniciando descarga de música...")
            for j, cancion in enumerate(datos['musica'], 1):
                try:
                    logging.info(f"⬇️ Descargando canción {j}/{len(datos['musica'])}: {cancion['name']}")
                    cancion['local_path'] = self.descargar_audio(cancion['url'])
                    if not cancion['local_path']:
                        raise Exception(f"Fallo al descargar canción: {cancion['name']}")
                except Exception as e:
                    logging.error(f"Error con canción {cancion['name']}: {str(e)}")
                    cancion['local_path'] = None
            
            # Filtrar música que no se pudo descargar
            datos['musica'] = [m for m in datos['musica'] if m['local_path']]
            
            logging.info(f"✅ Medios descargados: {len(datos['videos'])} videos y {len(datos['musica'])} canciones")
            return datos
        except Exception as e:
            logging.error(f"❌ Error cargando medios: {str(e)}")
            return {"videos": [], "musica": []}
    
    def seleccionar_video_aleatorio(self):
        self.reiniciar_videos_usados()
        videos_disponibles = [
            v for v in self.medios['videos'] 
            if v['local_path'] and v['name'] not in self.videos_usados
        ]
        
        if not videos_disponibles:
            logging.warning("⚠️ No hay videos nuevos disponibles, reiniciando selección")
            self.videos_usados = []
            videos_disponibles = [v for v in self.medios['videos'] if v['local_path']]
        
        if not videos_disponibles:
            raise Exception("No hay videos disponibles para transmitir")
        
        video = random.choice(videos_disponibles)
        self.videos_usados.append(video['name'])
        return video

    def preparar_buffer_transmision(self, video, playlist):
        """Prepara segmentos bufferizados para transmisión fluida"""
        audio_paths = [c['local_path'] for c in playlist]
        return self.media_buffer.create_buffer_segments(video['local_path'], audio_paths)

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
                "-y", "-ss", "00:00:05",
                "-i", video_path,
                "-vframes", "1",
                "-q:v", "2",
                "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1",
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
                    "description": "Síguenos en nuestras redes sociales:\n\n🎵 Spotify: https://open.spotify.com/intl-es/artist/7J4Rf0Q97OcDjg3kmBXSRj\n📷 Instagram: http://instagram.com/@desderelaxstation\n👍 Facebook: https://www.facebook.com/people/Desde-Relax-Station/61574709615178/\n🎶 TikTok: https://www.tiktok.com/@desderelaxstation\n\nMúsica relajante para estudiar, trabajar, meditar o dormir. Transmisión en vivo 24/7 con los mejores sonidos ambientales y música instrumental.",
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
                        "format": "720p",  # Cambiado a 720p para mejor rendimiento
                        "ingestionType": "rtmp",
                        "resolution": "720p",
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
                    logging.info("🖼️ Miniatura subida correctamente")
                except Exception as e:
                    logging.error(f"Error subiendo miniatura: {str(e)}")
                finally:
                    try:
                        os.remove(thumbnail_path)
                    except:
                        pass
            
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
            if any(p in palabra for p in palabras):
                contador[categoria] += 1
                
    max_categoria = max(contador, key=contador.get)
    return max_categoria if contador[max_categoria] > 0 else random.choice(list(PALABRAS_CLAVE.keys()))

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

def crear_lista_reproduccion(gestor, duracion_horas=8):
    """Crea una lista de reproducción aleatoria que durará aproximadamente duracion_horas"""
    canciones = [m for m in gestor.medios['musica'] if m['local_path']]
    if not canciones:
        raise Exception("No hay canciones disponibles")
    
    # Mezclar las canciones aleatoriamente
    random.shuffle(canciones)
    
    # Calcular cuántas canciones necesitamos (estimando 3.5 minutos por canción para mejor ajuste)
    canciones_necesarias = max(10, int((duracion_horas * 60) / 3.5))
    
    # Si no hay suficientes canciones, repetiremos algunas
    lista_reproduccion = []
    while len(lista_reproduccion) < canciones_necesarias:
        lista_reproduccion.extend(canciones)
    
    # Ajustar al número exacto necesario
    lista_reproduccion = lista_reproduccion[:canciones_necesarias]
    
    logging.info(f"🎶 Lista de reproducción creada con {len(lista_reproduccion)} canciones")
    return lista_reproduccion

def manejar_transmision(stream_data, youtube, gestor):
    try:
        # Preparar buffer de transmisión
        if not gestor.preparar_buffer_transmision(stream_data['video'], stream_data['playlist']):
            raise Exception("No se pudo preparar el buffer de transmisión")
        
        tiempo_inicio_ffmpeg = stream_data['start_time'] - timedelta(minutes=1)
        espera_ffmpeg = (tiempo_inicio_ffmpeg - datetime.utcnow()).total_seconds()
        
        if espera_ffmpeg > 0:
            logging.info(f"⏳ Esperando {espera_ffmpeg:.0f} segundos para iniciar FFmpeg...")
            time.sleep(espera_ffmpeg)
        
        # Comando FFmpeg optimizado para transmisión por segmentos
        cmd = [
            "ffmpeg",
            "-loglevel", "warning",
            "-re",
            "-f", "concat",
            "-safe", "0",
            "-i", "-",  # Leer lista de archivos desde stdin
            "-c", "copy",
            "-f", "flv",
            stream_data['rtmp']
        ]
        
        logging.info("🔧 Iniciando FFmpeg con transmisión por segmentos bufferizados")
        
        # Configurar proceso FFmpeg
        proceso = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Hilo para leer la salida de FFmpeg
        def leer_salida():
            for linea in proceso.stdout:
                if "error" in linea.lower() or "fail" in linea.lower():
                    logging.error(f"FFMPEG ERROR: {linea.strip()}")
                elif "warning" in linea.lower():
                    logging.warning(f"FFMPEG WARNING: {linea.strip()}")
        
        threading.Thread(target=leer_salida, daemon=True).start()
        
        # Enviar segmentos al FFmpeg
        def enviar_segmentos():
            try:
                while True:
                    segmento = gestor.media_buffer.get_next_segment()
                    if not segmento:
                        break
                    
                    # Enviar el segmento a FFmpeg
                    with open(segmento, 'rb') as f:
                        while True:
                            chunk = f.read(4096)
                            if not chunk:
                                break
                            proceso.stdin.write(chunk)
                    
                    logging.debug(f"📦 Segmento enviado: {os.path.basename(segmento)}")
                    time.sleep(0.1)  # Pequeña pausa entre segmentos
            
            except Exception as e:
                logging.error(f"Error enviando segmentos: {str(e)}")
            finally:
                proceso.stdin.close()
        
        threading.Thread(target=enviar_segmentos, daemon=True).start()
        
        logging.info("🟢 FFmpeg iniciado - Estableciendo conexión RTMP...")
        
        # Esperar a que el stream esté activo
        max_checks = 15
        stream_activo = False
        for i in range(max_checks):
            estado = youtube.obtener_estado_stream(stream_data['stream_id'])
            if estado == 'active':
                logging.info("✅ Stream activo - Transicionando a testing")
                if youtube.transicionar_estado(stream_data['broadcast_id'], 'testing'):
                    logging.info("🎬 Transmisión en VISTA PREVIA")
                    stream_activo = True
                break
            logging.info(f"🔄 Esperando activación del stream ({i+1}/{max_checks})")
            time.sleep(5)
        
        if not stream_activo:
            logging.error("❌ Stream no se activó a tiempo")
            proceso.terminate()
            return
        
        # Esperar hasta la hora programada
        tiempo_restante = (stream_data['start_time'] - datetime.utcnow()).total_seconds()
        if tiempo_restante > 0:
            logging.info(f"⏳ Esperando {tiempo_restante:.0f}s para LIVE...")
            time.sleep(tiempo_restante)
        
        if youtube.transicionar_estado(stream_data['broadcast_id'], 'live'):
            logging.info("🎥 Transmisión LIVE iniciada")
        else:
            raise Exception("No se pudo iniciar la transmisión")
        
        # Monitorear la transmisión
        tiempo_inicio = datetime.utcnow()
        ultimo_check = tiempo_inicio
        
        while (datetime.utcnow() - tiempo_inicio) < timedelta(hours=8):
            if proceso.poll() is not None:
                logging.warning("⚡ FFmpeg se detuvo, reconectando...")
                proceso = subprocess.Popen(cmd)
                threading.Thread(target=enviar_segmentos, daemon=True).start()
                ultimo_check = datetime.utcnow()
            
            # Verificar estado periódicamente
            if (datetime.utcnow() - ultimo_check) > timedelta(minutes=5):
                estado = youtube.obtener_estado_stream(stream_data['stream_id'])
                logging.info(f"🔄 Estado del stream: {estado}")
                ultimo_check = datetime.utcnow()
            
            time.sleep(15)
        
        # Finalizar transmisión
        proceso.terminate()
        
        if youtube.finalizar_transmision(stream_data['broadcast_id']):
            logging.info("🛑 Transmisión finalizada y archivada correctamente")
        else:
            logging.error("⚠️ No se pudo finalizar la transmisión correctamente")

    except Exception as e:
        logging.error(f"Error en hilo de transmisión: {str(e)}")
        youtube.finalizar_transmision(stream_data['broadcast_id'])

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
                # Seleccionar video aleatorio (sin repetir)
                video = gestor.seleccionar_video_aleatorio()
                logging.info(f"🎥 Video seleccionado: {video['name']}")
                
                # Crear playlist de música
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
                    "end_time": stream_info['scheduled_start'] + timedelta(hours=8)
                }

                # Iniciar transmisión en segundo plano
                threading.Thread(
                    target=manejar_transmision,
                    args=(current_stream, youtube, gestor),
                    daemon=True
                ).start()
                
                next_stream_time = current_stream['end_time'] + timedelta(minutes=5)
            
            else:
                # Esperar hasta que sea hora de la próxima transmisión
                tiempo_espera = (next_stream_time - datetime.utcnow()).total_seconds()
                if tiempo_espera <= 0:
                    current_stream = None
                    logging.info("🔄 Preparando nueva transmisión...")
                else:
                    logging.info(f"⏳ Próxima transmisión en {tiempo_espera/60:.1f} minutos")
                    time.sleep(min(60, tiempo_espera))
        
        except Exception as e:
            logging.error(f"🔥 Error crítico en ciclo de transmisión: {str(e)}")
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
