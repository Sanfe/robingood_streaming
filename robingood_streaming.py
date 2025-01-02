import os
import asyncio
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, MessageIdInvalidError
from aiohttp import web
import sqlite3
import logging
from pathlib import Path
from guessit import guessit
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')
PROXY_PORT = int(os.getenv('PROXY_PORT', 8080))
WAIT_TIME = int(os.getenv('WAIT_TIME', 120))  # Tiempo de espera entre ciclos (en segundos)
CHANNELS = {
    'Movies': {
        'id': int(os.getenv('MOVIES_CHANNEL_ID')),
        'folder': os.getenv('MOVIES_FOLDER')
    },
    'Series': {
        'id': int(os.getenv('SERIES_CHANNEL_ID')),
        'folder': os.getenv('SERIES_FOLDER')
    }
}
VALIDATE_DUPLICATES = os.getenv('VALIDATE_DUPLICATES', 'true').lower() == 'true'

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conexión al cliente de Telegram
client = TelegramClient('bot_session', API_ID, API_HASH)

# Conexión a la base de datos
DB_PATH = os.getenv('DB_PATH', 'processed_files.db')
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS files
                  (file_id TEXT PRIMARY KEY, channel TEXT, file_path TEXT)''')

async def authenticate():
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(PHONE_NUMBER)
        code = input("Introduce el código de Telegram: ")
        try:
            await client.sign_in(PHONE_NUMBER, code)
        except SessionPasswordNeededError:
            password = input("Introduce tu contraseña de verificación en dos pasos: ")
            await client.sign_in(password=password)

async def handle_proxy_request(request):
    channel = request.match_info['channel']
    file_id = request.match_info['file_id']

    try:
        # Obtener el mensaje desde Telegram
        message = await client.get_messages(CHANNELS[channel]['id'], ids=int(file_id))
        if not message or not message.file:
            return web.Response(status=404, text="Archivo no encontrado en el canal.")

        # Configurar respuesta de streaming
        response = web.StreamResponse()
        response.content_type = message.file.mime_type or 'application/octet-stream'
        response.headers['Content-Disposition'] = f'inline; filename="{message.file.name or file_id}"'
        await response.prepare(request)

        # Descargar y transmitir el archivo por bloques
        async for chunk in client.iter_download(message.media):
            await response.write(chunk)

        await response.write_eof()
        logger.info(f"Archivo {file_id} del canal {channel} transmitido correctamente.")
        return response

    except MessageIdInvalidError:
        return web.Response(status=404, text="Archivo no encontrado en el canal.")
    except Exception as e:
        logger.error(f"Error al procesar {channel}/{file_id}: {str(e)}")
        return web.Response(status=500, text="Error interno del servidor.")

async def process_channel(channel_name, channel_info):
    channel = await client.get_entity(channel_info['id'])
    folder_path = Path(channel_info['folder'])
    folder_path.mkdir(parents=True, exist_ok=True)

    # Verificar archivos en la base de datos
    cursor.execute("SELECT file_id, file_path FROM files WHERE channel=?", (channel_name,))
    processed_files = cursor.fetchall()

    for file_id, file_path in processed_files:
        try:
            # Verificar si el archivo sigue en el canal
            await client.get_messages(channel, ids=int(file_id))
        except MessageIdInvalidError:
            # Si no existe, eliminar de la base de datos y del sistema de archivos
            cursor.execute("DELETE FROM files WHERE file_id=? AND channel=?", (file_id, channel_name))
            conn.commit()
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Archivo {file_path} eliminado por no existir en el canal.")

    # Procesar mensajes nuevos
    async for message in client.iter_messages(channel):
        if message.file and message.file.mime_type.startswith('video/'):
            file_id = str(message.id)

            # Usar guessit para analizar el nombre del archivo
            guessed_metadata = guessit(message.file.name or f"video_{file_id}.mp4")
            logger.info(f"Metadatos extraídos con guessit: {guessed_metadata}")

            # Crear estructura de carpetas según los metadatos
            title = guessed_metadata.get('title', 'Unknown')
            season = guessed_metadata.get('season', None)
            episode = guessed_metadata.get('episode', None)
            year = guessed_metadata.get('year', '')

            if season is not None and episode is not None:
                proper_folder = folder_path / title / f"Season {season}"
                proper_folder.mkdir(parents=True, exist_ok=True)
                clean_file_name = f"{title} - S{season:02}E{episode:02}.mp4"
            else:
                proper_folder = folder_path / f"{title} ({year})"
                proper_folder.mkdir(parents=True, exist_ok=True)
                clean_file_name = f"{title} ({year}).mp4"

            # Crear archivo STRM
            strm_path = proper_folder / f"{clean_file_name}.strm"

            if VALIDATE_DUPLICATES:
                cursor.execute("SELECT * FROM files WHERE file_id=? AND channel=?", (file_id, channel_name))
                if cursor.fetchone():
                    logger.info(f"Archivo {file_id} del canal {channel_name} ya procesado. Saltando.")
                    continue

            with strm_path.open('w') as strm_file:
                strm_file.write(f"http://localhost:{PROXY_PORT}/{channel_name}/{file_id}")

            cursor.execute("INSERT INTO files (file_id, channel, file_path) VALUES (?, ?, ?)", (file_id, channel_name, str(strm_path)))
            conn.commit()

            logger.info(f"Archivo STRM creado: {strm_path}")

async def main():
    await authenticate()

    app = web.Application()
    app.router.add_get('/{channel}/{file_id}', handle_proxy_request)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', PROXY_PORT)
    await site.start()

    logger.info("Servidor HTTP de streaming iniciado.")

    while True:
        for channel_name, channel_info in CHANNELS.items():
            await process_channel(channel_name, channel_info)
        await asyncio.sleep(WAIT_TIME)

if __name__ == '__main__':
    asyncio.run(main())
