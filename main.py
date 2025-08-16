import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime, time, UTC
from dotenv import load_dotenv

load_dotenv()

LOGSTASH_HOST = os.getenv("LOGSTASH_HOST")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT"))
APP_LIST = os.getenv("APP_LIST").split(",")
RETRY_DELAY = int(os.getenv("RETRY_DELAY"))
UNSENT_FILE = "unsent.log"


LOG_FILENAME = "agent.log"
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(LOG_DIR, LOG_FILENAME)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    LOG_PATH,
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8',
    atTime=time(hour=6, minute=0, second=0)
)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class LogstashClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.writer = None
        self.connected = False
        self.queue = asyncio.Queue()
        self.load_unsent()

    def load_unsent(self):
        if os.path.exists(UNSENT_FILE):
            with open(UNSENT_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.queue.put_nowait(line)
            logger.info(f"Загружено {self.queue.qsize()} неотправленных сообщений из {UNSENT_FILE}")

    def save_unsent(self, message):
        with open(UNSENT_FILE, "a", encoding="utf-8") as f:
            f.write(message + "\n")

    async def connect(self):
        while not self.connected:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                self.writer = writer
                self.connected = True
                logger.info(f"Подключено к Logstash {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Не удалось подключиться к Logstash: {e}")
                await asyncio.sleep(RETRY_DELAY)

    async def send_loop(self):
        """Бесконечный цикл, который берёт сообщения из очереди и отправляет их."""
        while True:
            msg = await self.queue.get()
            if not self.connected or self.writer is None:
                await self.connect()
            try:
                self.writer.write((msg + "\n").encode())
                await self.writer.drain()
                logger.info(f"Отправлено: {msg}")
            except Exception as e:
                logger.error(f"Ошибка отправки: {e}")
                self.connected = False
                self.writer = None
                self.save_unsent(msg)
                await asyncio.sleep(RETRY_DELAY)
                self.queue.put_nowait(msg)  # вернуть в очередь для повторной попытки


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, source_app: str, logstash_client: LogstashClient):
    addr = writer.get_extra_info('peername')
    logger.info(f"[{source_app}] Подключение от {addr}")
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            message = data.decode().strip()
            if not message:
                continue
            logger.info(f"[{source_app}] Получено: {message}")
            payload = f"{datetime.now(UTC).isoformat()} | {source_app} | {message}"
            logstash_client.queue.put_nowait(payload)
    finally:
        logger.info(f"[{source_app}] Отключение клиента {addr}")
        writer.close()
        await writer.wait_closed()


async def main():
    logstash_client = LogstashClient(LOGSTASH_HOST, LOGSTASH_PORT)
    asyncio.create_task(logstash_client.send_loop())

    servers = []
    for item in APP_LIST:
        try:
            app_name, port_str = item.split(":")
            port = int(port_str)
        except ValueError:
            logger.error(f"Неправильный формат в APP_LIST: {item}, пропускаем")
            continue

        server = await asyncio.start_server(
            lambda r, w, sa=app_name: handle_client(r, w, sa, logstash_client),
            '0.0.0.0', port
        )
        servers.append(server)
        logger.info(f"Сервер запущен на порту {port} для приложения {app_name}")

    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    asyncio.run(main())
