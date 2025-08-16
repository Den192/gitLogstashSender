import asyncio

HOST = "127.0.0.1"  # адрес агента
PORT = 5001         # порт для теста

async def send_message(msg):
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write((msg + "\n").encode())
    await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    print(f"Отправка сообщений на {HOST}:{PORT}")
    try:
        while True:
            msg = input("> ")
            if not msg:
                continue
            await send_message(msg)
    except KeyboardInterrupt:
        print("\nВыход...")

if __name__ == "__main__":
    asyncio.run(main())
