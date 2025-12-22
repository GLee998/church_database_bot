"""
Запуск бота в режиме polling (для разработки и production)
"""
import logging
import asyncio
import threading
import uvicorn
import os
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler

from app.config import settings
from app.bot import bot
from app.sheets import sheets_client
from app.sessions import session_manager
from app.gemini import gemini
from app.auth import auth_manager

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=getattr(logging, settings.log_level.upper())
)
logger = logging.getLogger(__name__)


async def post_init(application):
    """Действия после инициализации бота"""
    try:
        # Инициализация Gemini AI
        await gemini.initialize()
        logger.info("✅ Gemini AI initialized")
        
        # Проверка Google Sheets
        headers = await sheets_client.get_headers()
        data = await sheets_client.get_all_data()
        logger.info(f"✅ Google Sheets connected: {len(headers)} columns, {len(data) - 1} records")
        
        # Проверка пользователей
        try:
            users = await auth_manager._get_users_data()
            logger.info(f"✅ Users loaded: {len(users) - 1 if users else 0} users")
        except:
            logger.warning("⚠️ Could not load users data")
        
        # Информация о боте
        bot_info = await application.bot.get_me()
        logger.info(f"🚀 Bot @{bot_info.username} is running in POLLING mode")
        logger.info(f"🆔 Bot ID: {bot_info.id}")
        logger.info(f"👑 Main admin ID: {settings.main_admin_id}")
        
    except Exception as e:
        logger.error(f"❌ Initialization error: {e}")
        raise


async def post_stop(application):
    """Действия перед остановкой бота"""
    logger.info("🛑 Stopping bot...")
    await session_manager.cleanup_expired_sessions()
    logger.info("✅ Cleanup completed")


def run_fastapi():
    """Запуск FastAPI сервера"""
    port = int(os.getenv("PORT", 8080))
    logger.info(f"🚀 Starting FastAPI server on port {port}...")
    try:
        # Устанавливаем новый цикл событий для uvicorn в этом потоке
        asyncio.set_event_loop(asyncio.new_event_loop())
        uvicorn.run("app.main:app", host="0.0.0.0", port=port, log_level="info")
    except Exception as e:
        logger.error(f"💥 FastAPI server error: {e}")


def main():
    """Основная функция запуска бота"""
    # Запуск FastAPI в отдельном потоке
    api_thread = threading.Thread(target=run_fastapi, name="FastAPIThread", daemon=True)
    api_thread.start()
    logger.info("📡 FastAPI server thread started")

    # Создаем приложение
    application = Application.builder() \
        .token(settings.telegram_token) \
        .post_init(post_init) \
        .post_stop(post_stop) \
        .build()
    
    # ========== РЕГИСТРАЦИЯ ОБРАБОТЧИКОВ КОМАНД ==========
    application.add_handler(CommandHandler("start", bot.handle_start_command))
    application.add_handler(CommandHandler("menu", bot.handle_menu_command))
    application.add_handler(CommandHandler("help", bot.handle_help_command))
    application.add_handler(CommandHandler("view", bot.handle_view_command))
    application.add_handler(CommandHandler("edit", bot.handle_edit_command))
    application.add_handler(CommandHandler("create", bot.handle_create_command))
    application.add_handler(CommandHandler("ask", bot.handle_ask_command))
    application.add_handler(CommandHandler("admin", bot.handle_admin_command))
    application.add_handler(CallbackQueryHandler(bot.handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    # ========== ЗАПУСК БОТА ==========
    logger.info("=" * 50)
    logger.info("🚀 Starting Church Telegram Bot v2.0")
    logger.info(f"📁 Environment: {settings.environment}")
    logger.info(f"🔧 Log level: {settings.log_level}")
    logger.info("=" * 50)
    
    try:
        logger.info("⏳ Starting polling...")
        application.run_polling(
            poll_interval=0.5,
            timeout=30,
            drop_pending_updates=True,
            allowed_updates=['message', 'callback_query']
        )
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"💥 Critical error: {e}")
        raise
    finally:
        logger.info("👋 Bot stopped")


if __name__ == "__main__":
    # Проверка обязательных переменных окружения
    required_vars = ['TELEGRAM_TOKEN', 'SHEET_ID', 'GEMINI_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not getattr(settings, var.lower(), None):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please check your .env file")
        exit(1)
    
    # Запуск
    main()
