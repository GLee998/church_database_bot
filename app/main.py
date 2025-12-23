"""
FastAPI приложение для Telegram бота
"""
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from app.config import settings
from app.bot import bot
from app.sheets import sheets_client
from app.api import router as api_router
from app.sessions import session_manager
from app.gemini import gemini
from app.auth import auth_manager

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=getattr(logging, settings.log_level.upper())
)
logger = logging.getLogger(__name__)

# Глобальные переменные
telegram_app: Application = None

# Pydantic модели
class TelegramWebhook(BaseModel):
    """Модель вебхука"""
    update_id: int
    message: dict = None
    callback_query: dict = None
    edited_message: dict = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом
    """
    global telegram_app
    
    # Startup
    logger.info("🚀 Starting Church Telegram Bot v2.0 Web Backend")
    
    # Если бот запущен через run_polling.py, он уже инициализирован
    # Если запущен как вебхук, инициализируем здесь
    if not telegram_app:
        try:
            telegram_app = Application.builder().token(settings.telegram_token).build()
            
            # Регистрация обработчиков (только для режима вебхука)
            telegram_app.add_handler(CommandHandler("start", bot.handle_start_command))
            telegram_app.add_handler(CommandHandler("menu", bot.handle_menu_command))
            telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
            telegram_app.add_handler(CallbackQueryHandler(bot.handle_callback))
            
            await telegram_app.initialize()
            await telegram_app.start()
            
            # Проверка бота
            bot_info = await telegram_app.bot.get_me()
            logger.info(f"✅ Bot initialized: @{bot_info.username}")
            
        except Exception as e:
            logger.error(f"❌ Startup failed: {e}")
            # В режиме polling мы можем проигнорировать ошибку инициализации здесь, 
            # так как бот инициализируется в run_polling.py
            if settings.environment != "production": # Пример условия
                 pass
            else:
                 raise

    # Инициализация Gemini (если еще не сделано)
    if not gemini.initialized:
        await gemini.initialize()
        logger.info("🤖 Gemini AI initialized")
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down web backend...")
    
    # Мы не останавливаем telegram_app здесь, если он управляется извне (polling)
    # Но если мы в режиме вебхука, то останавливаем
    # Для простоты: если мы его создали здесь, мы его и закроем
    # Но в данной архитектуре лучше оставить управление сессиями
    
    await session_manager.cleanup_expired_sessions()
    logger.info("✅ Cleanup completed")


# Создание FastAPI приложения
app = FastAPI(
    title="Church Telegram Bot v2.0",
    description="Telegram бот для управления церковной базой данных с Gemini AI",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.is_development else None,
    redoc_url="/redoc" if settings.is_development else None
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Статические файлы Mini App
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/photos", StaticFiles(directory="static/photos"), name="photos")

# Роутер API
app.include_router(api_router)

@app.get("/webapp")
async def webapp():
    """Эндпоинт для Mini App"""
    return FileResponse("static/index.html")

# Эндпоинты
@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "service": "Church Telegram Bot v2.0",
        "version": "2.0.0",
        "environment": settings.environment,
        "status": "online",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "api": "ok",
            "telegram_bot": "initialized" if telegram_app else "not_initialized",
            "google_sheets": "unknown",
            "gemini_ai": "initialized" if gemini.initialized else "not_initialized",
        }
    }
    
    # Проверка Google Sheets
    try:
        await sheets_client.get_headers()
        health_status["components"]["google_sheets"] = "connected"
    except Exception as e:
        health_status["components"]["google_sheets"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status


@app.post("/webhook")
async def telegram_webhook(webhook_data: TelegramWebhook):
    """Вебхук Telegram"""
    if not telegram_app:
        raise HTTPException(status_code=503, detail="Telegram bot not initialized")
    
    try:
        update = Update.de_json(webhook_data.dict(), telegram_app.bot)
        await telegram_app.process_update(update)
        return {"ok": True}
    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}", exc_info=True)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/admin")
async def admin_panel():
    """Админ панель (упрощенная для примера)"""
    return HTMLResponse(content="<h1>Admin Panel</h1><p>Work in progress...</p>")


# Обработчик ошибок
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Глобальный обработчик исключений"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)
