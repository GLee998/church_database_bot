"""
Интеграция с Gemini AI для анализа таблицы
"""
import logging
import asyncio
from typing import List, Dict, Any
import google.generativeai as genai
from datetime import datetime

from app.config import settings
from app.utils import formatter

logger = logging.getLogger(__name__)


class GeminiAI:
    """Класс для работы с Gemini AI"""
    
    def __init__(self):
        self.model = None
        self.initialized = False
    
    async def initialize(self):
        """Инициализация Gemini AI"""
        if self.initialized:
            return
        
        try:
            genai.configure(api_key=settings.gemini_api_key)
            
            # Настраиваем модель
            self.model = genai.GenerativeModel(
                model_name="gemini-2.5-flash",
                generation_config={
                    "temperature": 0.3,
                    "top_p": 0.8,
                    "top_k": 40,
                    "max_output_tokens": settings.max_gemini_tokens,
                },
                safety_settings=[
                    {
                        "category": "HARM_CATEGORY_HARASSMENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_HATE_SPEECH",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                ]
            )
            
            self.initialized = True
            logger.info("✅ Gemini AI initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Gemini AI: {e}")
            raise
    
    async def analyze_table(self, question: str, headers: List[str], data: List[List[str]]) -> str:
        """Анализ таблицы с помощью Gemini AI"""
        if not self.initialized:
            await self.initialize()
        
        try:
            # Форматируем таблицу для Gemini
            table_text = formatter.format_table_for_gemini(headers, data)
            
            # Получаем текущую дату
            current_date = datetime.now().strftime("%d.%m.%Y")

            # Создаем промпт с указанием даты
            prompt = f"""
            Ты аналитик базы данных.
            Сегодняшняя дата: {current_date}.
            
            Структура таблицы (колонки):
            {', '.join(headers)}
            
            Данные таблицы (первые 100 строк):
            {table_text}
            
            Вопрос пользователя: {question}
            
            Твоя задача:
            1. Понимать вопросы о данных в таблице
            2. Отвечать четко и по делу
            3. Если информации нет в таблице - честно говори об этом
            4. Форматируй ответ для Telegram (можно использовать emoji)
            5. Если вопрос требует подсчета или анализа - делай его
            
            Ответ должен быть на русском языке.
            Не придумывай информацию, которой нет в таблице.
            """
            
            # Отправляем запрос асинхронно
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )
            
            # Получаем текст ответа
            answer = response.text.strip()
            
            # Если ответ слишком длинный, обрезаем его
            if len(answer) > 3000:
                answer = answer[:3000] + "...\n\n⚠️ Ответ был сокращен из-за ограничений Telegram"
            
            return answer
            
        except Exception as e:
            logger.error(f"❌ Gemini AI error: {e}")
            return f"⚠️ Ошибка при обработке запроса: {str(e)}"
    
    async def get_table_summary(self, headers: List[str], data: List[List[str]]) -> str:
        """Получение краткого анализа таблицы"""
        if not self.initialized:
            await self.initialize()
        
        try:
            table_text = formatter.format_table_for_gemini(headers[:5], data[:50])  # Только часть данных для анализа
            
            prompt = f"""
            Дана таблица церковной базы данных.
            
            Колонки: {', '.join(headers)}
            
            Примеры данных:
            {table_text}
            
            Сделай краткий анализ:
            1. Основная структура данных
            2. Какие типы информации хранятся
            3. Предложения по улучшению (если есть)
            
            Отвечай кратко, по пунктам.
            """
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )
            
            return response.text.strip()
            
        except Exception as e:
            logger.error(f"❌ Gemini summary error: {e}")
            return "Не удалось получить анализ таблицы."


# Глобальный экземпляр
gemini = GeminiAI()