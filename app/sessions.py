"""
Управление сессиями пользователей
"""
import json
import time
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from app.config import settings

logger = logging.getLogger(__name__)


class SessionManager:
    """Менеджер сессий"""
    
    def __init__(self):
        self._sessions: Dict[int, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    def _create_new_session(self, chat_id: int) -> Dict[str, Any]:
        """Создание новой сессии"""
        return {
            'state': 'IDLE',
            'mode': None,
            'draft': {},
            'step': None,
            'last_letter': None,
            'people_list': [],
            'viewing_row': None,
            'editing_row': None,
            'gemini_question': None,
            'user_id': None,
            'last_access': time.time(),
            'created_at': datetime.now().isoformat()
        }
    
    async def get_session(self, chat_id: int) -> Dict[str, Any]:
        """Получение сессии"""
        async with self._lock:
            if chat_id in self._sessions:
                session = self._sessions[chat_id]
                # Проверка таймаута
                if time.time() - session.get('last_access', 0) < settings.session_timeout.total_seconds():
                    session['last_access'] = time.time()
                    return session
                else:
                    del self._sessions[chat_id]
            
            # Новая сессия
            new_session = self._create_new_session(chat_id)
            self._sessions[chat_id] = new_session
            return new_session
    
    async def save_session(self, chat_id: int, session_data: Dict[str, Any]):
        """Сохранение сессии"""
        async with self._lock:
            session_data['last_access'] = time.time()
            self._sessions[chat_id] = session_data
    
    async def clear_session(self, chat_id: int):
        """Очистка сессии"""
        async with self._lock:
            if chat_id in self._sessions:
                del self._sessions[chat_id]
    
    async def cleanup_expired_sessions(self):
        """Очистка устаревших сессий"""
        current_time = time.time()
        timeout = settings.session_timeout.total_seconds()
        
        async with self._lock:
            expired = [
                chat_id for chat_id, session in self._sessions.items()
                if current_time - session.get('last_access', 0) > timeout
            ]
            
            for chat_id in expired:
                del self._sessions[chat_id]
            
            if expired:
                logger.info(f"🧹 Cleaned {len(expired)} expired sessions")


# Глобальный экземпляр
session_manager = SessionManager()