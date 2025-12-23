"""
Аутентификация и управление доступом
"""
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from app.config import settings
from app.sheets import sheets_client
from app.utils import html

logger = logging.getLogger(__name__)


class AuthManager:
    """Менеджер аутентификации"""
    
    def __init__(self):
        self._users_cache = None
        self._logs_cache = None
    
    async def check_access(self, user_id: int, user_info: Dict[str, Any]) -> bool:
        """Проверка доступа"""
        # Главный администратор
        if user_id == settings.main_admin_id:
            await self._log_access(user_info, "GRANTED_ADMIN")
            return True
        
        # Проверка в белом списке
        has_access = await self._is_user_in_whitelist(user_id)
        
        # Логирование
        status = "GRANTED" if has_access else "DENIED"
        await self._log_access(user_info, status)
        
        return has_access
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверка администратора"""
        if user_id == settings.main_admin_id:
            return True
        
        try:
            users = await self._get_users_data()
            for user in users[1:]:
                if len(user) >= 4:
                    stored_id = int(user[0]) if user[0] else 0
                    if stored_id == user_id and user[3] == "admin":
                        return True
        except Exception as e:
            logger.error(f"Error checking admin: {e}")
        
        return False
    
    async def add_user(self, user_id: int, username: str, 
                      first_name: str, last_name: str, 
                      user_type: str = "user") -> str:
        """Добавление пользователя"""
        try:
            # Проверка существования
            users = await self._get_users_data()
            for user in users[1:]:
                if user and user[0] and int(user[0]) == user_id:
                    return f"⚠️ Пользователь {user_id} уже существует"
            
            # Добавление
            await sheets_client.append_row([
                user_id,
                username or "",
                f"{first_name or ''} {last_name or ''}".strip(),
                user_type
            ], "Users")
            
            self._users_cache = None
            
            role = "👑 Админ" if user_type == "admin" else "👤 Пользователь"
            return f"✅ Пользователь добавлен\nID: {user_id}\nРоль: {role}"
            
        except Exception as e:
            logger.error(f"Error adding user: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def remove_user(self, user_id: int) -> str:
        """Удаление пользователя"""
        if user_id == settings.main_admin_id:
            return "❌ Нельзя удалить главного администратора!"
        
        try:
            users = await self._get_users_data()
            found = False
            
            for i in range(len(users) - 1, 0, -1):
                if users[i] and users[i][0]:
                    if int(users[i][0]) == user_id:
                        worksheet = await sheets_client.get_worksheet("Users")
                        loop = asyncio.get_event_loop()
                        await loop.run_in_executor(None, worksheet.delete_rows, i + 1)
                        found = True
                        break
            
            if found:
                self._users_cache = None
                return "✅ Пользователь удален"
            else:
                return "❌ Пользователь не найден"
                
        except Exception as e:
            logger.error(f"Error removing user: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def get_users_list(self) -> str:
        """Список пользователей"""
        try:
            users = await self._get_users_data()
            
            if len(users) <= 1:
                return "📭 Список пользователей пуст"
            
            result = html.bold("👥 Список пользователей") + "\n\n"
            
            for i, user in enumerate(users[1:], start=1):
                if len(user) >= 4:
                    user_id = html.code(user[0] or "N/A")
                    username = user[1] or "Не указано"
                    name = user[2] or "Не указано"
                    role = "👑 Админ" if user[3] == "admin" else "👤 Пользователь"
                    
                    result += f"{i}. ID: {user_id}\n"
                    result += f"   👤: {html.escape(name)}\n"
                    result += f"   📱: {html.escape(username)}\n"
                    result += f"   🏷️: {role}\n\n"
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting users list: {e}")
            return f"❌ Ошибка: {str(e)}"
    
    async def get_stats(self) -> Dict[str, Any]:
        """Статистика системы"""
        stats = {}
        
        try:
            # Статистика базы
            main_data = await sheets_client.get_all_data()
            if main_data:
                stats['database'] = {
                    'records': len(main_data) - 1,
                    'columns': len(main_data[0]) if main_data[0] else 0
                }
            
            # Статистика пользователей
            try:
                users = await self._get_users_data()
                if users:
                    admin_count = sum(1 for u in users[1:] if len(u) >= 4 and u[3] == "admin")
                    user_count = len(users) - 1 - admin_count
                    
                    stats['users'] = {
                        'total': len(users) - 1,
                        'admins': admin_count,
                        'regular': user_count
                    }
            except:
                pass
            
            # Статистика логов
            try:
                logs = await self._get_logs_data()
                if logs:
                    granted = sum(1 for l in logs[1:] if len(l) >= 6 and l[5] in ["GRANTED", "GRANTED_ADMIN"])
                    denied = sum(1 for l in logs[1:] if len(l) >= 6 and l[5] == "DENIED")
                    
                    stats['logs'] = {
                        'total': len(logs) - 1,
                        'granted': granted,
                        'denied': denied
                    }
            except:
                pass
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
        
        return stats
    
    async def _get_users_data(self):
        """Данные пользователей"""
        if self._users_cache is None:
            try:
                self._users_cache = await sheets_client.get_all_data("Users")
            except Exception as e:
                logger.error(f"Error loading users: {e}")
                self._users_cache = []
        
        return self._users_cache
    
    async def _get_logs_data(self):
        """Данные логов"""
        if self._logs_cache is None:
            try:
                self._logs_cache = await sheets_client.get_all_data("AccessLog")
            except Exception as e:
                logger.error(f"Error loading logs: {e}")
                self._logs_cache = []
        
        return self._logs_cache
    
    async def _is_user_in_whitelist(self, user_id: int) -> bool:
        """Проверка белого списка"""
        users = await self._get_users_data()
        
        for user in users[1:]:
            if user and user[0]:
                try:
                    if int(user[0]) == user_id:
                        return True
                except (ValueError, TypeError):
                    continue
        
        return False
    
    async def _log_access(self, user_info: Dict[str, Any], status: str):
        """Логирование доступа"""
        try:
            row_data = [
                datetime.now().isoformat(),
                str(user_info.get('id', '')),
                f"@{user_info.get('username', '')}" if user_info.get('username') else "",
                user_info.get('first_name', ''),
                user_info.get('last_name', ''),
                status
            ]
            
            # Используем append_row из sheets_client для консистентности
            await sheets_client.append_row(row_data, "AccessLog")
            
        except Exception as e:
            logger.error(f"⚠️ Error logging access: {e}")

    async def log_action(self, user_id: int, action: str, details: str = ""):
        """Логирование действий пользователя"""
        try:
            # Получаем инфо о пользователе для лога
            users = await self._get_users_data()
            user_display = str(user_id)
            for user in users[1:]:
                if user and user[0] and str(user[0]) == str(user_id):
                    user_display = f"{user[2]} (@{user[1]})" if user[1] else user[2]
                    break

            row_data = [
                datetime.now().isoformat(),
                str(user_id),
                user_display,
                action,
                details
            ]
            
            await sheets_client.append_row(row_data, "ActionLog")
        except Exception as e:
            logger.error(f"⚠️ Error logging action: {e}")


# Глобальный экземпляр
auth_manager = AuthManager()