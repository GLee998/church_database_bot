"""
Основная логика Telegram бота версии 2.0
"""
import logging
import re
import asyncio
from typing import Dict, Any, Optional, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes
from telegram.error import BadRequest

from app.config import settings
from app.sheets import sheets_client
from app.sessions import session_manager
from app.auth import auth_manager
from app.gemini import gemini
from app.utils import html, formatter, validator

logger = logging.getLogger(__name__)


class TelegramBot:
    """Основной класс бота"""
    
    def __init__(self):
        self.sheets = sheets_client
        self.sessions = session_manager
        self.auth = auth_manager
        self.gemini_ai = gemini

    def _get_update_type(self, update: Update) -> Dict[str, bool]:
        """Определяет тип обновления"""
        return {
            'is_callback': hasattr(update, 'callback_query') and update.callback_query is not None,
            'is_message': hasattr(update, 'message') and update.message is not None,
            'is_edited_message': hasattr(update, 'edited_message') and update.edited_message is not None,
            'is_channel_post': hasattr(update, 'channel_post') and update.channel_post is not None,
            'is_edited_channel_post': hasattr(update, 'edited_channel_post') and update.edited_channel_post is not None,
        }    
    
    # ========== ОБРАБОТЧИКИ КОМАНД ==========
    
    async def handle_start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Приветственное сообщение
        welcome_message = (
            html.bold("🎉 Добро пожаловать в Церковную базу данных!") + "\n\n"
            "Я помогу вам управлять информацией о прихожанах.\n\n"
            "📊 Функции бота:\n"
            "• 🔍 Поиск и просмотр карточек\n"
            "• ✏️ Редактирование информации\n"
            "• ➕ Создание новых записей\n"
            "• 🤖 AI-ассистент для анализа данных\n"
            "• 🛡️ Админ-панель для управления доступом\n\n"
            "Используйте /menu для основного меню"
        )
        
        # Проверка доступа для нового пользователя
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text(
                html.bold("⛔ Доступ запрещен") + "\n\n"
                "У вас нет прав для использования этого бота.\n\n"
                f"Ваш ID: {html.code(str(user_id))}\n"
                "Обратитесь к администратору @Gosha_Lee, чтобы получить доступ.",
                parse_mode='HTML'
            )
            return
        
        await self.sessions.clear_session(chat_id)
        await update.message.reply_text(welcome_message, parse_mode='HTML')
        await self._send_main_menu(update, chat_id, user_id)
    
    async def handle_menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /menu"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text("⛔ У вас нет доступа к боту.")
            return
        
        await self.sessions.clear_session(chat_id)
        await self._send_main_menu(update, chat_id, user_id)
    
    async def handle_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /admin"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка прав администратора
        if not await self.auth.is_admin(user_id):
            await update.message.reply_text("❌ У вас нет прав администратора.")
            return
        
        # Получаем аргументы команды
        args = context.args
        
        if not args:
            # Если команда просто /admin
            await self._show_admin_menu(update, chat_id)
        elif args[0] == 'users':
            await self._show_users_list(update, chat_id)
        elif args[0] == 'logs':
            await self._show_access_logs(update, chat_id)
        elif args[0] == 'stats':
            await self._show_admin_stats(update, chat_id)
        elif args[0] == 'reload':
            await update.message.reply_text("🔄 Обновляю кэш базы данных...")
            try:
                count = await self.sheets.refresh_cache()
                await update.message.reply_text(f"✅ База данных обновлена!\nЗагружено записей: {count}")
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка обновления: {e}")
        elif args[0] == 'add' and len(args) > 1:
            try:
                user_id_to_add = int(args[1])
                user_type = args[2] if len(args) > 2 else "user"
                
                # Получаем информацию о пользователе из Telegram
                try:
                    user_info = await context.bot.get_chat(user_id_to_add)
                    result = await self.auth.add_user(
                        user_id_to_add,
                        user_info.username or "",
                        user_info.first_name or "",
                        user_info.last_name or "",
                        user_type
                    )
                except Exception:
                    # Если не удалось получить инфо, добавляем без нее
                    result = await self.auth.add_user(
                        user_id_to_add,
                        "",
                        "",
                        "",
                        user_type
                    )
                
                await update.message.reply_text(result)
            except ValueError:
                await update.message.reply_text("❌ Неверный формат ID пользователя.")
        elif args[0] == 'remove' and len(args) > 1:
            try:
                user_id_to_remove = int(args[1])
                result = await self.auth.remove_user(user_id_to_remove)
                await update.message.reply_text(result)
            except ValueError:
                await update.message.reply_text("❌ Неверный формат ID пользователя.")
        elif args[0] == 'help':
            await update.message.reply_text(
                html.bold("📋 Доступные команды админа:") + "\n\n"
                f"{html.code('/admin')} - Админ панель\n"
                f"{html.code('/admin users')} - Список пользователей\n"
                f"{html.code('/admin logs')} - Логи доступа\n"
                f"{html.code('/admin stats')} - Статистика\n"
                f"{html.code('/admin reload')} - Обновить базу из Google Sheets\n"
                f"{html.code('/admin add USER_ID [admin/user]')} - Добавить пользователя\n"
                f"{html.code('/admin remove USER_ID')} - Удалить пользователя\n"
                f"{html.code('/admin help')} - Эта справка",
                parse_mode='HTML'
            )
        elif args[0] == 'reload_users':
            await update.message.reply_text("🔄 Обновляю таблицу Users...")
            try:
                count = await self.sheets.refresh_cache("Users")
                self.auth._users_cache = None
                await update.message.reply_text(f"✅ Таблица Users обновлена!\nЗагружено: {count} строк")
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка: {e}")
                
        elif args[0] == 'reload_logs':
            await update.message.reply_text("🔄 Обновляю таблицу AccessLog...")
            try:
                count = await self.sheets.refresh_cache("AccessLog")
                self.auth._logs_cache = None
                await update.message.reply_text(f"✅ Таблица AccessLog обновлена!\nЗагружено: {count} строк")
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка: {e}")
        else:
            await update.message.reply_text(
                "❌ Неизвестная команда. Используйте /admin help для списка команд"
            )

    async def handle_help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_message = (
            html.bold("📚 Справка по командам") + "\n\n"
            f"{html.code('/start')} - Начать работу с ботом\n"
            f"{html.code('/menu')} - Главное меню\n"
            f"{html.code('/help')} - Эта справка\n"
            f"{html.code('/view')} - Поиск и просмотр карточек\n"
            f"{html.code('/edit')} - Редактирование карточек\n"
            f"{html.code('/create')} - Создание новой карточки\n"
            f"{html.code('/ask')} - Задать вопрос AI\n\n"
            f"{html.bold('🛡️ Админ команды:')}" + "\n"
            f"{html.code('/admin')} - Админ панель\n"
            f"{html.code('/admin users')} - Список пользователей\n"
            f"{html.code('/admin stats')} - Статистика\n"
            f"{html.code('/admin logs')} - Логи доступа\n"
            f"{html.code('/admin reload')} - Обновить базу\n\n"
            f"{html.code('/admin reload_users')} - Обновить только пользователей\n"
            f"{html.code('/admin reload_logs')} - Обновить только логи\n"
            "Или используйте кнопки в меню для удобной навигации."
        )
        
        
        await update.message.reply_text(help_message, parse_mode='HTML')
    
    async def handle_view_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /view"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text("⛔ У вас нет доступа к боту.")
            return
        
        session = await self.sessions.get_session(chat_id)
        session['mode'] = 'VIEW_ONLY'
        session['user_id'] = user_id
        await self.sessions.save_session(chat_id, session)
        
        await self._show_alphabet(update, chat_id)
    
    async def handle_edit_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /edit"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text("⛔ У вас нет доступа к боту.")
            return
        
        session = await self.sessions.get_session(chat_id)
        session['mode'] = 'EDIT'
        session['user_id'] = user_id
        await self.sessions.save_session(chat_id, session)
        
        await self._show_alphabet(update, chat_id)
    
    async def handle_create_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /create"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text("⛔ У вас нет доступа к боту.")
            return
        
        await self._start_creation(update, chat_id)
    
    async def handle_ask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /ask"""
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text("⛔ У вас нет доступа к боту.")
            return
        
        # Проверяем есть ли аргументы
        args = context.args
        if args:
            # Если вопрос задан сразу в команде
            question = ' '.join(args)
            
            # Устанавливаем сессию для Gemini
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'GEMINI_QUESTION'
            session['step'] = 'WAITING_QUESTION'
            session['user_id'] = user_id
            await self.sessions.save_session(chat_id, session)
            
            await self._process_gemini_question(update, chat_id, question)
        else:
            # Или переходим в режим вопросов
            await self._start_gemini_question(update, chat_id)
    
    # ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Основной обработчик сообщений"""
        if not update.message or update.message.text.startswith('/'):
            return
        
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        text = update.message.text
        
        logger.info(f"Message from {user_id}: {text}")
        
        # Проверка доступа
        user_info = {
            'id': user_id,
            'username': update.effective_user.username,
            'first_name': update.effective_user.first_name,
            'last_name': update.effective_user.last_name
        }
        
        if not await self.auth.check_access(user_id, user_info):
            await update.message.reply_text(
                html.bold("⛔ Доступ запрещен") + "\n\n"
                "У вас нет прав для использования этого бота.\n\n"
                f"Ваш ID: {html.code(str(user_id))}\n"
                "Обратитесь к администратору @Gosha_Lee, чтобы получить доступ.",
                parse_mode='HTML'
            )
            return
        
        # Получаем сессию
        session = await self.sessions.get_session(chat_id)
        session['user_id'] = user_id
        
        # Обработка текстовых команд
        if text in ('/start', '/menu', 'В главное меню', 'Меню', 'меню'):
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id, user_id)
            return
        
        # Обработка по состоянию сессии
        state = session.get('state', 'IDLE')
        
        if state == 'IDLE':
            await self._handle_idle_state(update, chat_id, text, session)
        elif state == 'ADMIN_MENU':
            await self._handle_admin_menu(update, chat_id, text, session)
        elif state == 'SELECTING_LETTER':
            await self._handle_letter_selection(update, chat_id, text, session)
        elif state == 'SELECTING_PERSON':
            await self._handle_person_selection(update, chat_id, text, session)
        elif state == 'VIEWING_CARD':
            await self._handle_viewing_card(update, chat_id, text, session)
        elif state == 'BUILDER_MODE':
            await self._handle_builder_mode(update, chat_id, text, session)
        elif state == 'GEMINI_QUESTION':
            await self._handle_gemini_question(update, chat_id, text, session)
        elif state == 'OTHER_MENU':
            await self._handle_other_menu(update, chat_id, text, session)
        elif state == 'SELECTING_MONTH':
            await self._handle_month_selection(update, chat_id, text, session)
        elif state == 'SELECTING_HOMEROOM_GROUP':
            await self._handle_homeroom_group_selection(update, chat_id, text, session)
        else:
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback-запросов"""
        query = update.callback_query
        
        try:
            await query.answer()
        except BadRequest:
            logger.warning("Expired callback query")
            return
        
        chat_id = query.message.chat.id
        user_id = update.effective_user.id
        data = query.data
        
        logger.info(f"Callback from {user_id}/{chat_id}: {data}")
        
        session = await self.sessions.get_session(chat_id)
        
        # Убеждаемся, что user_id есть в сессии
        session['user_id'] = user_id
        await self.sessions.save_session(chat_id, session)
        
        # Обработка различных действий
        if data == "back_to_main":
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        
        elif data == "bot_menu":
            await self._show_bot_menu(update, chat_id)

        elif data == "ask_gemini":
            await self._start_gemini_question(update, chat_id)
        
        elif data == "other_menu":
            await self._show_other_menu(update, chat_id)
        
        elif data == "show_birthdays":
            await self._show_month_selection(update, chat_id)
        
        elif data == "show_homeroom_groups":
            await self._show_homeroom_group_selection_menu(update, chat_id)
        
        elif data.startswith("letter_"):
            letter = data.replace("letter_", "")
            await self._show_people_by_letter(update, chat_id, letter)
        
        elif data.startswith("person_"):
            row_index = int(data.replace("person_", ""))
            
            if session.get('mode') == 'VIEW_ONLY':
                await self._show_read_only_card(update, chat_id, row_index)
            elif session.get('mode') == 'EDIT':
                await self._start_editing(update, chat_id, row_index)
        
        elif data == "back_to_letters":
            await self._show_alphabet(update, chat_id)
        
        elif data == "back_to_people":
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
            else:
                await self._show_alphabet(update, chat_id)
        
        elif data == "view":
            session['mode'] = 'VIEW_ONLY'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        
        elif data == "edit":
            session['mode'] = 'EDIT'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        
        elif data == "create":
            await self._start_creation(update, chat_id)
        
        elif data == "admin_panel":
            if not await self.auth.is_admin(user_id):
                await query.edit_message_text("❌ У вас нет прав администратора.")
                return
            await self._show_admin_menu(update, chat_id)
        
        elif data == "admin_users":
            await self._show_users_list(update, chat_id)
        
        elif data == "admin_stats":
            await self._show_admin_stats(update, chat_id)
        
        elif data == "admin_logs":
            await self._show_access_logs(update, chat_id)
        
        elif data == "admin_reload":
            await self._reload_database(update, chat_id)
        
        elif data == "admin_gemini_stats":
            await self._show_gemini_stats(update, chat_id)
        
        elif data == "admin_add_user":
            await self._ask_add_user(update, chat_id)
        
        elif data == "admin_remove_user":
            await self._ask_remove_user(update, chat_id)
        
        elif data == "back_to_admin":
            await self._show_admin_menu(update, chat_id)

        elif data == "back_to_other":
            await self._show_other_menu(update, chat_id)
        
        elif data.startswith("edit_field_"):
            field_name = data.replace("edit_field_", "")
            
            # Если это поле "Домашка", показываем кнопки выбора
            if field_name == settings.col_homeroom:
                await self._show_homeroom_selection_for_edit(update, chat_id, field_name)
                return
            
            # Если это поле "Статус", показываем кнопки выбора
            if field_name == settings.col_status:
                await self._show_status_selection_for_edit(update, chat_id, field_name)
                return
            
            session['step'] = 'WAITING_VALUE'
            session['current_field'] = field_name
            await self.sessions.save_session(chat_id, session)
            
            current_value = session['draft'].get(field_name, "")
            if field_name in settings.date_columns and current_value:
                current_value = self.sheets.format_date(current_value)
            
            message = f"Введите значение для {html.bold(field_name)}:\n"
            if field_name in settings.date_columns:
                message += "Формат: ДД.ММ.ГГГГ (например: 04.05.1998)\n"
            if current_value:
                message += f"(Текущее: {html.escape(str(current_value))})"
            
            await query.edit_message_text(message, parse_mode='HTML')
        
        elif data == "add_category":
            session['step'] = 'WAITING_NEW_CAT'
            await self.sessions.save_session(chat_id, session)
            await query.edit_message_text("Напишите название новой категории:")
        
        elif data == "save_card":
            await self._save_card(update, chat_id, session)
        
        elif data == "cancel_builder":
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        
        else:
            # Проверяем колбэки для выбора группы Домашки (самый специфичный префикс)
            if data.startswith("select_homeroom_group_"):
                group_name = data.replace("select_homeroom_group_", "")
                await self._show_people_by_homeroom(update, chat_id, group_name)
                return

            # Проверяем колбэки для выбора Домашки (менее специфичный)
            if data.startswith("select_homeroom_"):
                await self._handle_homeroom_selection_callback(update, chat_id, data)
                return
            
            # Проверяем колбэки для выбора Статуса
            if data.startswith("select_status_"):
                await self._handle_status_selection_callback(update, chat_id, data)
                return
            
            # Проверяем колбэки для выбора месяца (ДР)
            if data.startswith("select_month_"):
                month_num = int(data.replace("select_month_", ""))
                await self._show_birthdays_by_month(update, chat_id, month_num)
                return
            
            await query.edit_message_text("Неизвестная команда")
    
    # ========== ОСНОВНЫЕ МЕТОДЫ БОТА ==========

    async def _show_bot_menu(self, update: Update, chat_id: int):
        """Показать дополнительное меню бота"""
        keyboard = [
            [InlineKeyboardButton("🔍 Найти / Просмотреть", callback_data="view")],
            [InlineKeyboardButton("✏️ Редактировать карточку", callback_data="edit")],
            [InlineKeyboardButton("➕ Создать карточку", callback_data="create")],
            [InlineKeyboardButton("🤖 Задать вопрос AI", callback_data="ask_gemini")],
            [InlineKeyboardButton("⭐ Остальное", callback_data="other_menu")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = html.bold("🤖 Меню бота") + "\nВыберите действие:"
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def _send_main_menu(self, update: Update, chat_id: int, user_id: Optional[int] = None):
        """Отправка главного меню"""
        session = await self.sessions.get_session(chat_id)
        
        # Используем user_id из аргумента, если передан, иначе из сессии
        if user_id is None:
            user_id = session.get('user_id', 0)
        
        keyboard = []
        
        # Добавляем кнопку Mini App, если URL настроен
        if settings.webapp_url:
            keyboard.append([InlineKeyboardButton("🔐 Войти в базу данных", web_app=WebAppInfo(url=settings.webapp_url))])
            
        keyboard.append([InlineKeyboardButton("🤖 Меню бота", callback_data="bot_menu")])
        
        # Добавляем админ-панель для администраторов
        if await self.auth.is_admin(user_id):
            keyboard.append([InlineKeyboardButton("🛡️ Админ панель", callback_data="admin_panel")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session['state'] = 'IDLE'
        await self.sessions.save_session(chat_id, session)
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                html.bold("⛪ Церковная база данных") + "\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                html.bold("⛪ Церковная база данных") + "\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def _show_alphabet(self, update: Update, chat_id: int):
        """Показать алфавит для поиска"""
        try:
            data = await self.sheets.get_all_data()
            headers = data[0] if data else []
            name_index = headers.index(settings.col_first_name) if settings.col_first_name in headers else -1
            
            if name_index == -1:
                error_msg = f"⚠️ Ошибка: Нет колонки '{settings.col_first_name}'"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            # Собираем буквы
            letters = set()
            for row in data[1:]:
                if name_index < len(row):
                    name = row[name_index]
                    if name and isinstance(name, str):
                        first_char = name[0].upper()
                        if re.match(r'[А-ЯA-Z]', first_char):
                            letters.add(first_char)
            
            if not letters:
                msg = "В базе нет данных. Создайте первую карточку."
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(msg)
                else:
                    await update.message.reply_text(msg)
                await self.sessions.clear_session(chat_id)
                await self._send_main_menu(update, chat_id)
                return
            
            # Создаем клавиатуру
            sorted_letters = sorted(letters)
            keyboard = []
            row = []
            
            for letter in sorted_letters:
                row.append(InlineKeyboardButton(letter, callback_data=f"letter_{letter}"))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'SELECTING_LETTER'
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    "🔤 Выберите первую букву имени:",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    "🔤 Выберите первую букву имени:",
                    reply_markup=reply_markup
                )
                
        except Exception as e:
            logger.error(f"Error showing alphabet: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_people_by_letter(self, update: Update, chat_id: int, letter: str):
        """Показать людей на выбранную букву"""
        try:
            data = await self.sheets.get_all_data()
            headers = data[0] if data else []
            
            name_idx = headers.index(settings.col_first_name) if settings.col_first_name in headers else -1
            surname_idx = headers.index(settings.col_last_name) if settings.col_last_name in headers else -1
            birth_idx = headers.index(settings.col_birth_date) if settings.col_birth_date in headers else -1
            
            if name_idx == -1:
                error_msg = "❌ Ошибка: не найдена колонка с именами"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            # Собираем людей
            people = []
            name_counts = {}
            
            # Считаем тезок
            for i, row in enumerate(data[1:], start=2):
                if name_idx < len(row):
                    name = str(row[name_idx] or "").strip()
                    if name and name.upper().startswith(letter.upper()):
                        surname = str(row[surname_idx] or "").strip() if surname_idx != -1 and surname_idx < len(row) else ""
                        key = f"{name.lower()}_{surname.lower()}"
                        name_counts[key] = name_counts.get(key, 0) + 1
            
            # Формируем список
            for i, row in enumerate(data[1:], start=2):
                if name_idx < len(row):
                    name = str(row[name_idx] or "").strip()
                    if name and name.upper().startswith(letter.upper()):
                        surname = str(row[surname_idx] or "").strip() if surname_idx != -1 and surname_idx < len(row) else ""
                        key = f"{name.lower()}_{surname.lower()}"
                        
                        # Формируем отображаемое имя
                        display_name = f"{name} {surname}".strip()
                        
                        # Добавляем дату рождения если есть тезки
                        if name_counts.get(key, 0) > 1 and birth_idx != -1 and birth_idx < len(row) and row[birth_idx]:
                            birth_date = self.sheets.format_date(row[birth_idx])
                            if birth_date:
                                display_name = f"{name} {surname} (р. {birth_date})"
                        
                        people.append({
                            'text': display_name,
                            'row': i,
                            'display': f"{display_name} [#{i}]"
                        })
            
            if not people:
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(f"Нет имен на букву {letter}")
                else:
                    await update.message.reply_text(f"Нет имен на букву {letter}")
                await self._show_alphabet(update, chat_id)
                return
            
            # Создаем клавиатуру
            keyboard = []
            for person in people:
                keyboard.append([InlineKeyboardButton(person['display'], callback_data=f"person_{person['row']}")])
            
            keyboard.append([InlineKeyboardButton("⬅️ Назад к буквам", callback_data="back_to_letters")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'SELECTING_PERSON'
            session['last_letter'] = letter
            session['people_list'] = people
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    "👤 Выберите человека:",
                    reply_markup=reply_markup
                )
            else:
                await update.message.reply_text(
                    "👤 Выберите человека:",
                    reply_markup=reply_markup
                )
                
        except Exception as e:
            logger.error(f"Error showing people by letter: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_read_only_card(self, update: Update, chat_id: int, row_index: int):
        """Показать карточку только для чтения"""
        try:
            data = await self.sheets.get_all_data()
            if row_index > len(data):
                error_msg = "❌ Запись не найдена"
                if hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_msg)
                else:
                    await update.message.reply_text(error_msg)
                return
            
            headers = data[0]
            row_data = data[row_index - 1]
            
            message = html.bold("📋 Информация о прихожанине:") + "\n\n"
            has_data = False
            
            for i, header in enumerate(headers):
                if i < len(row_data):
                    value = row_data[i]
                    if value and str(value).strip():
                        # Форматируем дату если нужно
                        if header in settings.date_columns:
                            value = self.sheets.format_date(value)
                        
                        message += f"🔹 {html.bold(header)}: {html.escape(str(value))}\n"
                        has_data = True
            
            if not has_data:
                message += "(Нет данных)"
            
            keyboard = [
                [InlineKeyboardButton("⬅️ К списку имен", callback_data="back_to_people")],
                [InlineKeyboardButton("🏠 В главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сессию
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'VIEWING_CARD'
            session['viewing_row'] = row_index
            await self.sessions.save_session(chat_id, session)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing card: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _start_creation(self, update: Update, chat_id: int):
        """Начать создание новой карточки"""
        session = {
            'state': 'BUILDER_MODE',
            'mode': 'CREATE',
            'draft': {},
            'step': 'MENU',
            'editing_row': None
        }
        await self.sessions.save_session(chat_id, session)
        await self._show_builder_menu(update, chat_id, session)
    
    async def _start_editing(self, update: Update, chat_id: int, row_index: int):
        """Начать редактирование существующей карточки"""
        try:
            data = await self.sheets.get_all_data()
            if row_index > len(data):
                await update.callback_query.edit_message_text("❌ Запись не найдена")
                return
            
            headers = data[0]
            row_data = data[row_index - 1]
            
            # Создаем черновик из текущих данных
            draft = {}
            for i, header in enumerate(headers):
                if i < len(row_data) and row_data[i] and str(row_data[i]).strip():
                    draft[header] = row_data[i]
            
            session = await self.sessions.get_session(chat_id)
            session['state'] = 'BUILDER_MODE'
            session['mode'] = 'EDIT'
            session['draft'] = draft
            session['step'] = 'MENU'
            session['editing_row'] = row_index
            await self.sessions.save_session(chat_id, session)
            
            await self._show_builder_menu(update, chat_id, session)
            
        except Exception as e:
            logger.error(f"Error starting edit: {e}")
            await update.callback_query.edit_message_text(f"❌ Ошибка: {e}")
    
    async def _show_selection_menu_for_edit(self, update: Update, chat_id: int, field_name: str, values: List[str]):
        """Универсальный метод для показа кнопок выбора (Домашка, Статус)"""
        session = await self.sessions.get_session(chat_id)
        
        # Определяем префикс для callback_data
        if field_name == settings.col_homeroom:
            prefix = "select_homeroom_"
        elif field_name == settings.col_status:
            prefix = "select_status_"
        else:
            return # Should not happen
            
        # Создаем клавиатуру с кнопками
        keyboard = []
        row = []
        for i, value in enumerate(values):
            callback_data = f"{prefix}{i}"
            row.append(InlineKeyboardButton(value, callback_data=callback_data))
            
            # Размещаем по 2 кнопки в ряд
            if len(row) == 2:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
            
        # Добавляем кнопку "Назад"
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_builder_menu")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        current_value = session['draft'].get(field_name, "Не выбрано")
        
        message = (
            html.bold(f"Выберите значение для '{field_name}':") + "\n\n"
            f"(Текущее: {html.escape(str(current_value))})"
        )
        
        await update.callback_query.edit_message_text(
            message,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        session['step'] = f'WAITING_{field_name.upper()}_SELECTION'
        session['current_field'] = field_name
        await self.sessions.save_session(chat_id, session)

    async def _show_homeroom_selection_for_edit(self, update: Update, chat_id: int, field_name: str):
        """Показать кнопки для выбора Домашки (в режиме редактирования)"""
        await self._show_selection_menu_for_edit(update, chat_id, field_name, settings.homeroom_values)
        
    async def _show_status_selection_for_edit(self, update: Update, chat_id: int, field_name: str):
        """Показать кнопки для выбора Статуса (в режиме редактирования)"""
        await self._show_selection_menu_for_edit(update, chat_id, field_name, settings.status_values)
        

    async def _handle_selection_callback(self, update: Update, chat_id: int, data: str, values: List[str], field_name: str, prefix: str):
        """Обработка выбора из списка (Домашка/Статус)"""
        session = await self.sessions.get_session(chat_id)
        
        if data == "back_to_builder_menu":
            session['step'] = 'MENU'
            await self.sessions.save_session(chat_id, session)
            await self._show_builder_menu(update, chat_id, session)
            return
            
        try:
            # Извлекаем индекс из callback_data
            index_str = data.replace(prefix, "")
            index = int(index_str)
            
            if 0 <= index < len(values):
                selected_value = values[index]
                
                # Сохраняем выбранное значение
                session['draft'][field_name] = selected_value
                session['step'] = 'MENU'
                session['current_field'] = None
                
                await self.sessions.save_session(chat_id, session)
                
                # Возвращаемся в меню конструктора
                await self._show_builder_menu(update, chat_id, session)
            else:
                await update.callback_query.edit_message_text(f"❌ Неверный выбор для {field_name}.")
                
        except Exception as e:
            logger.error(f"Error handling selection: {e}")
            await update.callback_query.edit_message_text(f"❌ Ошибка обработки выбора: {e}")

    async def _handle_homeroom_selection_callback(self, update: Update, chat_id: int, data: str):
        """Обработка выбора Домашки в режиме редактирования"""
        await self._handle_selection_callback(update, chat_id, data, settings.homeroom_values, settings.col_homeroom, "select_homeroom_")

    async def _handle_status_selection_callback(self, update: Update, chat_id: int, data: str):
        """Обработка выбора Статуса в режиме редактирования"""
        await self._handle_selection_callback(update, chat_id, data, settings.status_values, settings.col_status, "select_status_")
        
    
    async def _show_builder_menu(self, update: Update, chat_id: int, session: Dict[str, Any]):
        """Показать меню конструктора"""
        try:
            headers = await self.sheets.get_headers()
            keyboard = []
            
            for header in headers:
                label = header
                if header in session['draft']:
                    value = session['draft'][header]
                    if header in settings.date_columns:
                        value = self.sheets.format_date(value)
                    label = f"✅ {header}: {html.escape(str(value))}"
                
                keyboard.append([InlineKeyboardButton(label, callback_data=f"edit_field_{header}")])
            
            keyboard.append([InlineKeyboardButton("➕ Доб. категорию", callback_data="add_category")])
            keyboard.append([
                InlineKeyboardButton("💾 СОХРАНИТЬ", callback_data="save_card"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel_builder")
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            mode_text = "создания" if session['mode'] == 'CREATE' else "редактирования"
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    html.bold(f"📝 Режим {mode_text}") + "\nНажмите на категорию, чтобы изменить её:",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    html.bold(f"📝 Режим {mode_text}") + "\nНажмите на категорию, чтобы изменить её:",
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing builder menu: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _save_card(self, update: Update, chat_id: int, session: Dict[str, Any]):
        """Сохранение карточки"""
        try:
            headers = await self.sheets.get_headers()
            row_data = []
            
            for header in headers:
                value = session['draft'].get(header, "")
                
                # Форматируем даты для Google Sheets
                if header in settings.date_columns and value:
                    # Если дата в формате ДД.ММ.ГГГГ, конвертируем
                    if isinstance(value, str) and re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', value):
                        try:
                            day, month, year = map(int, value.split('.'))
                            value = f"{year}-{month:02d}-{day:02d}"
                        except:
                            pass
                
                row_data.append(value)
            
            if session['mode'] == 'CREATE':
                await self.sheets.append_row(row_data)
                message = "✅ Карточка успешно создана!"
            else:
                row_index = session['editing_row']
                await self.sheets.update_row(row_index, row_data)
                message = "✅ Данные обновлены!"
            
            await self.sessions.clear_session(chat_id)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(message)
                await self._send_main_menu(update, chat_id)
                
        except Exception as e:
            logger.error(f"Error saving card: {e}")
            error_msg = f"❌ Ошибка при сохранении: {e}"
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    # ========== GEMINI AI МЕТОДЫ ==========
    
    async def _start_gemini_question(self, update: Update, chat_id: int):
        """Начать диалог с Gemini AI"""
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'GEMINI_QUESTION'
        session['step'] = 'WAITING_QUESTION'
        await self.sessions.save_session(chat_id, session)
        
        message = (
            html.bold("🤖 AI Ассистент") + "\n\n"
            "Задайте вопрос о данных в таблице.\n"
            "Например:\n"
            "• Сколько всего записей в базе?\n"
            "• Кто родился в мае?\n"
            "• Покажи всех с фамилией Цой\n"
            "• Сколько человек приняли крещение в 2025 году?\n"
            "• Сколько человек старше 60 лет?\n\n"
            "Отправьте ваш вопрос или /menu для выхода:"
        )
        
        # Определяем тип обновления
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(message, parse_mode='HTML')
        elif hasattr(update, 'message') and update.message:
            await update.message.reply_text(message, parse_mode='HTML')
        else:
            # Резервный вариант
            logger.warning(f"No message or callback in update: {update}")
    
    async def _process_gemini_question(self, update: Update, chat_id: int, question: str):
        """Обработка вопроса к Gemini AI"""
        try:
            # Определяем тип обновления
            is_callback = hasattr(update, 'callback_query') and update.callback_query is not None
            is_message = hasattr(update, 'message') and update.message is not None
            
            # Показываем индикатор обработки
            processing_text = "🤔 Анализирую ваш вопрос..."
            
            msg = None
            if is_callback:
                # Обработка callback запроса
                await update.callback_query.edit_message_text(processing_text)
                msg = update.callback_query.message
            elif is_message:
                # Обработка обычного сообщения
                msg = await update.message.reply_text(processing_text)
            else:
                # Неизвестный тип обновления
                logger.error(f"Unknown update type for Gemini: {update}")
                return
            
            # Получаем данные из таблицы
            headers = await self.sheets.get_headers()
            all_data = await self.sheets.get_all_data()
            
            # Проверяем, есть ли данные
            if not all_data or len(all_data) <= 1:
                response_text = "📭 База данных пуста или содержит только заголовки."
                if is_callback:
                    await update.callback_query.edit_message_text(response_text)
                else:
                    await msg.edit_text(response_text)
                return
            
            # Извлекаем данные (без заголовков)
            data = all_data[1:]  # Пропускаем заголовки
            
            # Логируем для отладки
            logger.info(f"Processing Gemini question: {question}")
            logger.info(f"Headers: {len(headers)} columns")
            logger.info(f"Data rows: {len(data)}")
            
            # Проверяем инициализацию Gemini
            if not self.gemini_ai.initialized:
                try:
                    await msg.edit_text("🔄 Инициализирую AI...")
                    await self.gemini_ai.initialize()
                except Exception as init_error:
                    logger.error(f"Gemini init error: {init_error}")
                    error_text = f"❌ Не удалось инициализировать AI"
                    if is_callback:
                        await update.callback_query.edit_message_text(error_text)
                    else:
                        await msg.edit_text(error_text)
                    return
            
            # Отправляем вопрос в Gemini
            await msg.edit_text("🧠 Обрабатываю данные...")
            
            try:
                # Анализируем через Gemini AI
                answer = await self.gemini_ai.analyze_table(question, headers, data)
                
                # Формируем ответ
                response = html.bold("🤖 Ответ AI:") + f"\n\n{answer}\n\n"
                response += "Задайте еще вопрос или /menu для выхода"
                
                if is_callback:
                    await update.callback_query.edit_message_text(response, parse_mode='HTML')
                else:
                    await msg.edit_text(response, parse_mode='HTML')
                
                # Обновляем сессию для продолжения диалога
                session = await self.sessions.get_session(chat_id)
                session['state'] = 'GEMINI_QUESTION'
                session['step'] = 'WAITING_QUESTION'
                await self.sessions.save_session(chat_id, session)
                
            except Exception as gemini_error:
                logger.error(f"Gemini analysis error: {gemini_error}")
                
                # Fallback: простой ответ на основе данных
                if "сколько" in question.lower() or "количество" in question.lower():
                    fallback = f"📊 Всего записей в базе: {len(data)}"
                elif "столбц" in question.lower() or "колонк" in question.lower():
                    fallback = f"🏷️ Количество колонок: {len(headers)}"
                else:
                    fallback = "🤖 Не удалось получить ответ от AI. Попробуйте другой вопрос."
                
                if is_callback:
                    await update.callback_query.edit_message_text(fallback)
                else:
                    await msg.edit_text(fallback)
                
        except Exception as e:
            logger.error(f"Gemini processing error: {e}", exc_info=True)
            
            # Отправляем ошибку в чат
            try:
                error_text = f"❌ Ошибка обработки. Попробуйте еще раз."
                
                if hasattr(update, 'message') and update.message:
                    await update.message.reply_text(error_text)
                elif hasattr(update, 'callback_query') and update.callback_query:
                    await update.callback_query.edit_message_text(error_text)
            except:
                pass  # Игнорируем ошибки при отправке ошибки
    
    async def _handle_gemini_question(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка вопроса к Gemini AI в режиме диалога"""
        if session.get('step') == 'WAITING_QUESTION':
            if text.lower() in ('/menu', 'меню', 'отмена', 'назад', '/start', '/help'):
                await self.sessions.clear_session(chat_id)
                await self._send_main_menu(update, chat_id)
                return
            
            # Обрабатываем вопрос
            await self._process_gemini_question(update, chat_id, text)
        else:
            # Если не в режиме ожидания вопроса, возвращаем в главное меню
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
    
    # ========== АДМИН МЕТОДЫ ==========
    
    async def _show_admin_menu(self, update: Update, chat_id: int):
        """Показать админ-меню"""
        keyboard = [
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("📋 Логи доступа", callback_data="admin_logs")],
            [InlineKeyboardButton("🤖 Статистика AI", callback_data="admin_gemini_stats")],
            [InlineKeyboardButton("➕ Добавить пользователя", callback_data="admin_add_user")],
            [InlineKeyboardButton("➖ Удалить пользователя", callback_data="admin_remove_user")],
            [InlineKeyboardButton("🔄 Обновить базу", callback_data="admin_reload")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'ADMIN_MENU'
        await self.sessions.save_session(chat_id, session)
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                html.bold("🛡️ Админ панель") + "\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                html.bold("🛡️ Админ панель") + "\nВыберите действие:",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def _show_users_list(self, update: Update, chat_id: int):
        """Показать список пользователей"""
        try:
            users_list = await self.auth.get_users_list()
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    users_list,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    users_list,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing users list: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_admin_stats(self, update: Update, chat_id: int):
        """Показать статистику"""
        try:
            stats = await self.auth.get_stats()
            
            message = html.bold("📊 Статистика системы") + "\n\n"
            
            if 'database' in stats:
                message += html.bold("📁 База данных:") + "\n"
                message += f"   📝 Записей: {stats['database'].get('records', 0)}\n"
                message += f"   🏷️ Категорий: {stats['database'].get('columns', 0)}\n\n"
            
            if 'users' in stats:
                message += html.bold("👥 Пользователи:") + "\n"
                message += f"   👑 Админов: {stats['users'].get('admins', 0)}\n"
                message += f"   👤 Пользователей: {stats['users'].get('regular', 0)}\n"
                message += f"   👥 Всего: {stats['users'].get('total', 0)}\n\n"
            
            if 'logs' in stats:
                message += html.bold("📋 Логи доступа:") + "\n"
                message += f"   ✅ Успешных: {stats['logs'].get('granted', 0)}\n"
                message += f"   ❌ Отказов: {stats['logs'].get('denied', 0)}\n"
                message += f"   📊 Всего: {stats['logs'].get('total', 0)}\n"
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing admin stats: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_access_logs(self, update: Update, chat_id: int):
        """Показать логи доступа"""
        try:
            logs_data = await self.sheets.get_all_data("AccessLog")
            
            if not logs_data or len(logs_data) <= 1:
                message = "📭 Логи доступа отсутствуют."
            else:
                message = html.bold("📋 Последние 10 попыток доступа") + "\n\n"
                
                # Берем последние 10 записей
                start = max(1, len(logs_data) - 10)
                
                for i in range(start, len(logs_data)):
                    log = logs_data[i]
                    try:
                        from datetime import datetime
                        date_str = log[0]
                        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        formatted_date = date_obj.strftime("%d.%m.%y %H:%M")
                        
                        message += html.bold(f"{formatted_date}") + "\n"
                        message += f"ID: {html.code(log[1] if len(log) > 1 else 'N/A')}\n"
                        message += f"Имя: {log[3] if len(log) > 3 else 'Не указано'}\n"
                        status = log[5] if len(log) > 5 else ""
                        message += f"Статус: {'❌ Отказано' if status == 'DENIED' else '✅ Разрешено'}\n"
                        message += "---\n"
                    except:
                        continue
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"Error showing access logs: {e}")
            error_msg = f"❌ Ошибка: {e}"
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
    
    async def _show_gemini_stats(self, update: Update, chat_id: int):
        """Показать статистику Gemini AI"""
        try:
            # Инициализируем Gemini если не инициализирован
            await self.gemini_ai.initialize()
            
            # Получаем данные для анализа
            headers = await self.sheets.get_headers()
            data = await self.sheets.get_all_data()
            
            await update.callback_query.edit_message_text("🤖 Анализирую таблицу...")
            
            # Получаем краткий анализ таблицы
            analysis = await self.gemini_ai.get_table_summary(headers, data[1:] if len(data) > 1 else [])
            
            message = (
                html.bold("📊 Анализ таблицы AI") + "\n\n"
                f"{analysis}\n\n"
                f"📈 Общее количество записей: {html.bold(str(len(data) - 1))}\n"
                f"🏷️ Количество категорий: {html.bold(str(len(headers)))}\n\n"
                "AI готов отвечать на вопросы о данных!"
            )
            
            keyboard = [
                [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
        except Exception as e:
            logger.error(f"Gemini stats error: {e}")
            await update.callback_query.edit_message_text(
                f"❌ Ошибка при анализе: {str(e)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
                ])
            )
    
    async def _reload_database(self, update: Update, chat_id: int):
        """Обновление базы данных"""
        await update.callback_query.edit_message_text("🔄 Обновляю ВСЕ таблицы...")
        
        try:
            # Обновляем все таблицы
            count = await self.sheets.refresh_cache()  # Без параметра = все листы
            
            # Явно сбрасываем кэш пользователей и логов
            self.auth._users_cache = None
            self.auth._logs_cache = None
            
            # Принудительно загружаем свежие данные
            await self.auth._get_users_data()
            
            await update.callback_query.edit_message_text(
                f"✅ Все таблицы обновлены!\n"
                f"Загружено строк: {count}\n\n"
                f"✅ Основная таблица\n"
                f"✅ Таблица Users\n"
                f"✅ Таблица AccessLog",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад в админ-панель", callback_data="back_to_admin")]
                ])
            )
            
        except Exception as e:
            await update.callback_query.edit_message_text(
                f"❌ Ошибка обновления: {str(e)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
                ])
            )
    
    async def _ask_add_user(self, update: Update, chat_id: int):
        """Запрос на добавление пользователя"""
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'ADMIN_MENU'
        session['step'] = 'WAITING_USER_ID_FOR_ADD'
        await self.sessions.save_session(chat_id, session)
        
        await update.callback_query.edit_message_text(
            "Введите ID пользователя для добавления (число):\n\n"
            "Можно получить ID через @userinfobot\n\n"
            "Формат: 123456789\n"
            "Или с указанием роли: 123456789 admin",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
            ])
        )
    
    async def _ask_remove_user(self, update: Update, chat_id: int):
        """Запрос на удаление пользователя"""
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'ADMIN_MENU'
        session['step'] = 'WAITING_USER_ID_FOR_REMOVE'
        await self.sessions.save_session(chat_id, session)
        
        await update.callback_query.edit_message_text(
            "Введите ID пользователя для удаления (число):",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin")]
            ])
        )
    
    # ========== МЕТОДЫ "ОСТАЛЬНОЕ" ==========
 
    async def _get_month_name(self, month_number: int) -> str:
        """Возвращает название месяца на русском"""
        month_names = {
            1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
            5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
            9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
        }
        return month_names.get(month_number, f"Месяц {month_number}")
    
    async def _show_other_menu(self, update: Update, chat_id: int):
        """Показать меню 'Остальное'"""
        keyboard = [
            [InlineKeyboardButton("🏠 Домашки", callback_data="show_homeroom_groups")],
            [InlineKeyboardButton("🎂 Дни рождения", callback_data="show_birthdays")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")] # Main menu is the back action from here
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = html.bold("⭐ Остальное") + "\nВыберите действие:"
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'OTHER_MENU'
        await self.sessions.save_session(chat_id, session)

    # =========================================================================
    # Задача 1: Промежуточное меню для Дней Рождения (12 месяцев)
    # =========================================================================
    async def _show_month_selection(self, update: Update, chat_id: int):
        """Показать кнопки выбора месяца для Дней Рождения"""
        
        keyboard = []
        months = [
            (1, "Январь"), (2, "Февраль"), (3, "Март"), (4, "Апрель"),
            (5, "Май"), (6, "Июнь"), (7, "Июль"), (8, "Август"),
            (9, "Сентябрь"), (10, "Октябрь"), (11, "Ноябрь"), (12, "Декабрь")
        ]
        
        row = []
        for month_num, month_name in months:
            row.append(InlineKeyboardButton(month_name, callback_data=f"select_month_{month_num}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
            
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_other")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = html.bold("🎂 Дни рождения") + "\n\nВыберите месяц:"
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
             await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'SELECTING_MONTH'
        await self.sessions.save_session(chat_id, session)

    async def _show_birthdays_by_month(self, update: Update, chat_id: int, month_num: int):
        """Показать дни рождения для выбранного месяца"""
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text("⏳ Загружаю дни рождения...")
        
        all_birthdays_data = await self.sheets.get_birthdays_data_by_month()
        birthdays_for_month = all_birthdays_data.get(month_num, [])
        month_name = await self._get_month_name(month_num)
        
        if not birthdays_for_month:
            message = html.bold(f"🎂 Дни рождения в {month_name}") + "\n\n" + "В этом месяце дней рождения нет."
        else:
            message = html.bold(f"🎂 Дни рождения в {month_name}") + "\n\n"
            
            for person in birthdays_for_month:
                name = person['name']
                day = person['day']
                year = person['year']
                row_index = person['row_index']
                
                year_str = f"({year} г.)" if year and year != 1900 else ""
                
                message += f"   • {day:02d}. {html.escape(name)} {year_str} [#{row_index}]\n"
                
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад к месяцам", callback_data="show_birthdays")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'SELECTING_MONTH' # Остаемся в режиме ДР
        await self.sessions.save_session(chat_id, session)
        
        try:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            elif hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
        except BadRequest as e:
            logger.warning(f"Message too long for callback edit: {e}")
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            elif hasattr(update, 'message') and update.message:
                pass
            
    # =========================================================================
    # Задача 1/4: Промежуточное меню для Домашек (15+ кнопок) и модификация вывода
    # =========================================================================

    async def _show_homeroom_group_selection_menu(self, update: Update, chat_id: int):
        """Показать кнопки выбора группы Домашки"""
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text("⏳ Загружаю список Домашек...")
        
        # Получаем актуальный список групп (включая группы не из конфига, и данные для вывода)
        # get_people_by_homeroom уже обновлен, чтобы включать возраст и статус.
        all_groups_data = await self.sheets.get_people_by_homeroom()
        group_names = sorted(all_groups_data.keys())
        
        keyboard = []
        row = []
        for group_name in group_names:
            people_count = len(all_groups_data[group_name])
            button_text = f"{group_name} ({people_count} чел.)"
            
            # Используем group_name как callback_data, так как он уникален
            row.append(InlineKeyboardButton(button_text, callback_data=f"select_homeroom_group_{group_name}"))
            
            if len(row) == 2:
                keyboard.append(row)
                row = []
        
        if row:
            keyboard.append(row)
            
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_other")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = html.bold("🏠 Домашки") + "\n\nВыберите группу для просмотра:"
        
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        
        session = await self.sessions.get_session(chat_id)
        session['state'] = 'SELECTING_HOMEROOM_GROUP'
        session['homeroom_groups_data'] = all_groups_data # Кэшируем данные, чтобы не загружать их повторно
        await self.sessions.save_session(chat_id, session)

    async def _show_people_by_homeroom(self, update: Update, chat_id: int, group_name: str):
        """Показать список людей в выбранной Домашней группе (с возрастом и статусом)"""
        
        session = await self.sessions.get_session(chat_id)
        
        # Попытка получить данные из сессии (если они были загружены для меню выбора)
        homeroom_groups = session.get('homeroom_groups_data')
        
        # Если данных нет в сессии (например, после рестарта бота), загружаем их
        if not homeroom_groups:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text("⏳ Загружаю данные...")
            homeroom_groups = await self.sheets.get_people_by_homeroom()
        
        people = homeroom_groups.get(group_name, [])
        
        if not people:
            message = html.bold(f"🏠 Домашка: {group_name}") + "\n\n" + "В этой группе нет записей."
        else:
            message = html.bold(f"🏠 Люди в группе: {group_name} ({len(people)} чел.)") + "\n\n"
            
            for person in people:
                name = person['name']
                age_str = person['age_str']
                status = person['status']
                
                # Формат вывода: Имя Фамилия (Возраст, Статус) - согласно задаче 4
                details = []
                if age_str != 'Н/Д':
                    details.append(age_str)
                if status:
                    details.append(status)
                    
                details_str = f" ({', '.join(details)})" if details else ""
                
                message += f"   • {html.escape(name)}{details_str}\n"
        
        keyboard = [
            [InlineKeyboardButton("⬅️ Назад к Домашкам", callback_data="show_homeroom_groups")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Обновляем сессию
        session['state'] = 'SELECTING_HOMEROOM_GROUP'
        await self.sessions.save_session(chat_id, session)
        
        try:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            elif hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
        except BadRequest as e:
            logger.warning(f"Message too long for callback edit: {e}")
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    message,
                    reply_markup=reply_markup,
                    parse_mode='HTML'
                )
            elif hasattr(update, 'message') and update.message:
                pass


    async def _handle_other_menu(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка меню 'Остальное'"""
        # Если в меню Остальное вводится текст, который совпадает с названием кнопки,
        # нужно, чтобы он работал (хотя мы ожидаем инлайн кнопки, это для совместимости)
        if 'Дни рождения' in text:
            await self._show_month_selection(update, chat_id)
        elif 'Домашки' in text:
            await self._show_homeroom_group_selection_menu(update, chat_id)
        elif 'Назад' in text:
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        else:
            await self._show_other_menu(update, chat_id)
            
    async def _handle_month_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка текстового ввода в режиме выбора месяца (игнорируем)"""
        # Так как это меню на inline кнопках, просто переотправляем меню
        if text.lower() in ('/menu', 'меню', 'отмена', 'назад', '/start', '/help'):
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
            return
        
        await update.message.reply_text("Выберите месяц с помощью кнопок.")
        await self._show_other_menu(update, chat_id)

    async def _handle_homeroom_group_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка текстового ввода в режиме выбора группы домашки (игнорируем)"""
        # Так как это меню на inline кнопках, просто переотправляем меню
        if text.lower() in ('/menu', 'меню', 'отмена', 'назад', '/start', '/help'):
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
            return
        
        await update.message.reply_text("Выберите группу с помощью кнопок.")
        await self._show_other_menu(update, chat_id)


    # ========== ОБРАБОТЧИКИ СОСТОЯНИЙ ==========
    
    async def _handle_idle_state(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка состояния IDLE"""
        if text == '🛡️ Админ панель':
            if not await self.auth.is_admin(session['user_id']):
                await update.message.reply_text("❌ У вас нет прав администратора.")
                return
            await self._show_admin_menu(update, chat_id)
        elif 'Создать карточку' in text or text == '/create':
            await self._start_creation(update, chat_id)
        elif 'Найти' in text or 'Просмотреть' in text or text == '/view':
            session['mode'] = 'VIEW_ONLY'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        elif 'Редактировать' in text or text == '/edit':
            session['mode'] = 'EDIT'
            await self.sessions.save_session(chat_id, session)
            await self._show_alphabet(update, chat_id)
        elif 'Задать вопрос' in text or 'AI' in text or text == '/ask':
            await self._start_gemini_question(update, chat_id)
        elif 'Остальное' in text or text == '/other':
            await self._show_other_menu(update, chat_id)
        else:
            await self._send_main_menu(update, chat_id)
    
    async def _handle_admin_menu(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка админ меню"""
        if session.get('step') == 'WAITING_USER_ID_FOR_ADD':
            try:
                parts = text.strip().split()
                user_id = int(parts[0])
                user_type = parts[1] if len(parts) > 1 else "user"
                
                if user_type not in ['admin', 'user']:
                    user_type = "user"
                
                result = await self.auth.add_user(user_id, "", "", "", user_type)
                await update.message.reply_text(result)
                session['step'] = None
                await self.sessions.save_session(chat_id, session)
                await self._show_admin_menu(update, chat_id)
            except ValueError:
                await update.message.reply_text("❌ Неверный формат ID. Введите число (и опционально 'admin' для прав администратора).")
        
        elif session.get('step') == 'WAITING_USER_ID_FOR_REMOVE':
            try:
                user_id = int(text.strip())
                result = await self.auth.remove_user(user_id)
                await update.message.reply_text(result)
                session['step'] = None
                await self.sessions.save_session(chat_id, session)
                await self._show_admin_menu(update, chat_id)
            except ValueError:
                await update.message.reply_text("❌ Неверный формат ID. Введите число.")
        
        elif text == '👥 Список пользователей':
            await self._show_users_list(update, chat_id)
        elif text == '📊 Статистика':
            await self._show_admin_stats(update, chat_id)
        elif text == '📋 Последние логи':
            await self._show_access_logs(update, chat_id)
        elif text == '🏠 Главное меню':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        else:
            await self._show_admin_menu(update, chat_id)
    
    async def _handle_letter_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка выбора буквы"""
        if text == '⬅️ Назад':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
            return
        
        # Проверяем, что текст - это одна буква
        if text and len(text) == 1 and validator.is_valid_letter(text):
            await self._show_people_by_letter(update, chat_id, text.upper())
        else:
            await self._show_alphabet(update, chat_id)
    
    async def _handle_person_selection(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка выбора человека"""
        if text == '⬅️ Назад к буквам':
            await self._show_alphabet(update, chat_id)
            return
        
        # Извлекаем ID из текста
        row_index = formatter.extract_row_id(text)
        if row_index > 0:
            if session.get('mode') == 'VIEW_ONLY':
                await self._show_read_only_card(update, chat_id, row_index)
            elif session.get('mode') == 'EDIT':
                await self._start_editing(update, chat_id, row_index)
        else:
            await update.message.reply_text("❌ Человек не найден (возможно, удален).")
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
    
    async def _handle_viewing_card(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка просмотра карточки"""
        if text == '⬅️ К списку имен':
            if session.get('last_letter'):
                await self._show_people_by_letter(update, chat_id, session['last_letter'])
            else:
                await self._show_alphabet(update, chat_id)
        elif text == '🏠 В главное меню':
            await self.sessions.clear_session(chat_id)
            await self._send_main_menu(update, chat_id)
        else:
            # Показываем ту же карточку
            if session.get('viewing_row'):
                await self._show_read_only_card(update, chat_id, session['viewing_row'])
    
    async def _handle_builder_mode(self, update: Update, chat_id: int, text: str, session: Dict[str, Any]):
        """Обработка режима конструктора"""
        if session['step'] == 'MENU':
            if text == '❌ Отмена':
                await self.sessions.clear_session(chat_id)
                await self._send_main_menu(update, chat_id)
            elif text == '➕ Доб. категорию':
                session['step'] = 'WAITING_NEW_CAT'
                await self.sessions.save_session(chat_id, session)
                await update.message.reply_text("Напишите название новой категории:")
            else:
                # Проверяем, является ли текст названием поля
                headers = await self.sheets.get_headers()
                for header in headers:
                    if text.startswith(header) or text.startswith(f"✅ {header}"):
                        session['step'] = 'WAITING_VALUE'
                        session['current_field'] = header
                        await self.sessions.save_session(chat_id, session)
                        
                        current_value = session['draft'].get(header, "")
                        if header in settings.date_columns and current_value:
                            current_value = self.sheets.format_date(current_value)
                        
                        message = f"Введите значение для {html.bold(header)}:\n"
                        if header in settings.date_columns:
                            message += "Формат: ДД.ММ.ГГГГ (например: 04.05.1998)\n"
                        if current_value:
                            message += f"(Текущее: {html.escape(str(current_value))})"
                        
                        await update.message.reply_text(message, parse_mode='HTML')
                        return
                
                await self._show_builder_menu(update, chat_id, session)
        
        elif session['step'] == 'WAITING_VALUE':
            field_name = session.get('current_field')
            if field_name:
                session['draft'][field_name] = text
                session['step'] = 'MENU'
                session['current_field'] = None
                await self.sessions.save_session(chat_id, session)
                await self._show_builder_menu(update, chat_id, session)
        
        elif session['step'] == 'WAITING_NEW_CAT':
            if text and text.strip():
                # Проверяем, нет ли уже такой категории
                headers = await self.sheets.get_headers()
                if text.strip() in headers:
                    await update.message.reply_text(f"❌ Категория '{text}' уже существует!")
                else:
                    await self.sheets.add_column(text.strip())
                    await update.message.reply_text(f"✅ Категория '{text}' добавлена!")
                
                session['step'] = 'MENU'
                await self.sessions.save_session(chat_id, session)
                await self._show_builder_menu(update, chat_id, session)


# Глобальный экземпляр бота
bot = TelegramBot()