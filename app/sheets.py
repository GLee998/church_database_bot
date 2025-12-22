"""
Асинхронный клиент для Google Sheets с кэшированием
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import gspread
from google.oauth2 import service_account
from google.auth import default as google_default

from app.config import settings
from app.utils import formatter

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets"""
    
    def __init__(self):
        self._client = None
        self._spreadsheet = None
        self._worksheets = {}
        
        # Кэш данных
        self._cache: Dict[str, List[List[Any]]] = {}
        self._cache_lock = asyncio.Lock()
    
    async def _get_client(self):
        """Получение клиента"""
        if self._client is None:
            try:
                if settings.google_credentials_file:
                    credentials = service_account.Credentials.from_service_account_file(
                        settings.google_credentials_file,
                        scopes=[
                            'https://www.googleapis.com/auth/spreadsheets',
                            'https://www.googleapis.com/auth/drive'
                        ]
                    )
                else:
                    credentials, _ = google_default()
                
                loop = asyncio.get_event_loop()
                self._client = await loop.run_in_executor(
                    None, 
                    lambda: gspread.authorize(credentials)
                )
                logger.info("✅ Google Sheets client authorized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Google Sheets client: {e}")
                raise
        return self._client
    
    async def _get_spreadsheet(self):
        """Получение таблицы"""
        if self._spreadsheet is None:
            client = await self._get_client()
            loop = asyncio.get_event_loop()
            self._spreadsheet = await loop.run_in_executor(
                None,
                lambda: client.open_by_key(settings.sheet_id)
            )
        return self._spreadsheet
    
    async def get_worksheet(self, title: str = None):
        """Получение листа"""
        spreadsheet = await self._get_spreadsheet()
        loop = asyncio.get_event_loop()
        
        cache_key = title if title else "MainSheet"
        
        if cache_key not in self._worksheets:
            try:
                if title is None:
                    worksheet = await loop.run_in_executor(None, lambda: spreadsheet.sheet1)
                else:
                    worksheet = await loop.run_in_executor(None, lambda: spreadsheet.worksheet(title))
                self._worksheets[cache_key] = worksheet
            except gspread.exceptions.WorksheetNotFound:
                # Создаем новый лист
                worksheet = await loop.run_in_executor(
                    None,
                    lambda: spreadsheet.add_worksheet(title=title, rows=1000, cols=20)
                )
                self._worksheets[cache_key] = worksheet
        
        return self._worksheets[cache_key]
    
    async def refresh_cache(self, worksheet_title: str = None):
        """Принудительное обновление кэша из Google Sheets"""
        if worksheet_title:
            # Обновляем конкретный лист
            cache_key = worksheet_title
            worksheet = await self.get_worksheet(worksheet_title)
            loop = asyncio.get_event_loop()
            
            logger.info(f"🔄 Refreshing cache for {cache_key}...")
            
            # Скачиваем данные
            data = await loop.run_in_executor(None, worksheet.get_all_values)
            
            async with self._cache_lock:
                self._cache[cache_key] = data
                
            logger.info(f"✅ Cache updated for {cache_key}: {len(data)} rows")
            return len(data)
        else:
            # Обновляем ВСЕ известные листы
            logger.info("🔄 Refreshing cache for ALL worksheets...")
            
            # Список листов для обновления
            worksheets_to_sync = ["MainSheet", "Users", "AccessLog"]
            total_rows = 0
            
            for sheet_name in worksheets_to_sync:
                try:
                    worksheet = await self.get_worksheet(sheet_name)
                    loop = asyncio.get_event_loop()
                    
                    # Скачиваем данные
                    data = await loop.run_in_executor(None, worksheet.get_all_values)
                    
                    async with self._cache_lock:
                        self._cache[sheet_name] = data
                    
                    logger.info(f"✅ {sheet_name}: {len(data)} rows")
                    total_rows += len(data)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to refresh {sheet_name}: {e}")
            
            logger.info(f"✅ All caches updated. Total rows: {total_rows}")
            return total_rows

    async def refresh_users_cache(self):
        """Обновление кэша пользователей"""
        return await self.refresh_cache("Users")
    
    async def refresh_logs_cache(self):
        """Обновление кэша логов"""
        return await self.refresh_cache("AccessLog")
    
    async def refresh_main_cache(self):
        """Обновление кэша основной таблицы"""
        return await self.refresh_cache()  # Без параметра = MainSheet

    async def get_all_data(self, worksheet_title: str = None) -> List[List[Any]]:
        """Получение всех данных"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        
        # Если нет в кэше - загружаем
        if cache_key not in self._cache:
            await self.refresh_cache(worksheet_title)
        
        # Возвращаем из кэша
        return self._cache.get(cache_key, [])
    
    async def get_headers(self, worksheet_title: str = None) -> List[str]:
        """Получение заголовков"""
        data = await self.get_all_data(worksheet_title)
        return data[0] if data else []
    
    async def append_row(self, data: List[Any], worksheet_title: str = None) -> int:
        """Добавление строки"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Отправляем в Google Sheets
        await loop.run_in_executor(None, worksheet.append_row, data)
        
        # Обновляем кэш
        async with self._cache_lock:
            if cache_key in self._cache:
                self._cache[cache_key].append([str(x) for x in data])
            else:
                await self.refresh_cache(worksheet_title)
        
        row_count = len(self._cache[cache_key])
        logger.info(f"📝 Row appended to {cache_key}, total: {row_count}")
        
        return row_count
    
    async def update_row(self, row_number: int, data: List[Any], worksheet_title: str = None):
        """Обновление строки"""
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Обновляем в Google Sheets
        await loop.run_in_executor(
            None,
            lambda: worksheet.update(f"A{row_number}", [data])
        )
        
        # Обновляем кэш
        async with self._cache_lock:
            if cache_key in self._cache:
                idx = row_number - 1
                if 0 <= idx < len(self._cache[cache_key]):
                    self._cache[cache_key][idx] = [str(x) for x in data]
            else:
                await self.refresh_cache(worksheet_title)
        
        logger.info(f"✏️ Row {row_number} updated in {cache_key}")
    
    async def add_column(self, column_name: str, worksheet_title: str = None) -> bool:
        """Добавление колонки"""
        headers = await self.get_headers(worksheet_title)
        if column_name in headers:
            return False
        
        cache_key = worksheet_title if worksheet_title else "MainSheet"
        worksheet = await self.get_worksheet(worksheet_title)
        loop = asyncio.get_event_loop()
        
        # Добавляем колонку
        col_index = len(headers) + 1
        cell = worksheet.cell(1, col_index)
        cell.value = column_name
        
        await loop.run_in_executor(None, worksheet.update_cells, [cell])
        
        # Сбрасываем кэш
        await self.refresh_cache(worksheet_title)
        
        return True
    
    @staticmethod
    def format_date(date_value: Any) -> str:
        """Форматирование даты"""
        return formatter.format_date(date_value)

    async def get_birthdays_data_by_month(self) -> Dict[int, List[Dict[str, Any]]]:
        """Получение списка дней рождения, сгруппированных по месяцам (сырые данные)."""
        all_data = await self.get_all_data()
        
        if not all_data or len(all_data) <= 1:
            return {}
        
        headers = all_data[0]
        data_rows = all_data[1:]
        
        try:
            name_idx = headers.index(settings.col_first_name)
            surname_idx = headers.index(settings.col_last_name)
            birth_idx = headers.index(settings.col_birth_date)
        except ValueError:
            logger.error("Birthday columns not found in sheet headers.")
            return {}

        birthdays: Dict[int, List[Dict[str, Any]]] = {}

        for i, row in enumerate(data_rows, start=2): # start=2 for row_index in sheet
            if birth_idx < len(row):
                birth_date_raw = row[birth_idx].strip()
                if birth_date_raw:
                    try:
                        # Пытаемся распарсить дату. Используем format_date для конвертации в ДД.ММ.ГГГГ
                        formatted_date_str = self.format_date(birth_date_raw)
                        
                        # Если формат ДД.ММ.ГГГГ, парсим его
                        d, m, y = map(int, formatted_date_str.split('.'))
                        
                        month = m
                        
                        name = str(row[name_idx]).strip() if name_idx < len(row) else ""
                        surname = str(row[surname_idx]).strip() if surname_idx < len(row) else ""
                        
                        person_data = {
                            'name': f"{name} {surname}".strip(),
                            'day': d,
                            'year': y,
                            'row_index': i
                        }
                        
                        if month not in birthdays:
                            birthdays[month] = []
                        birthdays[month].append(person_data)
                        
                    except Exception as e:
                        # logger.warning(f"Could not parse birth date '{birth_date_raw}' in row {i+1}: {e}")
                        pass
        
        # Сортируем дни рождения внутри каждого месяца по дню
        for month in birthdays:
            birthdays[month].sort(key=lambda x: x['day'])
            
        return birthdays

    async def get_people_by_homeroom(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получение списка людей, сгруппированных по Домашкам.
        Включает возраст и статус для задачи 4.
        """
        all_data = await self.get_all_data()
        
        if not all_data or len(all_data) <= 1:
            return {}
        
        headers = all_data[0]
        data_rows = all_data[1:]
        
        try:
            name_idx = headers.index(settings.col_first_name)
            surname_idx = headers.index(settings.col_last_name)
            homeroom_idx = headers.index(settings.col_homeroom)
            birth_idx = headers.index(settings.col_birth_date) # Для возраста
            status_idx = headers.index(settings.col_status) # Для статуса
        except ValueError:
            logger.error("Required columns for homeroom grouping/details not found in sheet headers. Ensure 'Имя', 'Фамилия', 'Домашка', 'Дата рождения', 'Статус' exist.")
            # Если не все колонки найдены, возвращаем пустой dict
            return {}

        homerooms: Dict[str, List[Dict[str, Any]]] = {}
 
        for homeroom in settings.homeroom_values:
            homerooms[homeroom] = []
 
        for i, row in enumerate(data_rows, start=2): # start=2 for row_index in sheet
            if homeroom_idx < len(row):
                homeroom_name = str(row[homeroom_idx]).strip()
                
                # Если поле "Домашка" пустое, назначаем "Не распределен"
                if not homeroom_name:
                    homeroom_name = "Не распределен"
                
                name = str(row[name_idx]).strip() if name_idx < len(row) else ""
                surname = str(row[surname_idx]).strip() if surname_idx < len(row) else ""
                
                # Проверяем, что индексы существуют в строке
                birth_date_raw = str(row[birth_idx]).strip() if birth_idx != -1 and birth_idx < len(row) else ""
                status_raw = str(row[status_idx]).strip() if status_idx != -1 and status_idx < len(row) else ""
                
                age = formatter.calculate_age(birth_date_raw)
                age_str = f"{age} лет" if age is not None else "Н/Д"
                
                person_data = {
                    'name': f"{name} {surname}".strip(),
                    'row_index': i,
                    'age_str': age_str,
                    'status': status_raw
                }
                
                # Добавляем только если имя не пустое
                if person_data['name']:
                    if homeroom_name not in homerooms:
                        homerooms[homeroom_name] = []
                    homerooms[homeroom_name].append(person_data)
        
        # Сортируем людей по имени внутри каждой домашки
        for homeroom in homerooms:
            homerooms[homeroom].sort(key=lambda x: x['name'])
            
        return {k: v for k, v in homerooms.items() if v} # Удаляем пустые группы

# Глобальный экземпляр
sheets_client = GoogleSheetsClient()