"""
Вспомогательные утилиты
"""
import re
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class HTMLFormatter:
    """Форматирование HTML текста"""
    
    @staticmethod
    def escape(text: str) -> str:
        """Экранирование HTML-символов"""
        if not text:
            return ""
        
        replacements = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;',
            '\t': '    ',
        }
        
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)
        
        return text
    
    @staticmethod
    def bold(text: str) -> str:
        """Жирный текст"""
        return f"<b>{text}</b>"
    
    @staticmethod
    def italic(text: str) -> str:
        """Курсивный текст"""
        return f"<i>{text}</i>"
    
    @staticmethod
    def code(text: str) -> str:
        """Код"""
        return f"<code>{text}</code>"
    
    @staticmethod
    def link(text: str, url: str) -> str:
        """Ссылка"""
        return f'<a href="{url}">{text}</a>'


class DataFormatter:
    """Форматирование данных"""
    
    @staticmethod
    def calculate_age(birth_date_raw: Any) -> Optional[int]:
        """Расчет возраста по дате рождения (строка)"""
        if not birth_date_raw:
            return None
        
        date_str = str(birth_date_raw).strip()
        if not date_str:
            return None
            
        formats = ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
        birth_date = None
        
        for fmt in formats:
            try:
                birth_date = datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                continue

        if not birth_date:
            return None
            
        today = datetime.now().date()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        return age
    
    @staticmethod
    def format_date(date_value: Any) -> str:
        """Форматирование даты"""
        if not date_value:
            return ""
        
        if isinstance(date_value, datetime):
            return date_value.strftime("%d.%m.%Y")
        
        if isinstance(date_value, str):
            formats = ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt).strftime("%d.%m.%Y")
                except ValueError:
                    continue
        
        return str(date_value)
    
    @staticmethod
    def extract_row_id(text: str) -> int:
        """Извлечение ID строки из текста"""
        match = re.search(r'\[#(\d+)\]$', text)
        if match:
            return int(match.group(1))
        return 0
    
    @staticmethod
    def format_table_for_gemini(headers: List[str], data: List[List[str]]) -> str:
        """Форматирование таблицы для Gemini"""
        # Преобразуем все значения в строки
        formatted_data = []
        for row in data:
            formatted_row = []
            for cell in row:
                formatted_row.append(str(cell) if cell is not None else "")
            formatted_data.append(formatted_row)
        
        # Создаем markdown таблицу
        result = "| " + " | ".join(headers) + " |\n"
        result += "|---" * len(headers) + "|\n"
        
        for row in formatted_data:
            result += "| " + " | ".join(row) + " |\n"
        
        return result


class Validator:
    """Валидация данных"""
    
    @staticmethod
    def is_valid_letter(text: str) -> bool:
        """Проверка, что текст - это одна буква"""
        return bool(text and len(text) == 1 and re.match(r'^[А-Яа-яA-Za-z]$', text))
    
    @staticmethod
    def is_valid_date(text: str) -> bool:
        """Проверка даты в формате ДД.ММ.ГГГГ"""
        pattern = r'^\d{1,2}\.\d{1,2}\.\d{4}$'
        if not re.match(pattern, text):
            return False
        
        try:
            day, month, year = map(int, text.split('.'))
            datetime(year, month, day)
            return True
        except ValueError:
            return False


# Глобальные экземпляры
html = HTMLFormatter()
formatter = DataFormatter()
validator = Validator()