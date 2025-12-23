"""
Конфигурация приложения
"""
import os
from typing import Optional, List
from pydantic_settings import BaseSettings
from pydantic import Field, validator
from datetime import timedelta


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # Telegram
    telegram_token: str = Field(..., env="TELEGRAM_TOKEN")
    main_admin_id: int = Field(526710245, env="MAIN_ADMIN_ID")
    
    # Google Sheets
    sheet_id: str = Field(..., env="SHEET_ID")
    google_credentials_file: Optional[str] = Field(None, env="GOOGLE_CREDENTIALS_FILE")
    
    # Gemini AI
    gemini_api_key: str = Field(..., env="GEMINI_API_KEY")
    max_gemini_tokens: int = Field(1000, env="MAX_GEMINI_TOKENS")
    
    # Названия колонок
    col_first_name: str = "Имя"
    col_last_name: str = "Фамилия"
    col_birth_date: str = "Дата рождения"
    col_homeroom: str = "Домашка"
    col_status: str = "Статус"
    col_photo: str = "Фото"
    
    # Дата колонки
    date_columns: List[str] = ["Дата рождения", "Дата", "Дата регистрации"]
    
    # Домашки (Homerooms)
    homeroom_values: List[str] = [
        "т. Лилия / Иордан",
        "Т.Роза / Grace",
        "Аркадий, Татьяна / Ковчег",
        "Руслан, Наталья / Осанна",
        "Слава, Ная / Домашка №1",
        "Гоша / Zion",
        "Ирина / Miracle",
        "Регина / Yeshua",
        "Диана / Yeshua Alive",
        "Виталик / Lion",
        "Лия / Heaven",
        "Ричард / Grace",
        "Ребенок",
        "Предподросток",
        "Не распределен",
    ]
    
    # Статусы
    status_values: List[str] = ["активный", "неактивный", "вип"]
    
    # Настройки приложения
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # Настройки сессий
    session_timeout_minutes: int = 30
    session_storage: str = Field("memory", env="SESSION_STORAGE")
    webapp_url: Optional[str] = Field(None, env="WEBAPP_URL")
    
    @property
    def session_timeout(self) -> timedelta:
        """Таймаут сессии"""
        return timedelta(minutes=self.session_timeout_minutes)
    
    @property
    def is_production(self) -> bool:
        """Проверка production окружения"""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Проверка development окружения"""
        return not self.is_production
    
    @validator('telegram_token')
    def validate_telegram_token(cls, v):
        if not v or len(v) < 10:
            raise ValueError('Invalid Telegram token')
        return v
    
    @validator('sheet_id')
    def validate_sheet_id(cls, v):
        if not v or len(v) < 10:
            raise ValueError('Invalid Google Sheet ID')
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Глобальный экземпляр настроек
settings = Settings()