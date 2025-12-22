# Church Database Bot v2.0

Telegram бот и Web App для управления церковной базой данных на базе Google Sheets с интеграцией Gemini AI.

## Функции

- **Управление прихожанами**: Просмотр, поиск, добавление и редактирование данных через Telegram или Mini App.
- **Dynamic Search**: Мгновенный поиск по базе в Mini App по мере ввода имени.
- **AI Ассистент**: Задавайте вопросы своей базе данных на естественном языке (благодаря Google Gemini).
- **Организация**: Группировка по домашним группам, отслеживание дней рождения.
- **Админ-панель**: Управление доступом пользователей и мониторинг активности.

## Технологии

- **Backend**: Python, FastAPI, python-telegram-bot
- **Frontend**: HTML5, Tailwind CSS, Telegram WebApp SDK
- **Database**: Google Sheets API (gspread)
- **AI**: Google Generative AI (Gemini)
- **Deployment**: Docker, Docker Compose

## Установка и запуск

1. Клонируйте репозиторий.
2. Создайте файл `.env` на основе примера.
3. Поместите ваш `google_creds.json` в корневую папку.
4. Запустите через Docker Compose:
   ```bash
   docker-compose up -d
   ```

## Настройка

Основные параметры настраиваются в файле `.env`:
- `TELEGRAM_TOKEN`: Токен вашего бота от @BotFather.
- `SHEET_ID`: ID вашей Google таблицы.
- `GEMINI_API_KEY`: Ключ API для Google Gemini.
- `WEBAPP_URL`: URL, где развернут Mini App (для работы кнопок в боте).

---
Разработано для эффективного служения.
