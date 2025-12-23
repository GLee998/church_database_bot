import os
import uuid
import io
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from PIL import Image
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
except ImportError:
    pass
from typing import List, Dict, Any, Optional
from app.sheets import sheets_client
from app.gemini import gemini
from app.auth import auth_manager
from app.config import settings
from pydantic import BaseModel

router = APIRouter(prefix="/api/miniapp", tags=["miniapp"])

class QuestionRequest(BaseModel):
    question: str

class PersonUpdate(BaseModel):
    data: List[Any]

@router.get("/headers")
async def get_headers():
    return await sheets_client.get_headers()

@router.get("/people")
async def get_people():
    data = await sheets_client.get_all_data()
    if not data:
        return []
    headers = data[0]
    people = []
    for i, row in enumerate(data[1:], start=2):
        person = {"row_index": i}
        for j, header in enumerate(headers):
            val = row[j] if j < len(row) else ""
            person[header] = val
        people.append(person)
    return people

@router.get("/birthdays")
async def get_birthdays():
    return await sheets_client.get_birthdays_data_by_month()

@router.get("/homerooms")
async def get_homerooms():
    return await sheets_client.get_people_by_homeroom()

@router.post("/ask")
async def ask_ai(req: QuestionRequest):
    headers = await sheets_client.get_headers()
    all_data = await sheets_client.get_all_data()
    if not all_data or len(all_data) <= 1:
        return {"answer": "База данных пуста."}
    
    if not gemini.initialized:
        await gemini.initialize()
    
    answer = await gemini.analyze_table(req.question, headers, all_data[1:])
    return {"answer": answer}

@router.post("/person/{row_index}")
async def update_person(row_index: int, person: PersonUpdate):
    try:
        await sheets_client.update_row(row_index, person.data)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/person")
async def create_person(person: PersonUpdate):
    try:
        new_row = await sheets_client.append_row(person.data)
        return {"success": True, "row_index": new_row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/config")
async def get_config():
    return {
        "homeroom_values": settings.homeroom_values,
        "status_values": settings.status_values,
        "date_columns": settings.date_columns,
        "col_photo": settings.col_photo
    }

@router.post("/person/{row_index}/photo")
async def upload_photo(row_index: int, file: UploadFile = File(...)):
    # Создаем директорию для фото если нет
    photo_dir = "static/photos"
    os.makedirs(photo_dir, exist_ok=True)
    
    # Генерируем имя файла (всегда сохраняем в jpg для совместимости и сжатия)
    filename = f"{uuid.uuid4()}.jpg"
    filepath = os.path.join(photo_dir, filename)
    
    try:
        # Читаем файл
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Файл пуст")
            
        image = Image.open(io.BytesIO(content))
        
        # Конвертируем в RGB если нужно (для PNG с прозрачностью или других форматов)
        if image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
        elif image.mode != "RGB":
            image = image.convert("RGB")

        # Сжатие/ресайз
        max_size = 800
        if max(image.size) > max_size:
            image.thumbnail((max_size, max_size), Image.LANCZOS)
        
        # Сохраняем с оптимизацией
        try:
            image.save(filepath, "JPEG", quality=85, optimize=True)
        except Exception as save_error:
            raise HTTPException(status_code=500, detail=f"Ошибка сохранения файла на сервере: {str(save_error)}")
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка обработки изображения: {str(e)}")
    
    photo_url = f"/photos/{filename}"
    
    # Обновляем ячейку в таблице
    headers = await sheets_client.get_headers()
    try:
        photo_col_idx = headers.index(settings.col_photo)
    except ValueError:
        # Добавляем колонку если нет
        await sheets_client.add_column(settings.col_photo)
        headers = await sheets_client.get_headers()
        photo_col_idx = headers.index(settings.col_photo)
    
    # Получаем текущую строку
    data = await sheets_client.get_all_data()
    row_data = list(data[row_index - 1])
    
    # Расширяем строку если она короче чем индекс колонки фото
    while len(row_data) <= photo_col_idx:
        row_data.append("")
    
    row_data[photo_col_idx] = photo_url
    await sheets_client.update_row(row_index, row_data)
    
    # Refresh cache to ensure it's updated for other users
    await sheets_client.refresh_main_cache()
    
    return {"photo_url": photo_url}

@router.delete("/person/{row_index}")
async def delete_person(row_index: int):
    try:
        await sheets_client.delete_row(row_index)
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/column/{column_name}")
async def delete_column(column_name: str):
    try:
        success = await sheets_client.delete_column(column_name)
        return {"success": success}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))