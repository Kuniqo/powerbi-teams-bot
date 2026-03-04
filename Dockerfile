# Imagen base ligera con Python 3.11
FROM python:3.11-slim

# Directorio de trabajo
WORKDIR /app

# Copiar requirements primero (para cacheo de capas Docker)
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código
COPY . .

# Puerto que expone el bot
EXPOSE 3978

# Comando de inicio
CMD ["python", "app.py"]
