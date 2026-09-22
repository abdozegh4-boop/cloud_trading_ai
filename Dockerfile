# ==========================================================
# Dockerfile - AI Trading Bot (Cloud Run)
# ==========================================================
FROM python:3.11-slim

# منع كتابة ملفات .pyc وتفعيل الطباعة الفورية للسجلات
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# تثبيت أدوات البناء المطلوبة لبعض الحزم (مثل twisted)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# نسخ ملف المتطلبات أولاً للاستفادة من الـ layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ باقي ملفات المشروع
COPY . .

# Cloud Run يمرر رقم المنفذ عبر متغير البيئة PORT (القيمة الافتراضية 8080)
ENV PORT=8080
EXPOSE 8080

# main.py يقرأ PORT من os.getenv بالفعل، فلا حاجة لتغيير شيء هنا
CMD ["python", "main.py"]
