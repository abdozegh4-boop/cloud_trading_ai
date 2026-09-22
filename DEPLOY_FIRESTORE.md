# نشر المشروع مع Firestore + لوحة ويب على Cloud Run

## ملخص التغييرات

| قبل | بعد |
|-----|-----|
| PostgreSQL (Neon/Aiven) | **Google Firestore** للبيانات الحية (لا PostgreSQL إطلاقاً) |
| Streamlit منفصل | **لوحة ويب** داخل نفس خدمة Cloud Run على `/dashboard` |
| Cloud Scheduler لإيقاظ الخدمة | **always-on**: `min-instances=1` + `--no-cpu-throttling` |
| BigQuery (اختياري) | يبقى كما هو للبيانات التاريخية الثقيلة (OHLC) |

تم حذف `dashboard.py` (Streamlit) و`database.py` (psycopg2) و`asyncpg`/`psycopg2-binary` من `requirements.txt`.

---

## 1. تفعيل Firestore في مشروع Google Cloud

```bash
gcloud config set project YOUR_PROJECT_ID

# تفعيل واجهة Firestore + BigQuery
gcloud services enable firestore.googleapis.com bigquery.googleapis.com

# إنشاء قاعدة بيانات Firestore (Native mode) إن لم تكن موجودة
gcloud firestore databases create --location=eur3 --type=firestore-native
```

في [Console](https://console.cloud.google.com/firestore) تأكد أن الوضع **Native**.

---

## 2. صلاحيات حساب الخدمة (Cloud Run)

حساب الخدمة المرتبط بخدمة Cloud Run يحتاج:

- `roles/datastore.user` (أو `Cloud Datastore User`)

```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

سكربتات `deploy.sh` / `deploy.ps1` تضيف هذا الربط تلقائياً بعد النشر.

---

## 3. متغيرات البيئة المطلوبة

```text
USE_FIRESTORE=1
FIRESTORE_PROJECT=YOUR_PROJECT_ID
# اختياري:
# FIRESTORE_DATABASE=(default)

# BigQuery (موصى به للـ OHLC والتاريخ الثقيل)
BIGQUERY_PROJECT=YOUR_PROJECT_ID
BIGQUERY_DATASET=trading_bot
BIGQUERY_LOCATION=EU

# الوضع غير الاقتصادي (always-on)
CHEAP_CLOUD_RUN=0
NON_CHEAP_ANALYSIS_INTERVAL_MINUTES=5
NON_CHEAP_OHLC_REFRESH_SECONDS=60
AUTO_ANALYSIS_INTERVAL_MINUTES=5
```

> `DATABASE_URL` لم يعد مستخدماً — لا تضبطه إطلاقاً.

---

## 4. أوقات التحديث لكل وضع

| المهمة | وضع اقتصادي (`CHEAP_CLOUD_RUN=1`) | وضع always-on (`CHEAP_CLOUD_RUN=0`) |
|--------|-----------------------------------|-------------------------------------|
| تحليل AI (NVIDIA) للمتابعات | كل `AUTO_ANALYSIS_INTERVAL_MINUTES` (افتراضي 10 د) — يتطلب Cloud Scheduler لإيقاظ الخدمة | **كل `NON_CHEAP_ANALYSIS_INTERVAL_MINUTES` (5 د)** — يعمل 24/7 لأن النسخة لا تنام |
| مزامنة شموع OHLC من cTrader | كل `OHLC_REFRESH_MINUTES` (15 د) | **كل `NON_CHEAP_OHLC_REFRESH_SECONDS` (60 ث)** |
| إشارات الأطر الزمنية (بدون AI) | فحص كل 60 ث (تحديث عند شمعة جديدة) | فحص كل 30 ث (تحديث عند شمعة جديدة) |
| لقطة حساب cTrader | كل `CTRADER_ACCOUNT_REFRESH_SECONDS` (20 ث) | كل `CTRADER_ACCOUNT_REFRESH_SECONDS` (20 ث) |
| المصادر الخارجية (Finnhub/TradingView/ForexFactory) | حسب أطرها المعتادة | حسب أطرها المعتادة |
| لوحة الويب | تجديد تلقائي كل 20 ث | تجديد تلقائي كل 20 ث |

**ملاحظة مهمة**: في الوضع غير الاقتصادي يجب دائماً النشر بـ `--min-instances=1 --no-cpu-throttling`، وإلا نامت APScheduler تماماً ولن تعمل أي مهمة مجدولة.

---

## 5. النشر (always-on)

```bash
gcloud run deploy trading-bot \
  --source . \
  --region europe-west1 \
  --platform managed \
  --allow-unauthenticated \
  --no-cpu-throttling \
  --min-instances=1 \
  --max-instances=1 \
  --memory=1Gi \
  --cpu=1 \
  --timeout=300 \
  --concurrency=40 \
  --set-env-vars="CHEAP_CLOUD_RUN=0,USE_FIRESTORE=1,FIRESTORE_PROJECT=YOUR_PROJECT_ID,BIGQUERY_PROJECT=YOUR_PROJECT_ID,BIGQUERY_DATASET=trading_bot,BIGQUERY_LOCATION=EU,NON_CHEAP_ANALYSIS_INTERVAL_MINUTES=5,NON_CHEAP_OHLC_REFRESH_SECONDS=60,AUTO_ANALYSIS_INTERVAL_MINUTES=5" \
  --set-secrets="TELEGRAM_BOT_TOKEN=TELEGRAM_BOT_TOKEN:latest,NVIDIA_API_KEY=NVIDIA_API_KEY:latest,FINNHUB_API_KEY=FINNHUB_API_KEY:latest,CTRADER_CLIENT_SECRET=CTRADER_CLIENT_SECRET:latest,CTRADER_ACCESS_TOKEN=CTRADER_ACCESS_TOKEN:latest"
```

أو شغّل سكربت النشر الكامل (ينظّف الخدمات/Scheduler القديمة ويهيّئ الأسرار):

```bash
chmod +x deploy.sh && ./deploy.sh
# أو على PowerShell:
.\deploy.ps1
```

---

## 6. الوصول إلى اللوحة

بعد النشر:

```
https://YOUR_SERVICE_URL/dashboard
```

مسارات API (JSON):

| المسار | المحتوى |
|--------|---------|
| `/api/dashboard` | ملخص شامل (runtime + صفقات + إشارات) من Firestore |
| `/api/status` | **حالة cTrader الحية**: الاتصال، الرصيد، حقوق الملكية، الصفقات المفتوحة، الوضع الحالي |
| `/api/signals` | التوصيات الحية (Entry/SL/TP + RSI/EMA/ATR) |
| `/api/ohlc?symbol=XAUUSD&timeframe=H1&limit=120` | شموع OHLC (BigQuery أولاً ثم Firestore) |
| `/api/technical?symbol=XAUUSD&timeframe=H1` | آخر لقطات المؤشرات الفنية |
| `/api/reports` | تقارير AI المخزنة |
| `/api/storage-health` | اختبار كتابة/قراءة Firestore للتشخيص |
| `/db/stats` | مراقبة أداء التخزين |
| `/cache/snapshot` | لقطة الكاش التشغيلي للوكلاء |

الصفحة الرئيسية `/` تعرض أيضاً `dashboard: "/dashboard"` وأوقات التحديث الحالية.

---

## 7. هيكل مجموعات Firestore

| المجموعة | الاستخدام |
|----------|-----------|
| `runtime/state` | حالة البوت (cheap_mode, auto_trading, …) |
| `telegram_ui/{chat_id}` | اختيارات المستخدم في التيليجرام |
| `active_watches/{id}` | المتابعات النشطة للتحديث التلقائي |
| `symbol_signals/{SYMBOL}` | آخر إشارة لكل زوج |
| `symbol_signals_history` | سجل تاريخي للإشارات |
| `trade_journal/{id}` | سجل الصفقات |
| `ai_reports` | تقارير التحليل |
| `ohlc_bars/{SYMBOL_TF_TIME}` | شموع OHLC (احتياطي إن لم يُفعَّل BigQuery) |
| `technical_snapshots` | لقطات المؤشرات الفنية (RSI/EMA/ATR/MACD) |
| `_health/ping` | اختبار صحة الكتابة (يستخدمه `/api/storage-health`) |

---

## 8. ملاحظات

- **لا PostgreSQL**: `db.py` مكتوب بالكامل لـ Firestore + BigQuery، و`db.pool` بقي كـ shim بقيمة `None` لتوافق بعض الفحوصات القديمة.
- **BigQuery اختياري**: إن لم يُضبط، تُخزَّن شموع OHLC ولقطات المؤشرات على Firestore مباشرة.
- **الفهارس**: بعض استعلامات Firestore قد تحتاج فهارس مركّبة. عند أول تشغيل راقب السجلات؛ روابط إنشاء الفهارس تظهر في رسائل الخطأ.
- **التكلفة**: always-on (`min-instances=1`) يعني دفع ثمن نسخة واحدة 24/7. للوضع الاقتصادي استخدم `CHEAP_CLOUD_RUN=1` مع Cloud Scheduler.
- **التنظيف التلقائي**: `DB_RETENTION_TECH_DAYS=14`، `DB_RETENTION_OHLC_DAYS=30`، `DB_RETENTION_REPORT_DAYS=30`، `DB_RETENTION_SIGNAL_DAYS=60`.

---

## 9. اختبار محلي

```bash
export USE_FIRESTORE=1
export FIRESTORE_PROJECT=YOUR_PROJECT_ID
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
python main.py
# ثم افتح http://localhost:8080/dashboard
```
