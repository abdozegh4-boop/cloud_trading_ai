# نشر المشروع على Google Cloud Run — دليل خطوة بخطوة (VS Code)

> **تحديث:** للانتقال إلى **Firestore + لوحة ويب** بدل PostgreSQL و Streamlit،
> راجع **[DEPLOY_FIRESTORE.md](./DEPLOY_FIRESTORE.md)**.

## 0. المتطلبات قبل البدء
- حساب Google Cloud مع مشروع منشأ (أو أنشئ واحد من https://console.cloud.google.com)
- تثبيت [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) على جهازك
- تفعيل الفوترة (Billing) على المشروع (Cloud Run يحتاجها حتى ضمن الحد المجاني)

افتح Terminal داخل VS Code (`` Ctrl+` ``) ونفّذ:

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud services enable run.googleapis.com artifactregistry.googleapis.com
```

## 1. بنية المشروع الجديد
انسخ هذه الملفات في مجلد مشروعك الجديد (تم تجهيزها لك بالفعل):

```
your-project/
├── main.py              (نفس الكود مع تعديل بسيط لمتغير الويبهوك)
├── db.py                (بدون تغيير)
├── requirements.txt     (بدون تغيير)
├── Dockerfile           (جديد)
├── .dockerignore        (جديد)
└── .env.example         (جديد - مرجعي فقط، لا يُرفع للسحابة)
```

## 2. النشر — وضع التوفير (موصى به لتقليل التكلفة)

`min-instances=1` و `no-cpu-throttling` يبقيان النسخة تعمل **طوال اليوم** (تكلفة ثابتة).  
للحد الأدنى من التكلفة استخدم **scale-to-zero** + **Cloud Scheduler** لإيقاظ الخدمة فقط عند الحاجة.

### 2-أ) نشر رخيص (الافتراضي الموصى به)

```bash
gcloud run deploy trading-bot \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --cpu-throttling \
  --min-instances=0 \
  --max-instances=1 \
  --memory=512Mi \
  --cpu=1 \
  --timeout=300 \
  --concurrency=40
```

| الخيار | المعنى للتكلفة |
|---|---|
| `--min-instances=0` | لا تدفع وقت الخمول — النسخة تنام بين الطلبات |
| `--cpu-throttling` | CPU فقط أثناء معالجة الطلب (أرخص من no-cpu-throttling) |
| `--max-instances=1` | سقف تكلفة/توازي |
| `--memory=512Mi` | أقل ذاكرة عملية كافية للبوت |
| `--timeout=300` | وقت كافٍ لمسار الوكلاء المتعددين عند الطلب |

**المقايضة:** عند أول طلب بعد النوم يحدث Cold Start (ثوانٍ) ويُعاد اتصال cTrader. الخلفية (APScheduler) تعمل فقط بينما النسخة دافئة.

### 2-ب) إيقاظ رخيص بدل min-instances=1 (Cloud Scheduler)

بعد النشر، أنشئ مهمة كل **12–15 دقيقة** (أقل تكرار = أقل تكلفة إيقاظ):

```bash
# تفعيل Cloud Scheduler API مرة واحدة
gcloud services enable cloudscheduler.googleapis.com

# استبدل SERVICE_URL و CRON_SECRET
gcloud scheduler jobs create http trading-bot-tick \
  --location us-central1 \
  --schedule="*/15 * * * *" \
  --uri="https://YOUR_SERVICE_URL/cron/tick?secret=YOUR_CRON_SECRET" \
  --http-method=GET \
  --attempt-deadline=320s \
  --time-zone="UTC"
```

اختياري — نبضة أخف كل 10 دقائق للحساب فقط:

```bash
gcloud scheduler jobs create http trading-bot-keepalive \
  --location us-central1 \
  --schedule="*/10 * * * *" \
  --uri="https://YOUR_SERVICE_URL/cron/keepalive?secret=YOUR_CRON_SECRET" \
  --http-method=GET \
  --attempt-deadline=60s \
  --time-zone="UTC"
```

في متغيرات البيئة ضع:

```text
CHEAP_CLOUD_RUN=1
CRON_SECRET=سلسلة_عشوائية_طويلة
CTRADER_ACCOUNT_REFRESH_SECONDS=60
FINNHUB_REFRESH_SECONDS=120
TRADINGVIEW_REFRESH_SECONDS=120
AUTO_ANALYSIS_INTERVAL_MINUTES=30
```

رفع فواصل التحديث يقلل عمل CPU أثناء كل إيقاظ.

### 2-ج) وضع always-on (أغلى — فقط إن احتجت اتصال cTrader دائم 24/7)

```bash
gcloud run deploy trading-bot \
  --source . \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --no-cpu-throttling \
  --min-instances=1 \
  --max-instances=1 \
  --memory=512Mi
```

و `CHEAP_CLOUD_RUN=0`.

| الخيار | التكلفة |
|---|---|
| `--min-instances=1` | فاتورة تقريباً مستمرة طوال الشهر |
| `--no-cpu-throttling` | CPU محسوب حتى بدون طلبات |

بعد انتهاء الأمر ستحصل على رابط مثل:
```
https://trading-bot-xxxxxxxxxx-uc.a.run.app
```
احتفظ بهذا الرابط.

## 3. ضبط متغيرات البيئة (Secrets)

**لا تضع مفاتيح حساسة (API keys, tokens) كـ plain env vars في أوامر gcloud مباشرة إن كان الكود مشترك.** الأفضل استخدام Secret Manager:

```bash
# مثال لإنشاء secret واحد (كرر لكل متغير حساس)
echo -n "القيمة_الفعلية" | gcloud secrets create GEMINI_API_KEY --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create TELEGRAM_BOT_TOKEN --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create DATABASE_URL --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create CTRADER_CLIENT_ID --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create CTRADER_CLIENT_SECRET --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create CTRADER_ACCESS_TOKEN --data-file=-
echo -n "القيمة_الفعلية" | gcloud secrets create FINNHUB_API_KEY --data-file=-
```

ثم اربطها بالخدمة مع بقية المتغيرات غير الحساسة (مثل SERVICE_URL بعد معرفته من الخطوة 2):

```bash
gcloud run services update trading-bot \
  --region us-central1 \
  --set-env-vars="SERVICE_URL=https://trading-bot-xxxxxxxxxx-uc.a.run.app,MY_TELEGRAM_CHAT_ID=123456789,CTRADER_HOST=demo.ctraderapi.com,CTRADER_PORT=5035,CTRADER_ACCOUNT_ID=YOUR_ACCOUNT_ID,AUTO_ANALYSIS_INTERVAL_MINUTES=10,GEMINI_MODEL=gemini-3.6-flash" \
  --set-secrets="GEMINI_API_KEY=GEMINI_API_KEY:latest,TELEGRAM_BOT_TOKEN=TELEGRAM_BOT_TOKEN:latest,DATABASE_URL=DATABASE_URL:latest,CTRADER_CLIENT_ID=CTRADER_CLIENT_ID:latest,CTRADER_CLIENT_SECRET=CTRADER_CLIENT_SECRET:latest,CTRADER_ACCESS_TOKEN=CTRADER_ACCESS_TOKEN:latest,FINNHUB_API_KEY=FINNHUB_API_KEY:latest"
```

> بديل أبسط (أقل أمانًا، مقبول للتجربة السريعة فقط): استبدل `--set-secrets` بإضافة نفس المتغيرات مباشرة داخل `--set-env-vars`.

## 4. طريقة أسهل: عبر ملف YAML بدل أوامر طويلة (اختياري)

أنشئ ملف `env.yaml` (لا ترفعه لأي مستودع Git):

```yaml
SERVICE_URL: "https://trading-bot-xxxxxxxxxx-uc.a.run.app"
MY_TELEGRAM_CHAT_ID: "123456789"
CTRADER_HOST: "demo.ctraderapi.com"
CTRADER_PORT: "5035"
CTRADER_ACCOUNT_ID: "YOUR_ACCOUNT_ID"
AUTO_ANALYSIS_INTERVAL_MINUTES: "10"
GEMINI_MODEL: "gemini-3.6-flash"
```

ثم:
```bash
gcloud run services update trading-bot --region us-central1 --env-vars-file=env.yaml
```

## 5. إعادة النشر بعد أي تعديل بالكود

في كل مرة تعدّل main.py أو db.py:
```bash
gcloud run deploy trading-bot --source . --region us-central1
```
(أعد تمرير خيارات التكلفة عند كل deploy إن غيّرتها؛ الافتراضي الموصى به: min-instances=0 + cpu-throttling)

## 6. التحقق من نجاح النشر

```bash
curl https://trading-bot-xxxxxxxxxx-uc.a.run.app/
```
يجب أن تحصل على:
```json
{"status": "online", "service": "AI Trading Assistant", ...}
```

راقب السجلات مباشرة:
```bash
gcloud run services logs read trading-bot --region us-central1 --limit=50
```

## 7. ملاحظات مهمة خاصة بمشروعك

1. **وضع التوفير (موصى به):** `min-instances=0` + `cpu-throttling` + Cloud Scheduler على `/cron/tick` كل 12–15 دقيقة. التكلفة تقترب من الصفر في الخمول.
2. **cTrader:** بعد Cold Start يُعاد الاتصال عبر خيط الإقلاع؛ أول تحليل بعد الإيقاظ قد يتأخر ثوانٍ حتى تصل الشموع.
3. **APScheduler:** يعمل فقط أثناء دفء النسخة؛ الاعتماد الأساسي للتحديث الدوري في وضع التوفير هو `/cron/tick`.
4. **always-on:** `min-instances=1` + `no-cpu-throttling` فقط إذا احتجت اتصالاً دائماً — تكلفة شهرية ثابتة (راجع [حاسبة Cloud Run](https://cloud.google.com/products/calculator)).
4. **Neon (PostgreSQL):** لا علاقة له بـ Cloud Run، يبقى يعمل كما هو فقط تأكد أن `DATABASE_URL` صحيح ومُدرج في الأسرار.
5. **الحد الأقصى لعمر الطلب (Request Timeout):** الافتراضي 300 ثانية على Cloud Run، يكفي غالبًا لأن الويبهوك يرد بسرعة والعمليات الطويلة (التحليل، الجدولة) تعمل بشكل غير متزامن في الخلفية.

---
بعد اتباع هذه الخطوات يكون مشروعك يعمل بالكامل على Cloud Run بنفس آلية عمله الحالية (Webhook + جدولة تلقائية + اتصال دائم بـ cTrader).
