#!/usr/bin/env bash
# ==========================================================
# deploy.sh — تنظيف كامل (خدمتين + 3 مهام Scheduler) ثم نشر نظيف واحد
# ==========================================================
# الاستخدام:
#   1) عدّل القيم في قسم "الإعدادات" تحت (التوكن الجديد بعد /revoke في BotFather).
#   2) من نفس مجلد المشروع (بجانب main.py و Dockerfile):
#        chmod +x deploy.sh
#        ./deploy.sh
set -euo pipefail

# ==================== الإعدادات (عدّل هذا القسم فقط) ====================
PROJECT_ID="zeghoudi-trade-ai"
REGION="europe-west1"                 # المنطقة اللي راح تُنشر فيها الخدمة الوحيدة الجديدة
SERVICE_NAME="trading-bot"

# مناطق قديمة فيها موارد لازم تنحذف (خدمات Cloud Run قديمة بمناطق مختلفة)
OLD_SERVICE_REGIONS=("europe-west1" "us-central1")

# مهام Cloud Scheduler القديمة الموجودة فعلياً (اسم:منطقة)
OLD_SCHEDULER_JOBS=(
  "trading-bot-tick:us-central1"
  "trading-bot-keepalive:us-central1"
  "trading-bot-keepalive:europe-west1"
)

# القيم الجديدة بعد التدوير (لا تضعها هنا لفترة طويلة — احذفها من الملف بعد التنفيذ)
NEW_TELEGRAM_BOT_TOKEN="8624993506:AAHeDw-ZUkZ1fkU3Z5H6l3KFwnr118yms7I"
NEW_NVIDIA_API_KEY="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"
NEW_FINNHUB_API_KEY="dae8079r01ql3jf9a350dae8079r01ql3jf9a35g"
NEW_CTRADER_CLIENT_SECRET="ZIWSeYB8Ug83R7tGHaPgpBnheCaeQkCZ92KUtONg8GDAoxMTp2"
NEW_CTRADER_ACCESS_TOKEN="fvPWfwBJgna0TjfuF3hdM0LpG3dC_PwKFStCXUROO4g"
# لم نعد نستخدم Neon/PostgreSQL — التخزين على Firestore + BigQuery
NEW_DATABASE_URL=""

# متغيرات غير حساسة
CTRADER_HOST="demo.ctraderapi.com"
CTRADER_PORT="5035"
CTRADER_CLIENT_ID="39132_uXg6OJABXDCNo0yvcotHQqb650jDrQlSPHPglKcBVHRolckOgl"
CTRADER_ACCOUNT_ID="46805030"
MY_TELEGRAM_CHAT_ID="8624993506"
# always-on: min-instances=1 + no-cpu-throttling كي لا تنام النسخة ويعمل APScheduler 24/7
CHEAP_CLOUD_RUN="0"
NVIDIA_MODEL="nvidia/nemotron-3-super-120b-a12b"
# تحليل NVIDIA كل 5 دقائق + OHLC كل 60 ثانية في الوضع غير الاقتصادي
NON_CHEAP_ANALYSIS_INTERVAL_MINUTES="5"
NON_CHEAP_OHLC_REFRESH_SECONDS="60"
AUTO_ANALYSIS_INTERVAL_MINUTES="5"
# تخزين: Firestore (رئيسي) + BigQuery (OHLC ثقيل)
USE_FIRESTORE="1"
FIRESTORE_PROJECT="${PROJECT_ID}"
BIGQUERY_PROJECT="${PROJECT_ID}"
BIGQUERY_DATASET="trading_bot"
BIGQUERY_LOCATION="EU"
# ==========================================================================

echo "==> ضبط المشروع الحالي"
gcloud config set project "${PROJECT_ID}"

echo "==> تفعيل الخدمات المطلوبة"
gcloud services enable run.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com firestore.googleapis.com bigquery.googleapis.com cloudscheduler.googleapis.com

echo "==> التأكد من وجود قاعدة بيانات Firestore (Native mode)"
gcloud firestore databases create --location=eur3 --type=firestore-native 2>/dev/null || \
  echo "   (قاعدة Firestore موجودة بالفعل — تجاهل)"

echo "==> حذف كل خدمات Cloud Run القديمة بكل المناطق المعروفة"
for r in "${OLD_SERVICE_REGIONS[@]}"; do
  gcloud run services delete "${SERVICE_NAME}" --region "${r}" --quiet || \
    echo "   (لا توجد خدمة بمنطقة ${r} — تجاهل)"
done

echo "==> حذف كل مهام Cloud Scheduler القديمة"
for entry in "${OLD_SCHEDULER_JOBS[@]}"; do
  job_name="${entry%%:*}"
  job_region="${entry##*:}"
  gcloud scheduler jobs delete "${job_name}" --location "${job_region}" --quiet || \
    echo "   (لا توجد مهمة ${job_name} بمنطقة ${job_region} — تجاهل)"
done

# ---------- دالة مساعدة: إنشاء أو تحديث سر في Secret Manager ----------
upsert_secret () {
  local name="$1"
  local value="$2"
  if [ -z "${value}" ]; then
    echo "   تخطي ${name} (قيمة فارغة)"
    return
  fi
  if gcloud secrets describe "${name}" >/dev/null 2>&1; then
    echo -n "${value}" | gcloud secrets versions add "${name}" --data-file=-
    echo "   ↻ تحديث نسخة جديدة للسر ${name}"
  else
    echo -n "${value}" | gcloud secrets create "${name}" --data-file=-
    echo "   + إنشاء السر ${name}"
  fi
}

echo "==> تجهيز الأسرار في Secret Manager"
upsert_secret "TELEGRAM_BOT_TOKEN"     "${NEW_TELEGRAM_BOT_TOKEN}"
upsert_secret "NVIDIA_API_KEY"         "${NEW_NVIDIA_API_KEY}"
upsert_secret "FINNHUB_API_KEY"        "${NEW_FINNHUB_API_KEY}"
upsert_secret "CTRADER_CLIENT_SECRET"  "${NEW_CTRADER_CLIENT_SECRET}"
upsert_secret "CTRADER_ACCESS_TOKEN"   "${NEW_CTRADER_ACCESS_TOKEN}"
# لا حاجة لـ DATABASE_URL بعد حذف Neon/PostgreSQL

# ملاحظة مهمة: القيم مفصولة بفاصلة "," بدون مسافات حول "=" أو ","،
# هذا بالضبط ما كان ناقصاً في النشر السابق وسبب تلاصق TELEGRAM_BOT_TOKEN بمتغيرات أخرى.
# وضع always-on: min-instances=1 + no-cpu-throttling كي لا تنام النسخة أبداً
# ويعمل APScheduler 24/7 (الوضع غير الاقتصادي) بدل الاعتماد على Cloud Scheduler.
echo "==> نشر الخدمة (always-on — منطقة واحدة فقط: ${REGION})"
gcloud run deploy "${SERVICE_NAME}" \
  --source . \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --no-cpu-throttling \
  --min-instances=1 \
  --max-instances=1 \
  --memory=1Gi \
  --cpu=1 \
  --timeout=300 \
  --concurrency=40 \
  --set-env-vars="CHEAP_CLOUD_RUN=${CHEAP_CLOUD_RUN},CTRADER_HOST=${CTRADER_HOST},CTRADER_PORT=${CTRADER_PORT},CTRADER_CLIENT_ID=${CTRADER_CLIENT_ID},CTRADER_ACCOUNT_ID=${CTRADER_ACCOUNT_ID},MY_TELEGRAM_CHAT_ID=${MY_TELEGRAM_CHAT_ID},NVIDIA_MODEL=${NVIDIA_MODEL},NON_CHEAP_ANALYSIS_INTERVAL_MINUTES=${NON_CHEAP_ANALYSIS_INTERVAL_MINUTES},NON_CHEAP_OHLC_REFRESH_SECONDS=${NON_CHEAP_OHLC_REFRESH_SECONDS},AUTO_ANALYSIS_INTERVAL_MINUTES=${AUTO_ANALYSIS_INTERVAL_MINUTES},USE_FIRESTORE=${USE_FIRESTORE},FIRESTORE_PROJECT=${FIRESTORE_PROJECT},BIGQUERY_PROJECT=${BIGQUERY_PROJECT},BIGQUERY_DATASET=${BIGQUERY_DATASET},BIGQUERY_LOCATION=${BIGQUERY_LOCATION}" \
  --set-secrets="TELEGRAM_BOT_TOKEN=TELEGRAM_BOT_TOKEN:latest,NVIDIA_API_KEY=NVIDIA_API_KEY:latest,FINNHUB_API_KEY=FINNHUB_API_KEY:latest,CTRADER_CLIENT_SECRET=CTRADER_CLIENT_SECRET:latest,CTRADER_ACCESS_TOKEN=CTRADER_ACCESS_TOKEN:latest"

echo "==> جلب رابط الخدمة الفعلي"
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" --region "${REGION}" --format='value(status.url)')
echo "   الرابط: ${SERVICE_URL}"

echo "==> تحديث SERVICE_URL على الخدمة (ضروري لضبط webhook تيليجرام بشكل صحيح)"
gcloud run services update "${SERVICE_NAME}" \
  --region "${REGION}" \
  --update-env-vars="SERVICE_URL=${SERVICE_URL}"

echo "==> منح حساب الخدمة صلاحية Firestore (datastore.user)"
RUN_SA=$(gcloud run services describe "${SERVICE_NAME}" --region "${REGION}" --format='value(spec.template.spec.serviceAccountName)' 2>/dev/null || echo "")
if [ -n "${RUN_SA}" ]; then
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${RUN_SA}" \
    --role="roles/datastore.user" 2>/dev/null || \
    echo "   (الصلاحية موجودة أو تعذّر إضافتها — تحقق من IAM)"
else
  echo "   (تعذّر جلب حساب الخدمة — أضف roles/datastore.user يدوياً لحساب الخدمة)"
fi

echo "==> عرض آخر السجلات للتأكد من ضبط الـ Webhook"
sleep 10
gcloud run services logs read "${SERVICE_NAME}" --region "${REGION}" --limit=50

echo ""
echo "✅ انتهى. تحقق من السطر: 'Telegram Webhook set to: ${SERVICE_URL}/telegram/webhook/...'"
echo "   ثم افتح البوت في تيليجرام وأرسل /start."
