# ==========================================================
# deploy.ps1 - full cleanup (2 services + 3 scheduler jobs) then one clean deploy
# ==========================================================
# Usage:
#   1) Edit the CONFIG section below (new token after /revoke in BotFather).
#   2) From the project folder (next to main.py and Dockerfile), in PowerShell:
#        Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#        .\deploy.ps1

$ErrorActionPreference = "Stop"

# ==================== CONFIG (edit only this section) ====================
$PROJECT_ID = "zeghoudi-trade-ai"
$REGION = "europe-west1"              # region for the single new service
$SERVICE_NAME = "trading-bot"

# Old regions that may still have a Cloud Run service to delete
$OLD_SERVICE_REGIONS = @("europe-west1", "us-central1")

# Old Cloud Scheduler jobs that exist today (name, region)
$OLD_SCHEDULER_JOBS = @(
    @{ Name = "trading-bot-tick"; Region = "us-central1" },
    @{ Name = "trading-bot-keepalive"; Region = "us-central1" },
    @{ Name = "trading-bot-keepalive"; Region = "europe-west1" }
)

# القيم الجديدة بعد التدوير — تُقرأ من متغيرات البيئة، لا تُكتب في الملف أبداً.
# مرّرها هكذا قبل تشغيل السكربت (PowerShell):
#   $env:NEW_TELEGRAM_BOT_TOKEN="..."; $env:NEW_NVIDIA_API_KEY="..."; ./deploy.ps1
# أو اتركها فارغة لتخطّي إنشاء الأسرار (تُعاد استخدام الموجودة في Secret Manager).
$NEW_TELEGRAM_BOT_TOKEN     = $env:NEW_TELEGRAM_BOT_TOKEN
$NEW_NVIDIA_API_KEY         = $env:NEW_NVIDIA_API_KEY
$NEW_FINNHUB_API_KEY        = $env:NEW_FINNHUB_API_KEY
$NEW_CTRADER_CLIENT_SECRET  = $env:NEW_CTRADER_CLIENT_SECRET
$NEW_CTRADER_ACCESS_TOKEN   = $env:NEW_CTRADER_ACCESS_TOKEN
# لم نعد نستخدم Neon/PostgreSQL — التخزين على Firestore + BigQuery

# Non-sensitive variables
$CTRADER_HOST        = "demo.ctraderapi.com"
$CTRADER_PORT        = "5035"
$CTRADER_CLIENT_ID   = "39132_uXg6OJABXDCNo0yvcotHQqb650jDrQlSPHPglKcBVHRolckOgl"
$CTRADER_ACCOUNT_ID  = "46805030"
$MY_TELEGRAM_CHAT_ID = "8624993506"
# always-on: min-instances=1 + no-cpu-throttling كي لا تنام النسخة ويعمل APScheduler 24/7
$CHEAP_CLOUD_RUN     = "0"
$NVIDIA_MODEL        = "nvidia/nemotron-3-super-120b-a12b"
# تحليل NVIDIA كل 5 دقائق + OHLC كل 60 ثانية في الوضع غير الاقتصادي
$NON_CHEAP_ANALYSIS_INTERVAL_MINUTES = "5"
$NON_CHEAP_OHLC_REFRESH_SECONDS      = "60"
$AUTO_ANALYSIS_INTERVAL_MINUTES      = "5"
# تخزين: Firestore (رئيسي) + BigQuery (OHLC ثقيل)
$USE_FIRESTORE       = "1"
$FIRESTORE_PROJECT   = $PROJECT_ID
$BIGQUERY_PROJECT    = $PROJECT_ID
$BIGQUERY_DATASET    = "trading_bot"
$BIGQUERY_LOCATION   = "EU"
# ==========================================================================

Write-Host "==> Setting active project"
gcloud config set project $PROJECT_ID

Write-Host "==> Enabling required services"
gcloud services enable run.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com firestore.googleapis.com bigquery.googleapis.com cloudscheduler.googleapis.com

Write-Host "==> Ensuring a Firestore database exists (Native mode)"
try {
    gcloud firestore databases create --location=eur3 --type=firestore-native | Out-Null
} catch {
    Write-Host "   (Firestore database already exists - skipping)"
}

Write-Host "==> Deleting old Cloud Run services in all known regions"
foreach ($r in $OLD_SERVICE_REGIONS) {
    try {
        gcloud run services delete $SERVICE_NAME --region $r --quiet
    } catch {
        Write-Host "   (no service in region $r - skipping)"
    }
}

Write-Host "==> Deleting old Cloud Scheduler jobs"
foreach ($job in $OLD_SCHEDULER_JOBS) {
    $jobName = $job.Name
    $jobRegion = $job.Region
    try {
        gcloud scheduler jobs delete $jobName --location $jobRegion --quiet
    } catch {
        Write-Host "   (no job $jobName in region $jobRegion - skipping)"
    }
}

# ---------- helper: create or update a secret in Secret Manager ----------
function Upsert-Secret {
    param(
        [string]$Name,
        [string]$Value
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        Write-Host "   skipping $Name (empty value)"
        return
    }
    $exists = $true
    try {
        gcloud secrets describe $Name | Out-Null
    } catch {
        $exists = $false
    }
    # كتابة بدون سطر جديد لتجنب \r\n في التوكنات (سبب InvalidURL سابقاً)
    $tmp = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($tmp, $Value.Trim())
        if ($exists) {
            gcloud secrets versions add $Name --data-file=$tmp
            Write-Host "   updated secret $Name with a new version"
        } else {
            gcloud secrets create $Name --data-file=$tmp
            Write-Host "   created secret $Name"
        }
    } finally {
        Remove-Item -Force $tmp -ErrorAction SilentlyContinue
    }
}

Write-Host "==> Preparing secrets in Secret Manager"
Upsert-Secret "TELEGRAM_BOT_TOKEN"    $NEW_TELEGRAM_BOT_TOKEN
Upsert-Secret "NVIDIA_API_KEY"        $NEW_NVIDIA_API_KEY
Upsert-Secret "FINNHUB_API_KEY"       $NEW_FINNHUB_API_KEY
Upsert-Secret "CTRADER_CLIENT_SECRET" $NEW_CTRADER_CLIENT_SECRET
Upsert-Secret "CTRADER_ACCESS_TOKEN"  $NEW_CTRADER_ACCESS_TOKEN
# لا حاجة لـ DATABASE_URL بعد حذف Neon/PostgreSQL

# Note: values are comma-separated with no spaces around "=" or ",".
# This is exactly what was missing in the earlier deploy and caused
# TELEGRAM_BOT_TOKEN to get glued to other variables.
# always-on: min-instances=1 + no-cpu-throttling so the instance never sleeps
# and APScheduler runs 24/7 (non-cheap mode) instead of relying on Cloud Scheduler.
$envVars = "CHEAP_CLOUD_RUN=$CHEAP_CLOUD_RUN,CTRADER_HOST=$CTRADER_HOST,CTRADER_PORT=$CTRADER_PORT,CTRADER_CLIENT_ID=$CTRADER_CLIENT_ID,CTRADER_ACCOUNT_ID=$CTRADER_ACCOUNT_ID,MY_TELEGRAM_CHAT_ID=$MY_TELEGRAM_CHAT_ID,NVIDIA_MODEL=$NVIDIA_MODEL,NON_CHEAP_ANALYSIS_INTERVAL_MINUTES=$NON_CHEAP_ANALYSIS_INTERVAL_MINUTES,NON_CHEAP_OHLC_REFRESH_SECONDS=$NON_CHEAP_OHLC_REFRESH_SECONDS,AUTO_ANALYSIS_INTERVAL_MINUTES=$AUTO_ANALYSIS_INTERVAL_MINUTES,USE_FIRESTORE=$USE_FIRESTORE,FIRESTORE_PROJECT=$FIRESTORE_PROJECT,BIGQUERY_PROJECT=$BIGQUERY_PROJECT,BIGQUERY_DATASET=$BIGQUERY_DATASET,BIGQUERY_LOCATION=$BIGQUERY_LOCATION"
$secretRefs = "TELEGRAM_BOT_TOKEN=TELEGRAM_BOT_TOKEN:latest,NVIDIA_API_KEY=NVIDIA_API_KEY:latest,FINNHUB_API_KEY=FINNHUB_API_KEY:latest,CTRADER_CLIENT_SECRET=CTRADER_CLIENT_SECRET:latest,CTRADER_ACCESS_TOKEN=CTRADER_ACCESS_TOKEN:latest"

Write-Host "==> Deploying the service (always-on - single region: $REGION)"
gcloud run deploy $SERVICE_NAME `
    --source . `
    --region $REGION `
    --platform managed `
    --allow-unauthenticated `
    --no-cpu-throttling `
    --min-instances=1 `
    --max-instances=1 `
    --memory=1Gi `
    --cpu=1 `
    --timeout=300 `
    --concurrency=40 `
    --set-env-vars=$envVars `
    --set-secrets=$secretRefs

Write-Host "==> Fetching the actual service URL"
$SERVICE_URL = gcloud run services describe $SERVICE_NAME --region $REGION --format="value(status.url)"
Write-Host "   URL: $SERVICE_URL"

Write-Host "==> Updating SERVICE_URL on the service (required for the Telegram webhook)"
gcloud run services update $SERVICE_NAME --region $REGION --update-env-vars="SERVICE_URL=$SERVICE_URL"

Write-Host "==> Granting the service account Firestore access (datastore.user)"
$RUN_SA = $null
try {
    $RUN_SA = gcloud run services describe $SERVICE_NAME --region $REGION --format="value(spec.template.spec.serviceAccountName)"
} catch {
    Write-Host "   (could not fetch the service account - add roles/datastore.user manually)"
}
if ($RUN_SA) {
    try {
        gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$RUN_SA" --role="roles/datastore.user" | Out-Null
    } catch {
        Write-Host "   (binding exists or could not be added - check IAM)"
    }
}

Write-Host "==> Showing recent logs to confirm the webhook was set"
Start-Sleep -Seconds 10
gcloud run services logs read $SERVICE_NAME --region $REGION --limit=50

Write-Host ""
Write-Host "DONE. Check for the line: Telegram Webhook set to: $SERVICE_URL/telegram/webhook/..."
Write-Host "Then open the bot in Telegram and send /start."
