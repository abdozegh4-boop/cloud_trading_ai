# ==========================================================
# deploy-run.ps1 — نشر تحديثات الكود على Cloud Run فقط
# ==========================================================
# يعيد استخدام الأسرار/الإعدادات الموجودة في الخدمة (لا يكتب أسراراً في الملف).
# الاستخدام من نفس مجلد المشروع (بجانب main.py و Dockerfile):
#   ./deploy-run.ps1
#
# لفرض نشر clean (إعادة بناء الصورة بالكامل): ./deploy-run.ps1 -Clean

param(
  [switch]$Clean
)

$ErrorActionPreference = "Stop"

# ==================== الإعدادات ====================
$PROJECT_ID  = "zeghoudi-trade-ai"
$REGION      = "europe-west1"
$SERVICE     = "trading-bot"
$TAG         = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
$IMAGE       = "gcr.io/$PROJECT_ID/$SERVICE" + ":$TAG"
# ====================================================

Write-Host "==> ضبط المشروع الحالي" -ForegroundColor Cyan
gcloud config set project $PROJECT_ID

if ($Clean) {
  Write-Host "==> وضع clean: بناء الصورة محلياً ورفعها إلى Artifact Registry" -ForegroundColor Cyan
  gcloud services enable artifactregistry.googleapis.com 2>$null | Out-Null
  $repoExists = $true
  try { gcloud artifacts repositories describe $SERVICE --location $REGION | Out-Null }
  catch { $repoExists = $false }
  if (-not $repoExists) {
    gcloud artifacts repositories create $SERVICE --location $REGION --repository-format docker
  }
  docker build -t $IMAGE .
  if ($LASTEXITCODE -ne 0) { Write-Host "[X] فشل بناء الصورة" -ForegroundColor Red; exit 1 }
  gcloud auth configure-docker gcr.io --quiet
  docker push $IMAGE
  if ($LASTEXITCODE -ne 0) { Write-Host "[X] فشل رفع الصورة" -ForegroundColor Red; exit 1 }

  Write-Host "==> نشر الصورة المبنية على Cloud Run" -ForegroundColor Cyan
  gcloud run deploy $SERVICE `
    --image $IMAGE `
    --region $REGION `
    --platform managed `
    --allow-unauthenticated `
    --no-cpu-throttling `
    --min-instances=1 `
    --max-instances=1 `
    --memory=1Gi `
    --cpu=1 `
    --timeout=300 `
    --concurrency=40
}
else {
  Write-Host "==> نشر سريع من المصدر (يحتفظ بكل الأسرار/الإعدادات الحالية)" -ForegroundColor Cyan
  gcloud run deploy $SERVICE `
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
    --concurrency=40
}

if ($LASTEXITCODE -ne 0) { Write-Host "[X] فشل النشر" -ForegroundColor Red; exit 1 }

Write-Host "==> جلب رابط الخدمة الفعلي" -ForegroundColor Cyan
$SERVICE_URL = (gcloud run services describe $SERVICE --region $REGION --format='value(status.url)').Trim()
Write-Host "   الرابط: $SERVICE_URL" -ForegroundColor Green

Write-Host "==> تحديث SERVICE_URL على الخدمة (ضروري لضبط webhook تيليجرام)" -ForegroundColor Cyan
gcloud run services update $SERVICE --region $REGION --update-env-vars="SERVICE_URL=$SERVICE_URL"

Write-Host "==> عرض آخر السجلات للتأكد من ضبط الـ Webhook" -ForegroundColor Cyan
Start-Sleep -Seconds 8
gcloud run services logs read $SERVICE --region $REGION --limit=40

Write-Host ""
Write-Host "[OK] انتهى. تحقق من السطر: 'Telegram Webhook set to: $SERVICE_URL/telegram/webhook/...'" -ForegroundColor Green
Write-Host "   ثم افتح البوت في تيليجرام وأرسل /start." -ForegroundColor Green
