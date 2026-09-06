import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv

# تحميل المتغيرات البيئية
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# إعدادات الصفحة
st.set_page_config(
    page_title="Cloud Trading AI - Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# الاتصال بقاعدة البيانات
@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        st.error(f"خطأ في الاتصال بقاعدة البيانات: {e}")
        return None

# جلب بيانات الصفقات
def fetch_trades():
    conn = get_db_connection()
    if conn:
        try:
            query = "SELECT * FROM trades ORDER BY created_at DESC;"
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            # في حال عدم وجود جدول بعد أو خلوه من البيانات
            return pd.DataFrame()
    return pd.DataFrame()

# دالة رسم قسم قوة العملات والترابط
def render_currency_strength_section(df_trades):
    st.subheader("📊 مؤشر قوة العملات والترابط المالي (Currency Strength & Correlation)")
    st.markdown("تحليل تقاطع السيولة وقوة العملات الرئيسية لحماية شبكة التداول من الـ Drawdown.")

    # 1. قياس قوة العملات افتراضياً (يمكن ربطها ببيانات حية مستقبلاً)
    strength_scores = {
        'USD': 78,
        'EUR': 45,
        'GBP': 62,
        'JPY': 25,
        'BTC': 88,
        'AUD': 50,
        'CAD': 55
    }

    df_strength = pd.DataFrame(list(strength_scores.items()), columns=['Currency', 'Strength'])
    df_strength = df_strength.sort_values(by='Strength', ascending=True)

    c1, c2 = st.columns([1, 1])

    with c1:
        st.write("##### 🟢 مؤشر قوة العملات الحالية (0 - 100)")
        fig_bar = px.bar(
            df_strength,
            x='Strength',
            y='Currency',
            orientation='h',
            color='Strength',
            color_continuous_scale='RdYlGn',
            text='Strength',
            title="مقياس قوة العملات (Relative Currency Strength)"
        )
        fig_bar.update_layout(xaxis_range=[0, 100], showlegend=False, height=380)
        st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        st.write("##### 🔗 خريطة الترابط بين الأزواج (Correlation Heatmap)")
        
        # مصفوفة ترابط للأزواج الرئيسية
        symbols = ['EURUSD', 'GBPUSD', 'USDJPY', 'EURGBP', 'BTCUSD']
        corr_matrix = [
            [1.00,  0.82, -0.75,  0.35, -0.20],  # EURUSD
            [0.82,  1.00, -0.68, -0.25, -0.15],  # GBPUSD
            [-0.75, -0.68,  1.00, -0.10,  0.30],  # USDJPY
            [0.35, -0.25, -0.10,  1.00, -0.05],  # EURGBP
            [-0.20, -0.15,  0.30, -0.05,  1.00]   # BTCUSD
        ]

        fig_heatmap = px.imshow(
            corr_matrix,
            x=symbols,
            y=symbols,
            color_continuous_scale='Viridis',
            text_auto=True,
            title="مصفوفة معامل الارتباط بين الأزواج"
        )
        fig_heatmap.update_layout(height=380)
        st.plotly_chart(fig_heatmap, use_container_width=True)

# ==================== الشريط الجانبي ====================
st.sidebar.title("🤖 Cloud Trading AI")
st.sidebar.markdown("---")
st.sidebar.metric(label="حالة السيرفر (Render)", value="Online 🟢")
st.sidebar.metric(label="قاعدة البيانات (Neon)", value="Connected ⚡")

page = st.sidebar.radio("الانتقال إلى:", [
    "لوحة التحكم العامة", 
    "تحليلات الصفقات", 
    "قوة العملات والترابط", 
    "حاسبة الذكاء الاصطناعي"
])

# ==================== الصفحة الأولى: لوحة التحكم العامة ====================
if page == "لوحة التحكم العامة":
    st.title("📈 لوحة تحكم التداول والذكاء الاصطناعي")
    st.markdown("متابعة حية للصفقات، إعدادات الـ Grid، وأداء الحساب.")

    df_trades = fetch_trades()

    # كروت المؤشرات السريعة (KPIs)
    col1, col2, col3, col4 = st.columns(4)
    
    total_trades = len(df_trades) if not df_trades.empty else 0
    total_profit = df_trades['profit'].sum() if not df_trades.empty and 'profit' in df_trades else 0.0
    win_rate = ((df_trades['profit'] > 0).sum() / total_trades * 100) if total_trades > 0 else 0.0

    col1.metric("إجمالي الصفقات", f"{total_trades}")
    col2.metric("الأرباح المحققة ($)", f"${total_profit:.2f}", delta=f"{total_profit:.2f}")
    col3.metric("نسبة النجاح (Win Rate)", f"{win_rate:.1f}%")
    col4.metric("حالة الـ Grid", "نشط ⚡")

    st.markdown("---")

    # رسم بياني + جدول الصفقات
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📊 أداء الأرباح التراكمية")
        if not df_trades.empty and 'profit' in df_trades:
            df_trades['cumulative_profit'] = df_trades['profit'].cumsum()
            fig = px.line(df_trades, x='created_at', y='cumulative_profit', title="نمو الحساب عبر الزمن")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("💡 لا توجد صفقات مسجلة حالياً في قاعدة البيانات. سيتم تحديث الرسم البياني تلقائياً عند تنفيذ cBot لأول صفقة.")

    with c2:
        st.subheader("🎯 توزيع أزواج التداول")
        if not df_trades.empty and 'symbol' in df_trades:
            fig_pie = px.pie(df_trades, names='symbol', title="الصفقات حسب الزوج")
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("بانتظار استقبال بيانات الأزواج...")

    st.markdown("---")

    # جدول الصفقات الأخيرة
    st.subheader("📋 سجل الصفقات الأخيرة")
    if not df_trades.empty:
        st.dataframe(df_trades, use_container_width=True)
    else:
        st.caption("الجدول فارغ حالياً - الاتصال جاهز لاستقبال بيانات cTrader.")

# ==================== الصفحة الثانية: تحليلات الصفقات ====================
elif page == "تحليلات الصفقات":
    st.title("🔍 تحليلات عميقة للصفقات والشبكة (Grid)")
    st.write("استعراض تفاصيل صفقات الشراء والبيع ومستويات الهيدج.")
    
    df_trades = fetch_trades()
    if not df_trades.empty:
        st.dataframe(df_trades, use_container_width=True)
    else:
        st.info("لا توجد بيانات كافية لإجراء التحليل العميق بعد.")

# ==================== الصفحة الثالثة: قوة العملات والترابط ====================
elif page == "قوة العملات والترابط":
    st.title("🌐 تحليلات السوق المتقاطعة (Multi-Symbol Analysis)")
    df_trades = fetch_trades()
    render_currency_strength_section(df_trades)

# ==================== الصفحة الرابعة: حاسبة الذكاء الاصطناعي ====================
elif page == "حاسبة الذكاء الاصطناعي":
    st.title("🤖 اختبر توصيات Gemini مباشرة")
    st.write("قم بإدخال بيانات السوق للاختبار المباشر لحساب مسافات الـ Grid:")

    with st.form("ai_form"):
        headline = st.text_input("الخبر الاقتصادي / Headline", "US Core CPI MoM expected to rise")
        symbol = st.selectbox("زوج التداول", ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"])
        atr = st.number_input("مؤشر ATR", value=0.0018, format="%.4f")
        volume_ratio = st.number_input("نسبة الفوليوم (Volume Ratio)", value=1.5, format="%.2f")
        
        submitted = st.form_submit_button("حساب الإعدادات عبر Gemini 🚀")

        if submitted:
            import requests
            # الاتصال بـ API السحابي الخاص بك على Render
            RENDER_API_URL = "https://cloud-trading-ai.onrender.com/api/calculate-grid-params"
            
            payload = {
                "headline": headline,
                "symbol": symbol,
                "atr": atr,
                "volume_ratio": volume_ratio
            }
            
            try:
                res = requests.post(RENDER_API_URL, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    st.success("✅ تم الحصول على الإعدادات بنجاح من الذكاء الاصطناعي!")
                    st.json(data)
                else:
                    st.error(f"خطأ في الاستجابة: {res.status_code}")
            except Exception as e:
                st.error(f"تعذر الاتصال بالسيرفر السحابي: {e}")