"""
=============================================================================
  PROJETO: Impacto do Clima nas Vendas do E-commerce Brasileiro (Olist)
  Problema de negócio: "Como condições climáticas afetam o volume e o ticket
  médio de vendas nas diferentes regiões e categorias do Brasil?"
  
  Fontes de dados:
    - Kaggle: Brazilian E-Commerce Public Dataset by Olist
    - OpenWeather API: Geocoding + dados climáticos
    - Open-Meteo API: Dados históricos gratuitos (2016-2018) [fallback]
  
  Saída: CSVs prontos para importar no Power BI
=============================================================================
"""

# =============================================================================
# 0. INSTALAÇÃO (rode no terminal antes de executar o script)
# pip install pandas requests tqdm kaggle geopy pyarrow openpyxl python-dotenv
# =============================================================================

import os
import json
import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env automaticamente
load_dotenv()

# =============================================================================
# 1. CONFIGURAÇÃO — as chaves vêm do arquivo .env (nunca coloque aqui!)
# =============================================================================

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
KAGGLE_USERNAME     = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY          = os.getenv("KAGGLE_KEY")

# Valida se as chaves foram carregadas corretamente
missing = [k for k, v in {
    "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
    "KAGGLE_USERNAME":     KAGGLE_USERNAME,
    "KAGGLE_KEY":          KAGGLE_KEY
}.items() if not v]

if missing:
    raise EnvironmentError(
        f"\n[ERRO] Chaves não encontradas no .env: {', '.join(missing)}\n"
        f"       Verifique se o arquivo .env existe na raiz do projeto\n"
        f"       e se as variáveis estão preenchidas corretamente."
    )

DATA_DIR    = Path("data")
OUTPUT_DIR  = Path("output_powerbi")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# Limite de cidades para geocoding (use None para todas; ~4.000 cidades únicas)
# Para teste inicial, use 50. Para produção, use None.
MAX_CITIES = None  

print("=" * 60)
print("  Olist + OpenWeather Pipeline")
print("=" * 60)


# =============================================================================
# 2. DOWNLOAD DO DATASET OLIST VIA KAGGLE API
# =============================================================================

def download_olist():
    """
    Faz download do dataset Olist do Kaggle.
    Alternativa manual: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    """
    olist_dir = DATA_DIR / "olist"
    if olist_dir.exists() and any(olist_dir.glob("*.csv")):
        print("[OK] Dataset Olist já baixado.")
        return

    print("\n[1/5] Baixando dataset Olist do Kaggle...")
    os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
    os.environ["KAGGLE_KEY"]      = KAGGLE_KEY

    try:
        import kaggle
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            "olistbr/brazilian-ecommerce",
            path=str(olist_dir),
            unzip=True
        )
        print(f"[OK] Olist salvo em {olist_dir}")
    except Exception as e:
        print(f"[ERRO] Kaggle download falhou: {e}")
        print("       Faça o download manual em:")
        print("       https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print("       e salve os CSVs em data/olist/")


# =============================================================================
# 3. CARREGAMENTO E EXPLORAÇÃO DOS DADOS OLIST
# =============================================================================

def load_olist():
    """Carrega e faz o merge dos principais DataFrames do Olist."""
    print("\n[2/5] Carregando dados Olist...")
    olist_dir = DATA_DIR / "olist"

    # --- Pedidos ---
    orders = pd.read_csv(olist_dir / "olist_orders_dataset.csv",
                         parse_dates=[
                             "order_purchase_timestamp",
                             "order_approved_at",
                             "order_delivered_customer_date",
                             "order_estimated_delivery_date"
                         ])

    # --- Itens dos pedidos (preço e frete) ---
    items = pd.read_csv(olist_dir / "olist_order_items_dataset.csv")

    # --- Produtos e categorias ---
    products = pd.read_csv(olist_dir / "olist_products_dataset.csv")
    categories = pd.read_csv(olist_dir / "olist_product_category_name_translation.csv")
    products = products.merge(categories, on="product_category_name", how="left")

    # --- Clientes com cidade/estado ---
    customers = pd.read_csv(olist_dir / "olist_customers_dataset.csv")

    # --- Pagamentos ---
    payments = pd.read_csv(olist_dir / "olist_order_payments_dataset.csv")
    payments_agg = payments.groupby("order_id").agg(
        total_payment=("payment_value", "sum"),
        installments=("payment_installments", "max"),
        payment_type=("payment_type", lambda x: x.mode().iloc[0])
    ).reset_index()

    # --- Avaliações ---
    reviews = pd.read_csv(olist_dir / "olist_order_reviews_dataset.csv")
    reviews_agg = reviews.groupby("order_id").agg(
        review_score=("review_score", "mean")
    ).reset_index()

    # --- Merge principal ---
    # Receita por pedido
    revenue_per_order = items.groupby("order_id").agg(
        order_revenue=("price", "sum"),
        order_freight=("freight_value", "sum"),
        items_count=("order_item_id", "count")
    ).reset_index()

    # Categoria dominante por pedido
    items_with_product = items.merge(products[["product_id",
                                               "product_category_name_english"]],
                                     on="product_id", how="left")
    category_per_order = items_with_product.groupby("order_id")[
        "product_category_name_english"
    ].agg(lambda x: x.mode().iloc[0] if (not x.empty and not x.mode().empty) else "unknown").reset_index()
    category_per_order.columns = ["order_id", "main_category"]

    # Junta tudo
    df = (orders
          .merge(customers, on="customer_id", how="left")
          .merge(revenue_per_order, on="order_id", how="left")
          .merge(category_per_order, on="order_id", how="left")
          .merge(payments_agg, on="order_id", how="left")
          .merge(reviews_agg, on="order_id", how="left")
         )

    # --- Features de tempo ---
    df["purchase_date"]  = df["order_purchase_timestamp"].dt.date
    df["purchase_year"]  = df["order_purchase_timestamp"].dt.year
    df["purchase_month"] = df["order_purchase_timestamp"].dt.month
    df["purchase_dow"]   = df["order_purchase_timestamp"].dt.day_name()
    df["purchase_hour"]  = df["order_purchase_timestamp"].dt.hour
    df["is_weekend"]     = df["order_purchase_timestamp"].dt.dayofweek >= 5

    # Dia de entrega vs estimado
    df["delivery_delay_days"] = (
        df["order_delivered_customer_date"] -
        df["order_estimated_delivery_date"]
    ).dt.days

    # Apenas pedidos entregues
    df_delivered = df[df["order_status"] == "delivered"].copy()

    print(f"[OK] {len(df_delivered):,} pedidos entregues carregados.")
    print(f"     Período: {df_delivered['purchase_date'].min()} → "
          f"{df_delivered['purchase_date'].max()}")
    print(f"     Cidades únicas: {df_delivered['customer_city'].nunique():,}")

    return df_delivered


# =============================================================================
# 4. GEOCODING: CIDADE → LATITUDE / LONGITUDE (OpenWeather Geocoding API)
# =============================================================================

def geocode_cities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Usa a OpenWeather Geocoding API (GRATUITA) para obter lat/lon
    de cada cidade brasileira presente no dataset.
    
    Endpoint: http://api.openweathermap.org/geo/1.0/direct
    """
    print("\n[3/5] Geocodificando cidades via OpenWeather...")

    cache_path = DATA_DIR / "city_coords.json"

    # Carrega cache existente
    if cache_path.exists():
        with open(cache_path) as f:
            coord_cache = json.load(f)
    else:
        coord_cache = {}

    cities = df["customer_city"].dropna().unique().tolist()
    if MAX_CITIES:
        cities = cities[:MAX_CITIES]

    cities_to_fetch = [c for c in cities if c not in coord_cache]
    print(f"   Total cidades: {len(cities)} | A buscar: {len(cities_to_fetch)}")

    for city in tqdm(cities_to_fetch, desc="Geocoding"):
        # Normaliza nome (Olist usa minúsculas)
        city_query = city.title()
        url = (
            f"http://api.openweathermap.org/geo/1.0/direct"
            f"?q={city_query},BR&limit=1&appid={OPENWEATHER_API_KEY}"
        )
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if data:
                coord_cache[city] = {"lat": data[0]["lat"], "lon": data[0]["lon"]}
            else:
                coord_cache[city] = None  # cidade não encontrada
        except Exception as e:
            coord_cache[city] = None
        time.sleep(0.1)  # respeita rate limit (60 req/min no plano free)

    # Salva cache
    with open(cache_path, "w") as f:
        json.dump(coord_cache, f, ensure_ascii=False, indent=2)

    # Adiciona lat/lon ao DataFrame
    df["lat"] = df["customer_city"].map(
        lambda c: coord_cache.get(c, {}).get("lat") if coord_cache.get(c) else None
    )
    df["lon"] = df["customer_city"].map(
        lambda c: coord_cache.get(c, {}).get("lon") if coord_cache.get(c) else None
    )

    found = df["lat"].notna().sum()
    print(f"[OK] Lat/lon encontradas para {found:,}/{len(df):,} pedidos.")
    return df


# =============================================================================
# 5a. OPÇÃO A — OpenWeather API (dados atuais / paid history)
# =============================================================================

def fetch_weather_openweather(lat: float, lon: float, dt: datetime) -> dict:
    """
    Para dados históricos reais use o endpoint pago:
      https://history.openweathermap.org/data/2.5/history/city
    
    Para o plano gratuito, use dados atuais (demonstração):
      https://api.openweathermap.org/data/2.5/weather
    
    Esta função tenta o endpoint histórico e faz fallback para atual.
    """
    dt_unix = int(dt.timestamp())
    
    # Tenta API histórica (plano pago)
    hist_url = (
        f"https://history.openweathermap.org/data/2.5/history/city"
        f"?lat={lat}&lon={lon}&type=hour"
        f"&start={dt_unix}&end={dt_unix + 3600}"
        f"&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    
    resp = requests.get(hist_url, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("list"):
            item = data["list"][0]
            return {
                "temp_c":      item["main"]["temp"],
                "humidity":    item["main"]["humidity"],
                "rain_mm":     item.get("rain", {}).get("1h", 0.0),
                "wind_kmh":    round(item["wind"]["speed"] * 3.6, 1),
                "weather_main": item["weather"][0]["main"],
                "weather_desc": item["weather"][0]["description"],
                "source":      "openweather_history"
            }
    
    # Fallback: dados atuais (só para demo / testes)
    curr_url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    resp2 = requests.get(curr_url, timeout=10)
    if resp2.status_code == 200:
        item = resp2.json()
        return {
            "temp_c":      item["main"]["temp"],
            "humidity":    item["main"]["humidity"],
            "rain_mm":     item.get("rain", {}).get("1h", 0.0),
            "wind_kmh":    round(item["wind"]["speed"] * 3.6, 1),
            "weather_main": item["weather"][0]["main"],
            "weather_desc": item["weather"][0]["description"],
            "source":      "openweather_current"
        }
    
    return None


# =============================================================================
# 5b. OPÇÃO B — Open-Meteo API (histórico gratuito 2016-2018) ✅ RECOMENDADO
# =============================================================================

def fetch_weather_open_meteo_bulk(lat: float, lon: float,
                                   start: str, end: str) -> pd.DataFrame:
    """
    Busca dados históricos diários da Open-Meteo (100% gratuita, sem chave).
    Retorna DataFrame com colunas de clima por data.
    
    Documentação: https://open-meteo.com/en/docs/historical-weather-api
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
        f"precipitation_sum,windspeed_10m_max,weathercode"
        f"&timezone=America/Sao_Paulo"
    )
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return pd.DataFrame()
    
    data = resp.json().get("daily", {})
    if not data:
        return pd.DataFrame()
    
    df_w = pd.DataFrame(data)
    df_w.rename(columns={
        "time":                  "date",
        "temperature_2m_max":    "temp_max_c",
        "temperature_2m_min":    "temp_min_c",
        "temperature_2m_mean":   "temp_mean_c",
        "precipitation_sum":     "rain_mm",
        "windspeed_10m_max":     "wind_kmh",
        "weathercode":           "wmo_code"
    }, inplace=True)

    # Mapeamento WMO weathercode → descrição
    wmo_map = {
        0: "Céu limpo",         1: "Predominantemente limpo",
        2: "Parcialmente nublado", 3: "Nublado",
        45: "Nevoeiro",          48: "Nevoeiro com geada",
        51: "Garoa leve",        53: "Garoa moderada",
        55: "Garoa intensa",     61: "Chuva fraca",
        63: "Chuva moderada",    65: "Chuva forte",
        71: "Neve fraca",        80: "Pancadas de chuva",
        81: "Pancadas moderadas", 82: "Pancadas violentas",
        95: "Tempestade",        99: "Tempestade com granizo"
    }
    df_w["weather_desc"] = df_w["wmo_code"].map(wmo_map).fillna("Desconhecido")

    # Categoria climática simplificada
    def weather_category(row):
        if row["rain_mm"] >= 10:
            return "Chuva intensa"
        elif row["rain_mm"] >= 1:
            return "Chuva leve"
        elif row["temp_mean_c"] >= 30:
            return "Calor extremo"
        elif row["temp_mean_c"] >= 24:
            return "Quente"
        elif row["temp_mean_c"] <= 15:
            return "Frio"
        else:
            return "Agradável"

    df_w["weather_category"] = df_w.apply(weather_category, axis=1)
    df_w["date"] = pd.to_datetime(df_w["date"]).dt.date
    df_w["source"] = "open_meteo"
    return df_w


# =============================================================================
# 6. ENRIQUECER PEDIDOS COM DADOS CLIMÁTICOS (abordagem por cidade)
# =============================================================================

def enrich_with_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada cidade única, faz uma chamada à Open-Meteo cobrindo
    todo o período de compras daquela cidade, depois faz merge por
    cidade + data da compra.
    """
    print("\n[4/5] Enriquecendo pedidos com dados climáticos (Open-Meteo)...")

    weather_cache_dir = DATA_DIR / "weather_cache"
    weather_cache_dir.mkdir(exist_ok=True)

    # Prepara datas de string
    df["purchase_date_str"] = pd.to_datetime(df["purchase_date"]).dt.strftime("%Y-%m-%d")
    global_start = df["purchase_date_str"].min()
    global_end   = df["purchase_date_str"].max()

    # Cidades únicas com coordenadas
    cities_df = (df[df["lat"].notna()]
                 .drop_duplicates(subset=["customer_city"])
                 [["customer_city", "customer_state", "lat", "lon"]]
                 .reset_index(drop=True))
    
    if MAX_CITIES:
        cities_df = cities_df.head(MAX_CITIES)

    all_weather = []

    for _, row in tqdm(cities_df.iterrows(), total=len(cities_df),
                       desc="Buscando clima por cidade"):
        city   = row["customer_city"]
        lat    = row["lat"]
        lon    = row["lon"]
        
        cache_file = weather_cache_dir / f"{city.replace(' ', '_')}.parquet"
        
        if cache_file.exists():
            wdf = pd.read_parquet(cache_file)
        else:
            wdf = fetch_weather_open_meteo_bulk(lat, lon, global_start, global_end)
            if not wdf.empty:
                wdf["customer_city"] = city
                wdf.to_parquet(cache_file, index=False)
            time.sleep(0.15)  # educado com a API
        
        if not wdf.empty:
            wdf["customer_city"] = city
            all_weather.append(wdf)

    if not all_weather:
        print("[AVISO] Nenhum dado climático recuperado. Verifique conexão.")
        return df

    weather_df = pd.concat(all_weather, ignore_index=True)
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

    # Merge com pedidos
    df["purchase_date"] = pd.to_datetime(df["purchase_date"]).dt.date
    df_enriched = df.merge(
        weather_df.drop(columns=["source"], errors="ignore"),
        left_on=["customer_city", "purchase_date"],
        right_on=["customer_city", "date"],
        how="left"
    )

    enriched_pct = df_enriched["temp_mean_c"].notna().mean() * 100
    print(f"[OK] {enriched_pct:.1f}% dos pedidos enriquecidos com dados climáticos.")
    return df_enriched


# =============================================================================
# 7. FEATURE ENGINEERING — variáveis para análise no Power BI
# =============================================================================

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Cria variáveis analíticas adicionais para o Power BI."""
    print("\n[5/5] Feature engineering...")

    # --- Estação do ano (hemisfério sul) ---
    def season(month):
        if month in [12, 1, 2]:  return "Verão"
        elif month in [3, 4, 5]: return "Outono"
        elif month in [6, 7, 8]: return "Inverno"
        else:                    return "Primavera"

    df["season"] = df["purchase_month"].apply(season)

    # --- Faixa de temperatura ---
    bins_temp   = [-10, 10, 18, 24, 30, 50]
    labels_temp = ["Muito frio", "Frio", "Agradável", "Quente", "Muito quente"]
    df["temp_band"] = pd.cut(df["temp_mean_c"], bins=bins_temp,
                             labels=labels_temp, right=False)

    # --- Dia chuvoso ---
    df["is_rainy"] = df["rain_mm"].fillna(0) >= 1.0

    # --- Ticket médio ---
    df["ticket_medio"] = df["order_revenue"] / df["items_count"].clip(lower=1)

    # --- Região do Brasil ---
    region_map = {
        "SP": "Sudeste", "RJ": "Sudeste", "MG": "Sudeste", "ES": "Sudeste",
        "RS": "Sul",     "SC": "Sul",     "PR": "Sul",
        "BA": "Nordeste","CE": "Nordeste","PE": "Nordeste","MA": "Nordeste",
        "PB": "Nordeste","RN": "Nordeste","AL": "Nordeste","SE": "Nordeste","PI": "Nordeste",
        "AM": "Norte",   "PA": "Norte",   "RO": "Norte",  "RR": "Norte",
        "AC": "Norte",   "AP": "Norte",   "TO": "Norte",
        "MT": "Centro-Oeste","MS": "Centro-Oeste","GO": "Centro-Oeste","DF": "Centro-Oeste"
    }
    df["region"] = df["customer_state"].map(region_map).fillna("Desconhecido")

    # --- Período do dia da compra ---
    def period_of_day(hour):
        if 6  <= hour < 12: return "Manhã"
        elif 12 <= hour < 18: return "Tarde"
        elif 18 <= hour < 23: return "Noite"
        else:                 return "Madrugada"

    df["period_of_day"] = df["purchase_hour"].apply(period_of_day)

    # --- Flag de atraso ---
    df["is_late"] = df["delivery_delay_days"].fillna(0) > 0

    # --- Índice de calor (Heat Index simplificado) ---
    T = df["temp_mean_c"].fillna(25)
    H = df["humidity"].fillna(60) if "humidity" in df.columns else pd.Series(60, index=df.index)
    df["heat_index"] = T + 0.33 * (H / 100 * 6.105 * np.exp((17.27 * T) / (237.7 + T))) - 4.0

    print("[OK] Features criadas: season, temp_band, is_rainy, region, period_of_day...")
    return df


# =============================================================================
# 8. EXPORTAÇÃO PARA POWER BI (modelo estrela)
# =============================================================================

def export_for_powerbi(df: pd.DataFrame):
    """
    Exporta tabelas no formato modelo estrela para o Power BI.
    Usa separador ponto-e-vírgula (;) e decimal vírgula (,)
    para compatibilidade com Windows Brasil — sem erros de tipo no Power BI.

      - fact_orders        → fato (pedidos enriquecidos com clima)
      - dim_date           → dimensão tempo
      - dim_geography      → dimensão geografia
      - dim_category       → dimensão categoria de produto
      - dim_weather        → dimensão clima (para segmentações)
    """
    print("\n[6/6] Exportando para Power BI...")

    # Configuração padrão brasileiro para todos os CSVs
    CSV_OPTS = dict(index=False, encoding="utf-8-sig", sep=";", decimal=",")

    # Garante tipos numéricos corretos antes de exportar
    cols_float = [
        "order_revenue", "order_freight", "total_payment",
        "ticket_medio", "review_score", "temp_mean_c",
        "temp_max_c", "temp_min_c", "rain_mm", "wind_kmh",
        "heat_index", "lat", "lon"
    ]
    cols_int = ["delivery_delay_days", "purchase_year", "purchase_month",
                "purchase_hour", "items_count", "installments"]

    for col in cols_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in cols_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # ---- FACT: Pedidos ----
    fact_cols = [
        "order_id", "customer_id", "purchase_date", "purchase_year",
        "purchase_month", "purchase_dow", "purchase_hour",
        "customer_city", "customer_state", "region",
        "main_category", "order_revenue", "order_freight",
        "total_payment", "items_count", "ticket_medio",
        "installments", "payment_type", "review_score",
        "is_weekend", "is_rainy", "is_late", "season", "period_of_day",
        "temp_mean_c", "temp_max_c", "temp_min_c",
        "rain_mm", "wind_kmh", "weather_desc", "weather_category",
        "temp_band", "heat_index", "delivery_delay_days",
        "lat", "lon"
    ]
    existing = [c for c in fact_cols if c in df.columns]
    fact_orders = df[existing].copy()
    fact_orders.to_csv(OUTPUT_DIR / "fact_orders.csv", **CSV_OPTS)
    print(f"  fact_orders.csv      → {len(fact_orders):,} linhas")

    # ---- DIM: Data ----
    dates = pd.date_range(start="2016-01-01", end="2018-12-31", freq="D")
    dim_date = pd.DataFrame({
        "date":         dates.date,
        "year":         dates.year,
        "month":        dates.month,
        "month_name":   dates.strftime("%B"),
        "quarter":      dates.quarter,
        "week":         dates.isocalendar().week.values,
        "day_of_week":  dates.day_name(),
        "is_weekend":   dates.dayofweek >= 5,
        "season":       [season_func(m) for m in dates.month],
        "year_month":   dates.strftime("%Y-%m"),
    })
    dim_date.to_csv(OUTPUT_DIR / "dim_date.csv", **CSV_OPTS)
    print(f"  dim_date.csv         → {len(dim_date):,} linhas")

    # ---- DIM: Geografia ----
    dim_geo = (df[["customer_city", "customer_state", "region", "lat", "lon"]]
               .drop_duplicates(subset=["customer_city", "customer_state"])
               .sort_values(["customer_state", "customer_city"])
               .reset_index(drop=True))
    dim_geo.to_csv(OUTPUT_DIR / "dim_geography.csv", **CSV_OPTS)
    print(f"  dim_geography.csv    → {len(dim_geo):,} linhas")

    # ---- DIM: Categoria ----
    dim_cat = (df[["main_category"]]
               .drop_duplicates()
               .dropna()
               .sort_values("main_category")
               .reset_index(drop=True))
    dim_cat["category_group"] = dim_cat["main_category"].apply(categorize_group)
    dim_cat.to_csv(OUTPUT_DIR / "dim_category.csv", **CSV_OPTS)
    print(f"  dim_category.csv     → {len(dim_cat):,} linhas")

    # ---- DIM: Clima (lookup) ----
    dim_weather = pd.DataFrame({
        "weather_category": ["Céu limpo", "Agradável", "Chuva leve",
                             "Chuva intensa", "Quente", "Calor extremo", "Frio"],
        "is_precipitation":  [False, False, True, True, False, False, False],
        "comfort_score":     [5, 4, 3, 2, 3, 2, 3],
        "online_shopping_tendency": ["Neutra", "Neutra", "Alta", "Alta",
                                     "Neutra", "Alta", "Alta"]
    })
    dim_weather.to_csv(OUTPUT_DIR / "dim_weather.csv", **CSV_OPTS)
    print(f"  dim_weather.csv      → {len(dim_weather):,} linhas")

    # ---- Summary para validação ----
    summary = {
        "total_orders":       len(fact_orders),
        "total_revenue_brl":  round(fact_orders["order_revenue"].sum(), 2),
        "avg_ticket_brl":     round(fact_orders["ticket_medio"].mean(), 2),
        "rainy_orders_pct":   round(fact_orders["is_rainy"].mean() * 100, 1),
        "cities_with_weather": int(fact_orders["temp_mean_c"].notna().sum()),
        "date_range_start":   str(fact_orders["purchase_date"].min()),
        "date_range_end":     str(fact_orders["purchase_date"].max()),
        "exported_at":        datetime.now().isoformat()
    }
    with open(OUTPUT_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*50}")
    print(f"  Exportação concluída → {OUTPUT_DIR}/")
    print(f"  Receita total:   R$ {summary['total_revenue_brl']:,.2f}")
    print(f"  Ticket médio:    R$ {summary['avg_ticket_brl']:.2f}")
    print(f"  Pedidos na chuva: {summary['rainy_orders_pct']}%")
    print(f"{'='*50}")


# =============================================================================
# HELPERS
# =============================================================================

def season_func(month):
    if month in [12, 1, 2]:  return "Verão"
    elif month in [3, 4, 5]: return "Outono"
    elif month in [6, 7, 8]: return "Inverno"
    else:                    return "Primavera"


def categorize_group(cat):
    """Agrupa categorias em macro-grupos para análise no Power BI."""
    if not isinstance(cat, str):
        return "Outros"
    cat = cat.lower()
    if any(k in cat for k in ["electronics", "computer", "tablet", "telephony"]):
        return "Eletrônicos"
    elif any(k in cat for k in ["furniture", "home", "bed", "bath", "kitchen"]):
        return "Casa e Decoração"
    elif any(k in cat for k in ["fashion", "shoes", "watches", "sport"]):
        return "Moda e Esporte"
    elif any(k in cat for k in ["health", "beauty", "perfumery", "baby"]):
        return "Saúde e Beleza"
    elif any(k in cat for k in ["food", "drinks", "market"]):
        return "Alimentos"
    elif any(k in cat for k in ["auto", "construction", "tools"]):
        return "Auto e Ferramentas"
    elif any(k in cat for k in ["books", "music", "dvds", "art"]):
        return "Livros e Cultura"
    else:
        return "Outros"


# =============================================================================
# 9. MEDIDAS DAX PARA O POWER BI
# =============================================================================

DAX_MEASURES = """
============================================================
  MEDIDAS DAX — Cole no Power BI Desktop (Área de Medidas)
============================================================

// ── Medidas Básicas ──────────────────────────────────────

Receita Total = 
    SUMX(fact_orders, fact_orders[order_revenue])

Ticket Médio = 
    AVERAGEX(fact_orders, fact_orders[ticket_medio])

Total Pedidos = 
    COUNTROWS(fact_orders)

Avaliação Média = 
    AVERAGE(fact_orders[review_score])

Atraso Médio (dias) = 
    AVERAGE(fact_orders[delivery_delay_days])


// ── Análise Climática ────────────────────────────────────

Receita em Dias de Chuva = 
    CALCULATE(
        [Receita Total],
        fact_orders[is_rainy] = TRUE()
    )

Receita em Dias Sem Chuva = 
    CALCULATE(
        [Receita Total],
        fact_orders[is_rainy] = FALSE()
    )

Lift Receita Chuva vs Sem Chuva % = 
    DIVIDE(
        [Receita em Dias de Chuva] - [Receita em Dias Sem Chuva],
        [Receita em Dias Sem Chuva],
        0
    ) * 100

Temperatura Média dos Pedidos = 
    AVERAGE(fact_orders[temp_mean_c])

Pedidos em Calor Extremo = 
    CALCULATE(
        [Total Pedidos],
        fact_orders[temp_band] = "Muito quente"
    )


// ── Sazonalidade ─────────────────────────────────────────

Receita por Estação = 
    CALCULATE(
        [Receita Total],
        ALLEXCEPT(fact_orders, fact_orders[season])
    )

% Receita da Estação = 
    DIVIDE(
        [Receita Total],
        CALCULATE([Receita Total], ALL(fact_orders[season])),
        0
    ) * 100

Crescimento MoM % = 
    VAR ReceitaMes = [Receita Total]
    VAR ReceitaMesAnterior = 
        CALCULATE(
            [Receita Total],
            DATEADD(dim_date[date], -1, MONTH)
        )
    RETURN
        DIVIDE(ReceitaMes - ReceitaMesAnterior, ReceitaMesAnterior, 0) * 100

Crescimento YoY % = 
    VAR ReceitaAno = [Receita Total]
    VAR ReceitaAnoAnterior = 
        CALCULATE(
            [Receita Total],
            DATEADD(dim_date[date], -1, YEAR)
        )
    RETURN
        DIVIDE(ReceitaAno - ReceitaAnoAnterior, ReceitaAnoAnterior, 0) * 100


// ── Análise por Categoria ────────────────────────────────

Top Categoria = 
    TOPN(
        1,
        VALUES(fact_orders[main_category]),
        [Receita Total]
    )

Receita Eletrônicos Chuva = 
    CALCULATE(
        [Receita Total],
        fact_orders[main_category] = "electronics",
        fact_orders[is_rainy] = TRUE()
    )

Correlação Chuva–Receita por Categoria = 
    // Use como medida base em gráfico de dispersão
    // X = rain_mm médio, Y = Receita Total, filtrado por categoria
    AVERAGEX(
        VALUES(fact_orders[main_category]),
        CALCULATE(AVERAGE(fact_orders[rain_mm]))
    )


// ── Entrega e Satisfação ─────────────────────────────────

% Pedidos Atrasados = 
    DIVIDE(
        CALCULATE([Total Pedidos], fact_orders[is_late] = TRUE()),
        [Total Pedidos],
        0
    ) * 100

Atraso Médio em Dias de Chuva = 
    CALCULATE(
        AVERAGE(fact_orders[delivery_delay_days]),
        fact_orders[is_rainy] = TRUE()
    )

Score Satisfação = 
    CALCULATE(
        AVERAGE(fact_orders[review_score]),
        fact_orders[review_score] > 0
    )


// ── KPIs de Negócio ─────────────────────────────────────

Receita Acumulada (YTD) = 
    TOTALYTD([Receita Total], dim_date[date])

Meta Receita = 
    [Receita Total] * 1.15  // exemplo: 15% acima do realizado

Atingimento Meta % = 
    DIVIDE([Receita Total], [Meta Receita], 0) * 100

Receita por Pedido = 
    DIVIDE([Receita Total], [Total Pedidos], 0)
"""

# =============================================================================
# 10. GUIA DE ESTRUTURA DO REPOSITÓRIO GITHUB
# =============================================================================

README_CONTENT = """
# 🌦️ Olist Weather Analytics — Clima & E-commerce no Brasil

> **Pergunta de negócio:** Como as condições climáticas afetam as vendas
> do e-commerce brasileiro por região, estação e categoria de produto?

## 📊 Dashboard Power BI
[Adicione aqui o link do relatório publicado no Power BI Service]

## 🗂️ Estrutura do Projeto
```
olist-weather-analytics/
├── data/
│   ├── olist/              ← dataset Kaggle (não sobe ao Git)
│   ├── weather_cache/      ← cache parquet por cidade
│   └── city_coords.json    ← cache geocoding
├── output_powerbi/
│   ├── fact_orders.csv
│   ├── dim_date.csv
│   ├── dim_geography.csv
│   ├── dim_category.csv
│   └── dim_weather.csv
├── olist_weather_pipeline.py   ← ETL principal
├── requirements.txt
├── .gitignore
└── README.md
```

## 🔧 Como Executar
```bash
pip install -r requirements.txt
python olist_weather_pipeline.py
```

## 📌 Principais Insights
- [ ] Dias de chuva aumentam/diminuem vendas em X%
- [ ] Calor extremo impacta categoria Y
- [ ] Região Nordeste vs Sul: padrões opostos
- [ ] Atrasos de entrega correlacionam com chuva

## 🛠️ Stack
Python · Pandas · OpenWeather API · Open-Meteo · Power BI · DAX · GitHub
"""

# =============================================================================
# MAIN — executa o pipeline completo
# =============================================================================

if __name__ == "__main__":
    # Passo 1: Download Olist
    download_olist()

    # Passo 2: Carrega e mergeia dados Olist
    df = load_olist()

    # Passo 3: Geocoding das cidades
    df = geocode_cities(df)

    # Passo 4: Enriquecimento climático (Open-Meteo — gratuito e histórico)
    df = enrich_with_weather(df)

    # Passo 5: Feature engineering
    df = feature_engineering(df)

    # Passo 6: Exporta para Power BI
    export_for_powerbi(df)

    # Salva medidas DAX em arquivo texto
    with open(OUTPUT_DIR / "dax_measures.txt", "w", encoding="utf-8") as f:
        f.write(DAX_MEASURES)
    print("\n  dax_measures.txt     → copie as medidas no Power BI Desktop")

    # Salva README
    with open("README.md", "w", encoding="utf-8") as f:
        f.write(README_CONTENT)

    print("\n✅ Pipeline concluído! Importe os CSVs de output_powerbi/ no Power BI.")
    print("   Siga o modelo estrela:")
    print("   fact_orders → dim_date (purchase_date = date)")
    print("   fact_orders → dim_geography (customer_city)")
    print("   fact_orders → dim_category (main_category)")
    print("   fact_orders → dim_weather (weather_category)")