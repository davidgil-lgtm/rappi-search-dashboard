"""
refresh.py
──────────
Conecta a Snowflake con OKTA SSO, corre las 4 queries de keywords,
genera index.html con los datos frescos y hace push a GitHub Pages.

Uso: doble clic en run.bat  (o  py refresh.py  desde la terminal)
"""

import json
import subprocess
import sys
from datetime import datetime

import snowflake.connector
import pandas as pd

from config import SNOWFLAKE_CONFIG

# ================================================================
# KEYWORDS POR SUBVERTICAL (extraídas de DM_LOCAL_SEARCH)
# ================================================================
KWS = {
    "super": [
        "agua","leche","queso","huevos","cerveza","pan","helado","tomate","pollo","chocolate",
        "papas","jamon","limon","cocacola zero","papa","arroz","banano","aguacate","yogurt griego",
        "papel higienico","carne","cafe","cocacola","mantequilla","crema de leche","cebolla",
        "coca cola","gaseosa","salchicha","arepa","soda","hielo","huevo","vino","galletas",
        "aceite","cilantro","atun","electrolit","yogurt","carne molida","queso parmesano",
        "zanahoria","pan tajado","platano","queso crema","hatsu","tocineta","pechuga","gomitas",
        "arandanos","leche deslactosada","lechuga","azucar","brownie","fresa","ajo",
        "jugo de naranja","pasta","mani","shampoo","aguardiente","jugo","sal","doritos",
        "gatorade","papitas","pañitos humedos","jabon","manzana","chorizo","arepas","cereal",
        "pechuga de pollo","avena","queso mozzarella","pimenton","mango","fruta",
        "jabon para ropa","suero","milo","granola","desodorante","quesito","pepino",
        "bolsa para basura","salsa de tomate","vino blanco","chocolatina","aceite de oliva",
        "crema dental","tortillas","coca cola zero","fresas","espinaca","mayonesa",
        "queso tajado","galleta","de todito"
    ],
    "farmacia": [
        "acetaminofen","electrolit","vitamina c","suero","jeringa","dolex","shampoo",
        "desodorante","pedialyte","ibuprofeno","cepillo de dientes","prueba de embarazo",
        "crema dental","gripa","enterogermina","gomitas","pañitos humedos","jeringa de insulina",
        "mounjaro","loratadina","tampones","toalla higienica nosotras","esmalte","condones",
        "dolex gripa","pax noche","hidraplus","noraver garganta","naproxeno","mieltertos",
        "cetirizina","jabon","proteina","gasa","amoxicilina","solucion salina","pañales",
        "noxpirin","advil","alcohol","suero fisiologico","lubricante","loperamida","pax",
        "dove","noraver","postday","azitromicina 500 mg","clotrimazol","acondicionador",
        "cerave","centrum","omeprazol","micropore","nivea","sildenafil","pestañina",
        "desloratadina","strepsil","gaviscon","protectores","ensure","chocolatina","tapabocas",
        "colgate","vick vaporub","bloqueador","isdin","toallas","buscapina","diclofenaco",
        "tinte","aciclovir","maquillaje","guantes","vitamin d3 k2","protector solar",
        "pax dia","seda dental","listerine","buscapina fem","dolex forte","algodon",
        "fluconazol","engystol","tramadol","sevedol","aguja","cuchilla de afeitar","smecta",
        "vaselina","ovulos","ponds","similac","pañitos","monster","toalla higienica",
        "papel higienico","leche","chocolate","helado","agua","papas","gatorade","cerveza"
    ],
    "mascotas": [
        "churu","agility gold","royal canin","arena","hills","pixie","nexgard","br for cat",
        "chunky","taste of the wild","fortiflora","proplan","arena de gato","bravecto",
        "collar isabelino","aciflux","monello","max cat","fancy feast","agility",
        "royal canin gato","galletas","taste of wild","bolsas","hueso","nexgard spectra",
        "tapete","agility gold gato","arena calabaza","barf","wow can","equilibrio",
        "comida humeda","shampoo","royal canin gastrointestinal","meloxicam","mungos",
        "hills gato","pañitos","ronik","evolve","pro plan","baxidin","gastrointestinal",
        "gato","arena para gatos","comida perro","equilibrio gatos","dog chow",
        "simparica trio","arena tofu","desparasitante","arena gato","excellent","pañales",
        "diamond naturals","hills id","juguete","cepillo","arena de maiz","felix",
        "hemolitan","nicilan","bonnat","laika","gatos","tiki cat","fresh step",
        "alimento humedo","cama","alernex","collar","arnes","antipulgas","snack",
        "arenero","fitovete","bismopet","previcox","royal","arena gatos","fancy",
        "engystol","suero","gasa","clorhexidina","comida humeda gato","epiotic",
        "cat chow","royal canin puppy","arena maiz","gosbi","canisan d","max",
        "br cat","br","comida gato","agility gold gatos","agility gold piel",
        "mascotas","veterinaria"
    ],
    "especializada": [
        "buldak","salmon","camaron","ramen","carne","yogurt griego","queso","pan",
        "pollo","leche","proteina","la fazenda","tomate","chocolate","huevos","atun",
        "tocineta","chorizo","carne molida","jamon","langostino","aguacate","queso cottage",
        "granola","cebolla","leche de coco","camarones","pechuga","arroz","cilantro",
        "pan de arroz","galletas","papa","limon","cottage","tilapia","gyoza","soya",
        "tofu","edamame","yogurt","banano","arequipe","arepa","coco","ajo","costilla",
        "chicharron","chamoy","lomo","hamburguesa","cafe","crema de leche","pulpo",
        "salchicha","salsa","jengibre","mantequilla","mayonesa","pescado","aceite",
        "pasta","salmon ahumado","cazuela","cerveza","bondiola","papa criolla","creatina",
        "kefir","costilla de cerdo","cazuela mariscos","platano","queso crema","ajonjoli",
        "pimenton","zanahoria","mango","lechuga","cereal","stevia","huevo","miel",
        "calamar","agua","colombina","gomitas","helado","kimchi","arroz de sushi",
        "lomo de res","torta","evok","pastaio","la nacional","nesspreso","aceite de oliva",
        "fresa","pepino","espinaca"
    ]
}

WEEKS_TO_FETCH = 9  # últimas N semanas

# ================================================================
# CONEXIÓN A SNOWFLAKE
# ================================================================
def connect():
    print("🔐 Abriendo navegador para login con OKTA...")
    print("   Por favor inicia sesión en la ventana que se abre.")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("✅ Conexión exitosa\n")
    return conn

# ================================================================
# DETECTAR SEMANAS DISPONIBLES
# ================================================================
def get_available_weeks(conn):
    print("📅 Detectando semanas disponibles...")
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT WEEK
        FROM FIVETRAN.CPGS_DATASCIENCE.DM_LOCAL_SEARCH
        WHERE COUNTRY_CODE = 'CO'
          AND WEEK IS NOT NULL
          AND WEEK >= DATEADD(week, -{WEEKS_TO_FETCH + 1}, CURRENT_DATE)
        ORDER BY WEEK DESC
        LIMIT {WEEKS_TO_FETCH}
    """)
    weeks = [str(row[0]) for row in cur.fetchall()]
    print(f"   Semanas encontradas: {weeks}\n")
    return weeks

# ================================================================
# QUERY POR SUBVERTICAL
# ================================================================
def build_union(vertical, weeks):
    kws = KWS[vertical]
    kw_list = ", ".join([f"'{k.replace(chr(39), chr(39)*2)}'" for k in kws])

    blocks = []
    for w in weeks:
        blocks.append(f"""
    SELECT
        '{w}'::DATE                   AS WEEK,
        S.KEYWORD_SEARCHED::VARCHAR   AS KEYWORD_SEARCHED,
        S.VERTICAL_SUB_GROUP::VARCHAR AS VERTICAL_SUB_GROUP,
        ST.BRAND_GROUP::VARCHAR       AS BRAND_GROUP,
        S.HAS_ATC::NUMBER             AS HAS_ATC
    FROM FIVETRAN.CPGS_DATASCIENCE.DM_LOCAL_SEARCH S
    INNER JOIN RP_SILVER_DB_PROD.CPGS_LOCAL_ANALYTICS.TBL_DIM_STORES ST
        ON S.STORE_ID = ST.STORE_ID AND S.COUNTRY_CODE = ST.COUNTRY
    WHERE S.COUNTRY_CODE = 'CO'
      AND S.WEEK = '{w}'
      AND S.SOURCE_TYPE_EVENT = 'LOCAL_SEARCH'
      AND S.VERTICAL_SUB_GROUP IN ('{vertical}', 'turbo')
      AND LOWER(TRIM(S.KEYWORD_SEARCHED)) IN ({kw_list})""")

    union_sql = "\n    UNION ALL".join(blocks)

    return f"""
WITH BASE AS ({union_sql}
),
AGG AS (
    SELECT
        WEEK,
        LOWER(TRIM(KEYWORD_SEARCHED))   AS KEYWORD,
        VERTICAL_SUB_GROUP,
        BRAND_GROUP,
        COUNT(*)                        AS TOTAL_SEARCHES,
        SUM(HAS_ATC)                    AS ATC_SEARCHES,
        ROUND(SUM(HAS_ATC) * 100.0 / NULLIF(COUNT(*), 0), 2) AS PCT_ATC
    FROM BASE
    GROUP BY WEEK, LOWER(TRIM(KEYWORD_SEARCHED)), VERTICAL_SUB_GROUP, BRAND_GROUP
),
KW_RANK AS (
    SELECT WEEK, KEYWORD,
        SUM(TOTAL_SEARCHES) AS KW_TOTAL,
        RANK() OVER (PARTITION BY WEEK ORDER BY SUM(TOTAL_SEARCHES) DESC) AS KW_ORDER
    FROM AGG GROUP BY WEEK, KEYWORD
)
SELECT
    A.WEEK, K.KW_ORDER, A.KEYWORD, K.KW_TOTAL,
    A.VERTICAL_SUB_GROUP, A.BRAND_GROUP,
    A.TOTAL_SEARCHES, A.ATC_SEARCHES, A.PCT_ATC
FROM AGG A
INNER JOIN KW_RANK K ON A.WEEK = K.WEEK AND A.KEYWORD = K.KEYWORD
ORDER BY A.WEEK DESC, K.KW_ORDER ASC, A.TOTAL_SEARCHES DESC
"""

def run_query(conn, vertical, weeks):
    print(f"📊 Consultando {vertical.upper()}...")
    cur = conn.cursor()
    cur.execute(build_union(vertical, weeks))
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df["week"] = df["week"].astype(str)
    print(f"   {len(df)} filas obtenidas")
    return df

# ================================================================
# GENERAR HTML
# ================================================================
def generate_html(data: dict, weeks: list, updated_at: str) -> str:
    # Serialize data to JSON for JS
    js_data = {}
    for v, df in data.items():
        js_data[v] = df.to_dict(orient="records")

    js_weeks = json.dumps(weeks)
    js_payload = json.dumps(js_data)

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Keyword ATC Analytics · Rappi CO</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500&family=Cabinet+Grotesk:wght@400;500;700;800&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
:root{{
  --bg:#080a0f;--bg2:#0f1118;--bg3:#161b26;
  --b1:rgba(255,255,255,.06);--b2:rgba(255,255,255,.1);
  --tx:#e8ecf5;--tx2:#8b95a8;--tx3:#4e5668;
  --super:#ff5733;--turbo:#00d4ff;--farma:#2ecc7a;--pets:#f0b429;--esp:#b57bee;
  --green:#34d399;--red:#f87171;--r:14px;
  --mono:'IBM Plex Mono',monospace;--display:'Cabinet Grotesk',sans-serif;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--tx);font-family:var(--display);overflow-x:hidden;font-size:13px}}
.app{{display:flex;height:100vh;overflow:hidden}}

/* SIDEBAR */
.sb{{width:200px;background:var(--bg2);border-right:1px solid var(--b1);display:flex;flex-direction:column;flex-shrink:0}}
.sb-logo{{padding:20px 18px 16px;border-bottom:1px solid var(--b1)}}
.sb-logo h1{{font-size:15px;font-weight:800;display:flex;align-items:center;gap:7px}}
.sb-logo h1 span{{width:7px;height:7px;border-radius:50%;background:var(--super);display:inline-block}}
.sb-logo p{{font-family:var(--mono);font-size:9px;color:var(--tx3);letter-spacing:1.5px;text-transform:uppercase;margin-top:3px}}
.sb-updated{{font-family:var(--mono);font-size:8px;color:var(--tx3);padding:8px 18px;border-bottom:1px solid var(--b1)}}
.sb-updated span{{color:var(--green)}}
.sb-lbl{{font-family:var(--mono);font-size:8.5px;letter-spacing:2px;text-transform:uppercase;color:var(--tx3);padding:14px 14px 6px}}
.sb-item{{display:flex;align-items:center;gap:8px;padding:7px 14px;cursor:pointer;transition:.15s;font-size:12px;color:var(--tx2);border-left:2px solid transparent}}
.sb-item:hover{{color:var(--tx);background:rgba(255,255,255,.04)}}
.sb-item.on{{color:var(--tx);background:rgba(255,255,255,.06);border-left-color:var(--super)}}
.sb-sep{{height:1px;background:var(--b1);margin:6px 14px}}
.v-pills{{padding:12px 14px;margin-top:auto;border-top:1px solid var(--b1)}}
.v-lbl{{font-family:var(--mono);font-size:8.5px;letter-spacing:2px;text-transform:uppercase;color:var(--tx3);margin-bottom:8px}}
.vpill{{display:flex;align-items:center;gap:7px;padding:6px 10px;border-radius:8px;cursor:pointer;font-size:11.5px;color:var(--tx2);transition:.15s;margin-bottom:2px}}
.vpill::before{{content:'';width:5px;height:5px;border-radius:50%;flex-shrink:0}}
.vpill[data-v=super]::before{{background:var(--super)}}
.vpill[data-v=farmacia]::before{{background:var(--farma)}}
.vpill[data-v=mascotas]::before{{background:var(--pets)}}
.vpill[data-v=especializada]::before{{background:var(--esp)}}
.vpill:hover{{background:rgba(255,255,255,.05);color:var(--tx)}}
.vpill.on[data-v=super]{{background:rgba(255,87,51,.12);color:var(--super)}}
.vpill.on[data-v=farmacia]{{background:rgba(46,204,122,.12);color:var(--farma)}}
.vpill.on[data-v=mascotas]{{background:rgba(240,180,41,.12);color:var(--pets)}}
.vpill.on[data-v=especializada]{{background:rgba(181,123,238,.12);color:var(--esp)}}

/* MAIN */
.main{{flex:1;display:flex;flex-direction:column;overflow:hidden}}
.topbar{{padding:14px 24px;border-bottom:1px solid var(--b1);background:var(--bg2);display:flex;align-items:center;gap:12px;flex-shrink:0}}
.topbar-title{{font-size:17px;font-weight:800;flex:1;display:flex;align-items:center;gap:10px}}
.vbadge{{font-family:var(--mono);font-size:9px;letter-spacing:1.5px;padding:3px 9px;border-radius:20px;text-transform:uppercase}}
.ctrl{{background:var(--bg3);border:1px solid var(--b2);color:var(--tx);border-radius:8px;padding:6px 11px;font-size:11px;font-family:var(--mono);cursor:pointer;outline:none}}
.turbo-btn{{display:flex;align-items:center;gap:7px;background:var(--bg3);border:1px solid var(--b2);border-radius:8px;padding:6px 12px;cursor:pointer;font-size:11px;font-family:var(--mono);color:var(--tx2);transition:.15s}}
.turbo-btn::before{{content:'';width:5px;height:5px;border-radius:50%;background:var(--turbo);flex-shrink:0}}
.turbo-btn.on{{border-color:var(--turbo);color:var(--turbo);background:rgba(0,212,255,.06)}}
.content{{flex:1;overflow-y:auto;padding:20px 24px}}
::-webkit-scrollbar{{width:4px}}.scrollbar-thumb{{background:var(--b2)}}

/* CARDS */
.card{{background:var(--bg2);border:1px solid var(--b1);border-radius:var(--r);padding:18px 20px}}
.card-hd{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px}}
.card-hd h2{{font-size:13px;font-weight:700}}
.card-hd p{{font-size:10.5px;color:var(--tx2);margin-top:2px}}
.ch200{{position:relative;height:200px}}.ch260{{position:relative;height:260px}}
.ch300{{position:relative;height:300px}}.ch360{{position:relative;height:360px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.g1{{margin-bottom:14px}}
.kpis{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px}}
.kpi{{background:var(--bg2);border:1px solid var(--b1);border-radius:var(--r);padding:14px 16px;position:relative;overflow:hidden}}
.kpi::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px}}
.kpi.c1::before{{background:var(--super)}}.kpi.c2::before{{background:var(--turbo)}}
.kpi.c3::before{{background:var(--green)}}.kpi.c4::before{{background:var(--pets)}}
.kpi.c5::before{{background:var(--esp)}}
.kpi-lbl{{font-family:var(--mono);font-size:8.5px;letter-spacing:1.5px;text-transform:uppercase;color:var(--tx3);margin-bottom:8px}}
.kpi-val{{font-size:24px;font-weight:800;line-height:1;margin-bottom:4px}}
.kpi-sub{{font-size:10px;color:var(--tx2)}}

/* TABLE */
.tbl{{width:100%;border-collapse:collapse}}
.tbl th{{font-family:var(--mono);font-size:9px;letter-spacing:1.2px;text-transform:uppercase;color:var(--tx3);padding:7px 10px;text-align:left;border-bottom:1px solid var(--b1)}}
.tbl td{{padding:7px 10px;font-size:11.5px;border-bottom:1px solid rgba(255,255,255,.03)}}
.tbl tbody tr:hover td{{background:rgba(255,255,255,.02)}}
.mono{{font-family:var(--mono);font-size:11px}}
.bar-wrap{{display:flex;align-items:center;gap:7px}}
.bar-bg{{flex:1;height:3px;border-radius:2px;background:var(--b1)}}
.bar-fill{{height:100%;border-radius:2px}}
.pct-num{{font-family:var(--mono);font-size:10.5px;min-width:38px;text-align:right}}
.up{{color:var(--green)}}.dn{{color:var(--red)}}
.badge-t{{font-family:var(--mono);font-size:8.5px;padding:2px 7px;border-radius:20px;text-transform:uppercase}}
.bt{{background:rgba(0,212,255,.15);color:var(--turbo)}}
.bv{{background:rgba(255,87,51,.12);color:var(--super)}}

/* SEARCH */
.srch{{position:relative;margin-bottom:14px}}
.srch input{{width:100%;background:var(--bg3);border:1px solid var(--b2);border-radius:9px;padding:8px 14px 8px 34px;font-size:12px;font-family:var(--mono);color:var(--tx);outline:none}}
.srch input:focus{{border-color:var(--super)}}
.srch svg{{position:absolute;left:11px;top:50%;transform:translateY(-50%);opacity:.4;width:14px;height:14px}}
.tabs{{display:flex;gap:3px;background:var(--bg3);border-radius:9px;padding:3px;width:fit-content;margin-bottom:14px}}
.tab{{padding:5px 14px;border-radius:6px;font-size:11px;font-family:var(--mono);cursor:pointer;color:var(--tx2);transition:.15s}}
.tab.on{{background:var(--bg2);color:var(--tx)}}
.canib-grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}}
.ck{{background:var(--bg3);border:1px solid var(--b1);border-radius:10px;padding:10px 12px}}
.ck-name{{font-size:11.5px;font-weight:700;margin-bottom:8px}}
.ck-row{{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}}
.ck-lbl{{font-family:var(--mono);font-size:9px;color:var(--tx2)}}
.ck-val{{font-family:var(--mono);font-size:11px;font-weight:500}}
@keyframes fu{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:translateY(0)}}}}
.fu{{animation:fu .25s ease forwards}}
</style>
</head>
<body>
<div class="app">
<aside class="sb">
  <div class="sb-logo">
    <h1><span></span>Keyword ATC</h1>
    <p>Rappi · Colombia</p>
  </div>
  <div class="sb-updated">Actualizado: <span>{updated_at}</span></div>
  <div class="sb-lbl">Vistas</div>
  <div class="sb-item on" onclick="setView('overview',this)">📊 Overview</div>
  <div class="sb-item" onclick="setView('keywords',this)">🔍 Keywords & ATC</div>
  <div class="sb-item" onclick="setView('brands',this)">🏪 Brand Groups</div>
  <div class="sb-item" onclick="setView('cannib',this)">⚡ Canibalización</div>
  <div class="sb-item" onclick="setView('trends',this)">📈 Tendencia</div>
  <div class="v-pills">
    <div class="v-lbl">Subvertical</div>
    <div class="vpill on" data-v="super" onclick="setV('super',this)">Super</div>
    <div class="vpill" data-v="farmacia" onclick="setV('farmacia',this)">Farmacia</div>
    <div class="vpill" data-v="mascotas" onclick="setV('mascotas',this)">Mascotas</div>
    <div class="vpill" data-v="especializada" onclick="setV('especializada',this)">Especializada</div>
  </div>
</aside>
<main class="main">
  <div class="topbar">
    <div class="topbar-title"><span id="viewLbl">Overview</span><span class="vbadge" id="vbadge">SUPER</span></div>
    <select class="ctrl" id="weekSel" onchange="render()">
      <option value="all">Todas las semanas</option>
    </select>
    <div class="turbo-btn on" id="turboBtn" onclick="toggleTurbo()">TURBO incluido</div>
  </div>
  <div class="content" id="content"></div>
</main>
</div>

<script>
const RAW_DATA = {js_payload};
const ALL_WEEKS = {js_weeks};

// ── state ──
let cV='super', cView='overview', showTurbo=true;
let charts={{}};
const VC={{super:'#ff5733',farmacia:'#2ecc7a',mascotas:'#f0b429',especializada:'#b57bee',turbo:'#00d4ff'}};

// ── init week selector ──
const wsel=document.getElementById('weekSel');
ALL_WEEKS.forEach(w=>{{const o=document.createElement('option');o.value=w;o.textContent=w;wsel.appendChild(o)}});

// ── utils ──
const fmt=n=>n>=1000?(n/1000).toFixed(1)+'K':Number(n).toLocaleString();
const pct=(a,b)=>b===0?0:+(a/b*100).toFixed(1);
const hex=(h,a)=>{{const r=parseInt(h.slice(1,3),16),g=parseInt(h.slice(3,5),16),b=parseInt(h.slice(5,7),16);return`rgba(${{r}},${{g}},${{b}},${{a}})` }};
const vc=v=>VC[v]||'#fff';
const kill=()=>Object.keys(charts).forEach(k=>{{if(charts[k]){{charts[k].destroy();delete charts[k]}}}});

function getRows(){{
  const w=document.getElementById('weekSel').value;
  let rows=RAW_DATA[cV]||[];
  if(w!=='all') rows=rows.filter(r=>r.week===w);
  if(!showTurbo) rows=rows.filter(r=>r.vertical_sub_group!=='turbo');
  return rows;
}}

function kwStats(){{
  const rows=getRows(); const map={{}};
  rows.forEach(r=>{{
    const k=r.keyword;
    if(!map[k]) map[k]={{kw:k,searches:0,atc:0}};
    map[k].searches+=+r.total_searches; map[k].atc+=+r.atc_searches;
  }});
  return Object.values(map).map(r=>{{r.pct=pct(r.atc,r.searches);return r}}).sort((a,b)=>b.searches-a.searches);
}}

function brandStats(){{
  const rows=getRows(); const map={{}};
  rows.forEach(r=>{{
    const k=r.brand_group;
    if(!map[k]) map[k]={{brand:k,searches:0,atc:0,isTurbo:r.vertical_sub_group==='turbo'}};
    map[k].searches+=+r.total_searches; map[k].atc+=+r.atc_searches;
  }});
  return Object.values(map).map(r=>{{r.pct=pct(r.atc,r.searches);return r}}).sort((a,b)=>b.searches-a.searches);
}}

function weeklyBrand(brand){{
  return ALL_WEEKS.map(w=>{{
    const rows=(RAW_DATA[cV]||[]).filter(r=>r.week===w&&r.brand_group===brand);
    const s=rows.reduce((a,r)=>a+ +r.total_searches,0);
    const a=rows.reduce((a,r)=>a+ +r.atc_searches,0);
    return{{searches:s,atc:a,pct:pct(a,s)}};
  }}).reverse();
}}

function weeklyAll(){{
  return ALL_WEEKS.map(w=>{{
    const rows=(RAW_DATA[cV]||[]).filter(r=>r.week===w&&r.vertical_sub_group!=='turbo');
    const s=rows.reduce((a,r)=>a+ +r.total_searches,0);
    const a=rows.reduce((a,r)=>a+ +r.atc_searches,0);
    return{{searches:s,atc:a,pct:pct(a,s)}};
  }}).reverse();
}}

function weeklyTurbo(){{
  return ALL_WEEKS.map(w=>{{
    const rows=(RAW_DATA[cV]||[]).filter(r=>r.week===w&&r.vertical_sub_group==='turbo');
    const s=rows.reduce((a,r)=>a+ +r.total_searches,0);
    const a=rows.reduce((a,r)=>a+ +r.atc_searches,0);
    return{{searches:s,atc:a,pct:pct(a,s)}};
  }}).reverse();
}}

const wlbl=ALL_WEEKS.slice().reverse().map(w=>w.slice(5));

function copts(extra={{}}){{
  return{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{backgroundColor:'#1a1e2a',titleColor:'#e8ecf5',bodyColor:'#8b95a8',padding:10,cornerRadius:8}}}},
    scales:{{x:{{grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'#4e5668',font:{{family:'IBM Plex Mono',size:9}}}}}},
             y:{{grid:{{color:'rgba(255,255,255,.04)'}},ticks:{{color:'#4e5668',font:{{family:'IBM Plex Mono',size:9}}}}}}}},
    ...extra}};
}}

// ── setters ──
function setV(v,el){{
  cV=v;
  document.querySelectorAll('.vpill').forEach(p=>p.classList.remove('on'));
  el.classList.add('on');
  const c=vc(v),badge=document.getElementById('vbadge');
  badge.textContent=v.toUpperCase();badge.style.background=hex(c,.15);badge.style.color=c;
  render();
}}
function setView(v,el){{
  cView=v;
  document.querySelectorAll('.sb-item').forEach(i=>i.classList.remove('on'));
  el.classList.add('on');
  const lbls={{overview:'Overview',keywords:'Keywords & ATC',brands:'Brand Groups',cannib:'Canibalización',trends:'Tendencia'}};
  document.getElementById('viewLbl').textContent=lbls[v];
  render();
}}
function toggleTurbo(){{
  showTurbo=!showTurbo;
  const el=document.getElementById('turboBtn');
  el.classList.toggle('on',showTurbo);
  el.textContent=showTurbo?'TURBO incluido':'TURBO excluido';
  render();
}}
function render(){{kill();if(cView==='overview')rOverview();else if(cView==='keywords')rKeywords();else if(cView==='brands')rBrands();else if(cView==='cannib')rCannib();else rTrends();}}

// ── OVERVIEW ──
function rOverview(){{
  const c=vc(cV),kt=kwStats(),bt=brandStats();
  const totS=bt.reduce((s,r)=>s+r.searches,0),totA=bt.reduce((s,r)=>s+r.atc,0);
  const avgP=pct(totA,totS);
  const turbo=bt.find(b=>b.isTurbo);
  const nonT=bt.filter(b=>!b.isTurbo);
  const avgNT=pct(nonT.reduce((s,r)=>s+r.atc,0),nonT.reduce((s,r)=>s+r.searches,0));
  const tdiff=turbo?(turbo.pct-avgNT).toFixed(1):null;

  document.getElementById('content').innerHTML=`
  <div class="kpis fu">
    <div class="kpi c1"><div class="kpi-lbl">Total Búsquedas</div><div class="kpi-val">${{fmt(totS)}}</div><div class="kpi-sub">local_search · CO</div></div>
    <div class="kpi c2"><div class="kpi-lbl">ATC Total</div><div class="kpi-val">${{fmt(totA)}}</div><div class="kpi-sub">add to cart</div></div>
    <div class="kpi c3"><div class="kpi-lbl">% ATC Promedio</div><div class="kpi-val">${{avgP}}%</div><div class="kpi-sub">todas las brands</div></div>
    <div class="kpi c4"><div class="kpi-lbl">TURBO % ATC</div><div class="kpi-val" style="color:var(--turbo)">${{turbo?turbo.pct+'%':'—'}}</div>
      <div class="kpi-sub ${{tdiff>0?'dn':'up'}}">${{tdiff?((tdiff>0?'↑ +':'↓ ')+tdiff+'pp vs resto'):'—'}}</div></div>
    <div class="kpi c5"><div class="kpi-lbl">Top Keyword</div><div class="kpi-val" style="font-size:16px;line-height:1.2">${{kt[0]?.kw||'—'}}</div>
      <div class="kpi-sub">${{fmt(kt[0]?.searches||0)}} búsquedas</div></div>
  </div>
  <div class="g2 fu">
    <div class="card"><div class="card-hd"><div><h2>% ATC por Brand Group</h2><p>Incluye Turbo para comparar</p></div></div><div class="ch200"><canvas id="cBA"></canvas></div></div>
    <div class="card"><div class="card-hd"><div><h2>Top 10 Keywords — Volumen</h2><p>Búsquedas totales</p></div></div><div class="ch200"><canvas id="cKV"></canvas></div></div>
  </div>
  <div class="g2 fu">
    <div class="card"><div class="card-hd"><div><h2>Top 15 Keywords — % ATC</h2><p>Conversión por keyword</p></div></div><div class="ch260"><canvas id="cKA"></canvas></div></div>
    <div class="card"><div class="card-hd"><div><h2>Vertical vs TURBO — Búsquedas semanales</h2><p>Volumen comparado</p></div></div><div class="ch260"><canvas id="cVT"></canvas></div></div>
  </div>`;

  const maxA=Math.max(...bt.map(r=>r.pct));
  charts.cBA=new Chart(document.getElementById('cBA'),{{type:'bar',data:{{labels:bt.map(r=>r.brand?.split(' ')[0]||r.brand),datasets:[{{data:bt.map(r=>r.pct),backgroundColor:bt.map(r=>r.isTurbo?hex(VC.turbo,.8):hex(c,r.pct/maxA*.7+.2)),borderRadius:5,borderSkipped:false}}]}},options:{{...copts(),plugins:{{...copts().plugins,tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.parsed.y}}% ATC`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>v+'%'}}}}}}}}}});

  const top10=kt.slice(0,10);
  charts.cKV=new Chart(document.getElementById('cKV'),{{type:'bar',data:{{labels:top10.map(r=>r.kw),datasets:[{{data:top10.map(r=>r.searches),backgroundColor:hex(c,.6),borderRadius:5,borderSkipped:false}}]}},options:{{...copts(),indexAxis:'y',plugins:{{...copts().plugins,tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{fmt(ctx.parsed.x)}} búsquedas`}}}}}}}}}});

  const top15=kt.slice(0,15);
  charts.cKA=new Chart(document.getElementById('cKA'),{{type:'bar',data:{{labels:top15.map(r=>r.kw),datasets:[{{data:top15.map(r=>r.pct),backgroundColor:top15.map((_,i)=>hex(c,.9-i*.04)),borderRadius:5,borderSkipped:false}}]}},options:{{...copts(),indexAxis:'y',plugins:{{...copts().plugins,tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.parsed.x}}% ATC`}}}}}},scales:{{...copts().scales,x:{{...copts().scales.x,ticks:{{...copts().scales.x.ticks,callback:v=>v+'%'}}}}}}}}}});

  const vw=weeklyAll(),tw=weeklyTurbo();
  charts.cVT=new Chart(document.getElementById('cVT'),{{type:'line',data:{{labels:wlbl,datasets:[
    {{label:cV.toUpperCase(),data:vw.map(r=>r.searches),borderColor:c,backgroundColor:hex(c,.08),fill:true,tension:0.3,pointRadius:3,borderWidth:2}},
    {{label:'TURBO',data:tw.map(r=>r.searches),borderColor:VC.turbo,backgroundColor:hex(VC.turbo,.06),fill:true,tension:0.3,pointRadius:3,borderWidth:2,borderDash:[4,3]}}
  ]}},options:{{...copts(),plugins:{{legend:{{display:true,labels:{{color:'#8b95a8',font:{{family:'IBM Plex Mono',size:9}},boxWidth:8}}}},tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.dataset.label}}: ${{fmt(ctx.parsed.y)}}`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>fmt(v)}}}}}}}}}});
}}

// ── KEYWORDS ──
function rKeywords(){{
  const c=vc(cV),kt=kwStats();
  document.getElementById('content').innerHTML=`
  <div class="srch fu"><svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
    <input type="text" id="kwSrch" placeholder="Buscar keyword..." oninput="fKw()" style="color:var(--tx)"></div>
  <div class="tabs fu"><div class="tab on" onclick="sTab('vol',this)">Volumen</div><div class="tab" onclick="sTab('pct',this)">% ATC</div></div>
  <div class="card fu"><div class="card-hd"><div><h2>Top Keywords — Ranking completo</h2><p>${{cV.toUpperCase()}} + TURBO · búsquedas + ATC</p></div></div>
  <table class="tbl"><thead><tr><th>#</th><th>Keyword</th><th>Búsquedas</th><th>ATC</th><th>% ATC</th></tr></thead><tbody id="kwBody"></tbody></table></div>`;
  window._kd=kt; rKwBody(kt);
}}
function rKwBody(rows){{
  const c=vc(cV),mS=Math.max(...rows.map(r=>r.searches)),mP=Math.max(...rows.map(r=>r.pct));
  document.getElementById('kwBody').innerHTML=rows.map((r,i)=>`<tr>
    <td class="mono" style="color:var(--tx3)">${{i+1}}</td>
    <td><strong>${{r.kw}}</strong></td>
    <td><div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:${{r.searches/mS*100}}%;background:${{hex(c,.5)}}"></div></div><span class="pct-num mono">${{fmt(r.searches)}}</span></div></td>
    <td class="mono">${{fmt(r.atc)}}</td>
    <td><div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:${{r.pct/mP*100}}%;background:${{c}}"></div></div><span class="pct-num" style="color:${{c}}">${{r.pct}}%</span></div></td>
  </tr>`).join('');
}}
function fKw(){{const q=document.getElementById('kwSrch').value.toLowerCase();rKwBody(window._kd.filter(r=>r.kw.includes(q)));}}
function sTab(s,el){{document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));el.classList.add('on');rKwBody([...window._kd].sort((a,b)=>s==='vol'?b.searches-a.searches:b.pct-a.pct));}}

// ── BRANDS ──
function rBrands(){{
  const c=vc(cV),bt=brandStats();
  const avg=pct(bt.reduce((s,r)=>s+r.atc,0),bt.reduce((s,r)=>s+r.searches,0));
  document.getElementById('content').innerHTML=`
  <div class="g2 fu">
    <div class="card"><div class="card-hd"><div><h2>Búsquedas por Brand</h2></div></div><div class="ch260"><canvas id="cBV"></canvas></div></div>
    <div class="card"><div class="card-hd"><div><h2>% ATC por Brand</h2></div></div><div class="ch260"><canvas id="cBP"></canvas></div></div>
  </div>
  <div class="card fu"><div class="card-hd"><div><h2>Detalle completo</h2></div></div>
  <table class="tbl"><thead><tr><th>#</th><th>Brand Group</th><th>Búsquedas</th><th>ATC</th><th>% ATC</th><th>vs. promedio</th></tr></thead>
  <tbody>${{bt.map((r,i)=>{{const d=(r.pct-avg).toFixed(1),cl=d>0?'up':'dn',ar=d>0?'↑':'↓',bc=r.isTurbo?VC.turbo:c;
    return`<tr><td class="mono" style="color:var(--tx3)">${{i+1}}</td>
    <td><span style="font-family:var(--mono);font-size:8.5px;padding:2px 7px;border-radius:20px;background:${{hex(bc,.15)}};color:${{bc}}">${{r.brand}}</span></td>
    <td class="mono">${{fmt(r.searches)}}</td><td class="mono">${{fmt(r.atc)}}</td>
    <td><div class="bar-wrap"><div class="bar-bg"><div class="bar-fill" style="width:${{r.pct}}%;background:${{bc}}"></div></div><span class="pct-num" style="color:${{bc}}">${{r.pct}}%</span></div></td>
    <td class="mono ${{cl}}">${{ar}} ${{Math.abs(d)}}pp</td></tr>`}}).join('')}}
  </tbody></table></div>`;

  charts.cBV=new Chart(document.getElementById('cBV'),{{type:'bar',data:{{labels:bt.map(r=>r.brand?.split(' ')[0]||r.brand),datasets:[{{data:bt.map(r=>r.searches),backgroundColor:bt.map(r=>hex(r.isTurbo?VC.turbo:c,.65)),borderRadius:5,borderSkipped:false}}]}},options:{{...copts(),plugins:{{...copts().plugins,tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{fmt(ctx.parsed.y)}} búsquedas`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>fmt(v)}}}}}}}}}});
  charts.cBP=new Chart(document.getElementById('cBP'),{{type:'bar',data:{{labels:bt.map(r=>r.brand?.split(' ')[0]||r.brand),datasets:[{{data:bt.map(r=>r.pct),backgroundColor:bt.map(r=>hex(r.isTurbo?VC.turbo:c,.65)),borderRadius:5,borderSkipped:false}}]}},options:{{...copts(),plugins:{{...copts().plugins,tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.parsed.y}}% ATC`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>v+'%'}}}}}}}}}});
}}

// ── CANIBALIZACIÓN ──
function rCannib(){{
  const c=vc(cV);
  const rows=RAW_DATA[cV]||[];
  const weeks=document.getElementById('weekSel').value==='all'?ALL_WEEKS:[document.getElementById('weekSel').value];
  const kwSet=[...new Set(rows.filter(r=>weeks.includes(r.week)).map(r=>r.keyword))].slice(0,40);
  const diff=kwSet.map(kw=>{{
    let vs=0,va=0,ts=0,ta=0;
    rows.filter(r=>weeks.includes(r.week)&&r.keyword===kw).forEach(r=>{{
      if(r.vertical_sub_group==='turbo'){{ts+= +r.total_searches;ta+= +r.atc_searches}}
      else{{vs+= +r.total_searches;va+= +r.atc_searches}}
    }});
    return{{kw,vp:pct(va,vs),tp:pct(ta,ts),diff:+(pct(ta,ts)-pct(va,vs)).toFixed(1),tvol:ts}};
  }}).sort((a,b)=>Math.abs(b.diff)-Math.abs(a.diff));

  const wins=diff.filter(r=>r.diff>5).slice(0,6);
  const lose=diff.filter(r=>r.diff<-5).slice(0,6);
  const top20=diff.slice(0,20);

  document.getElementById('content').innerHTML=`
  <div class="kpis fu" style="grid-template-columns:repeat(3,1fr)">
    <div class="kpi c2"><div class="kpi-lbl">Keywords Turbo gana</div><div class="kpi-val" style="color:var(--turbo)">${{diff.filter(r=>r.diff>0).length}}</div><div class="kpi-sub">de ${{diff.length}} keywords analizadas</div></div>
    <div class="kpi c1"><div class="kpi-lbl">Keywords ${{cV}} gana</div><div class="kpi-val" style="color:${{c}}">${{diff.filter(r=>r.diff<0).length}}</div><div class="kpi-sub">mayor % ATC</div></div>
    <div class="kpi c3"><div class="kpi-lbl">Diferencia promedio</div><div class="kpi-val">${{(diff.reduce((s,r)=>s+r.diff,0)/diff.length).toFixed(1)}}pp</div><div class="kpi-sub">Turbo − ${{cV}}</div></div>
  </div>
  <div class="g2 fu">
    <div class="card"><div class="card-hd"><div><h2>TURBO vs ${{cV.toUpperCase()}} — % ATC</h2><p>Top 20 keywords</p></div></div><div class="ch360"><canvas id="cCann"></canvas></div></div>
    <div class="card">
      <div class="card-hd"><div><h2>🟢 Turbo convierte mejor (+5pp)</h2></div></div>
      <div class="canib-grid">${{wins.map(r=>`<div class="ck"><div class="ck-name">${{r.kw}}</div>
        <div class="ck-row"><span class="ck-lbl">TURBO</span><span class="ck-val" style="color:var(--turbo)">${{r.tp}}%</span></div>
        <div class="ck-row"><span class="ck-lbl">${{cV.toUpperCase()}}</span><span class="ck-val" style="color:${{c}}">${{r.vp}}%</span></div>
        <div class="ck-row"><span class="ck-lbl">diferencia</span><span class="ck-val up">+${{r.diff}}pp</span></div></div>`).join('')}}
      </div>
      <div style="margin-top:12px"><div class="card-hd" style="margin-bottom:8px"><div><h2>🔴 ${{cV.toUpperCase()}} convierte mejor (+5pp)</h2></div></div>
      <div class="canib-grid">${{lose.map(r=>`<div class="ck"><div class="ck-name">${{r.kw}}</div>
        <div class="ck-row"><span class="ck-lbl">${{cV.toUpperCase()}}</span><span class="ck-val" style="color:${{c}}">${{r.vp}}%</span></div>
        <div class="ck-row"><span class="ck-lbl">TURBO</span><span class="ck-val" style="color:var(--turbo)">${{r.tp}}%</span></div>
        <div class="ck-row"><span class="ck-lbl">diferencia</span><span class="ck-val dn">${{r.diff}}pp</span></div></div>`).join('')}}
      </div></div>
    </div>
  </div>`;

  charts.cCann=new Chart(document.getElementById('cCann'),{{type:'bar',data:{{labels:top20.map(r=>r.kw),datasets:[
    {{label:cV.toUpperCase(),data:top20.map(r=>r.vp),backgroundColor:hex(c,.7),borderRadius:4,borderSkipped:false}},
    {{label:'TURBO',data:top20.map(r=>r.tp),backgroundColor:hex(VC.turbo,.7),borderRadius:4,borderSkipped:false}}
  ]}},options:{{...copts(),indexAxis:'y',plugins:{{legend:{{display:true,labels:{{color:'#8b95a8',font:{{family:'IBM Plex Mono',size:9}},boxWidth:8}}}},tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.dataset.label}}: ${{ctx.parsed.x}}%`}}}}}},scales:{{...copts().scales,x:{{...copts().scales.x,ticks:{{...copts().scales.x.ticks,callback:v=>v+'%'}}}}}}}}}});
}}

// ── TENDENCIA ──
function rTrends(){{
  const c=vc(cV);
  const bt=brandStats().slice(0,4);
  document.getElementById('content').innerHTML=`
  <div class="g2 fu">
    <div class="card"><div class="card-hd"><div><h2>% ATC semanal — Top brands</h2></div></div><div class="ch300"><canvas id="cTB"></canvas></div></div>
    <div class="card"><div class="card-hd"><div><h2>Vertical vs TURBO — % ATC semanal</h2></div></div><div class="ch300"><canvas id="cTV"></canvas></div></div>
  </div>
  <div class="card fu"><div class="card-hd"><div><h2>Keyword individual — tendencia semanal</h2></div>
    <select class="ctrl" id="kwPick" onchange="rKwT()" style="font-size:10px">
      ${{kwStats().slice(0,25).map((k,i)=>`<option value="${{k.kw}}"${{i===0?' selected':''}}>${{k.kw}}</option>`).join('')}}
    </select></div>
    <div class="ch260"><canvas id="cTK"></canvas></div></div>`;

  const pal=[c,VC.turbo,'#f0b429','#34d399'];
  charts.cTB=new Chart(document.getElementById('cTB'),{{type:'line',data:{{labels:wlbl,datasets:bt.map((b,i)=>{{
    const wd=weeklyBrand(b.brand);
    return{{label:b.brand,data:wd.map(r=>r.pct),borderColor:b.isTurbo?VC.turbo:pal[i]||c,backgroundColor:'transparent',
      pointBackgroundColor:b.isTurbo?VC.turbo:pal[i]||c,pointRadius:3,tension:0.3,borderWidth:b.isTurbo?2:1.5,borderDash:b.isTurbo?[4,3]:[]}};}})}},
    options:{{...copts(),plugins:{{legend:{{display:true,labels:{{color:'#8b95a8',font:{{family:'IBM Plex Mono',size:9}},boxWidth:8}}}},tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.dataset.label}}: ${{ctx.parsed.y}}%`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>v+'%'}}}}}}}}}});

  const vw=weeklyAll(),tw=weeklyTurbo();
  charts.cTV=new Chart(document.getElementById('cTV'),{{type:'line',data:{{labels:wlbl,datasets:[
    {{label:cV.toUpperCase(),data:vw.map(r=>r.pct),borderColor:c,backgroundColor:hex(c,.08),fill:true,tension:0.3,pointRadius:3,borderWidth:2}},
    {{label:'TURBO',data:tw.map(r=>r.pct),borderColor:VC.turbo,backgroundColor:hex(VC.turbo,.06),fill:true,tension:0.3,pointRadius:3,borderWidth:2,borderDash:[4,3]}}
  ]}},options:{{...copts(),plugins:{{legend:{{display:true,labels:{{color:'#8b95a8',font:{{family:'IBM Plex Mono',size:9}},boxWidth:8}}}},tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.dataset.label}}: ${{ctx.parsed.y}}%`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>v+'%'}}}}}}}}}});
  rKwT();
}}

function rKwT(){{
  const kw=document.getElementById('kwPick')?.value; if(!kw)return;
  const c=vc(cV),pal=[c,VC.turbo,'#f0b429','#34d399'];
  const brands=brandStats().slice(0,4);
  if(charts.cTK){{charts.cTK.destroy();delete charts.cTK}}
  charts.cTK=new Chart(document.getElementById('cTK'),{{type:'line',data:{{labels:wlbl,datasets:brands.map((b,i)=>{{
    const data=ALL_WEEKS.slice().reverse().map(w=>{{
      const rows=(RAW_DATA[cV]||[]).filter(r=>r.week===w&&r.keyword===kw&&r.brand_group===b.brand);
      const s=rows.reduce((a,r)=>a+ +r.total_searches,0),a=rows.reduce((a,r)=>a+ +r.atc_searches,0);
      return pct(a,s);
    }});
    return{{label:b.brand,data,borderColor:b.isTurbo?VC.turbo:pal[i]||c,backgroundColor:'transparent',
      pointBackgroundColor:b.isTurbo?VC.turbo:pal[i]||c,pointRadius:3,tension:0.3,borderWidth:b.isTurbo?2:1.5,borderDash:b.isTurbo?[4,3]:[]}};}})}},
    options:{{...copts(),plugins:{{legend:{{display:true,labels:{{color:'#8b95a8',font:{{family:'IBM Plex Mono',size:9}},boxWidth:8}}}},tooltip:{{...copts().plugins.tooltip,callbacks:{{label:ctx=>` ${{ctx.dataset.label}}: ${{ctx.parsed.y}}%`,title:t=>`"${{kw}}" · ${{t[0].label}}`}}}}}},scales:{{...copts().scales,y:{{...copts().scales.y,ticks:{{...copts().scales.y.ticks,callback:v=>v+'%'}}}}}}}}}});
}}

// ── INIT ──
const badge=document.getElementById('vbadge');
badge.style.background=hex(VC.super,.15);badge.style.color=VC.super;
render();
</script>
</body>
</html>"""

# ================================================================
# PUSH A GITHUB
# ================================================================
def git_push(updated_at):
    print("📤 Subiendo a GitHub Pages...")
    cmds = [
        ["git", "add", "index.html"],
        ["git", "commit", "-m", f"Dashboard actualizado: {updated_at}"],
        ["git", "push", "origin", "main"]
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"   Error en {' '.join(cmd)}:")
            print(result.stderr)
        else:
            print(f"   ✓ {' '.join(cmd[:2])}")

# ================================================================
# MAIN
# ================================================================
def main():
    print("=" * 55)
    print("  Rappi Search Dashboard — Refresh")
    print("=" * 55)

    conn = connect()
    weeks = get_available_weeks(conn)

    if not weeks:
        print("❌ No se encontraron semanas disponibles. Abortando.")
        sys.exit(1)

    data = {}
    for vertical in ["super", "farmacia", "mascotas", "especializada"]:
        df = run_query(conn, vertical, weeks)
        data[vertical] = df

    conn.close()
    print("\n✅ Queries completadas. Generando dashboard...\n")

    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = generate_html(data, weeks, updated_at)

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"✅ index.html generado ({len(html)//1024} KB)\n")

    git_push(updated_at)

    print("\n" + "=" * 55)
    print("  ✅ Dashboard actualizado exitosamente")
    print(f"  🌐 https://davidgil-lgtm.github.io/rappi-search-dashboard/")
    print("=" * 55)
    input("\nPresiona Enter para cerrar...")

if __name__ == "__main__":
    main()
