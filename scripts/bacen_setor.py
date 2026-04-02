"""
BACEN — Irregularidades por Assunto — Todos os Bancos [v2 — GitHub Live]
Quando rodado com --github: salva docs/data.json e docs/meta.json (incremental)
Quando rodado localmente: comportamento original

pip install requests pandas beautifulsoup4
python scripts/bacen_setor.py --anos 4
python scripts/bacen_setor.py --github --anos 4
"""
import requests, pandas as pd, json, io, sys, time, re
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

GITHUB_MODE = "--github" in sys.argv
OUTPUT_DIR  = Path("docs") if GITHUB_MODE else Path(r"C:\Users\Lenovo\Desktop\Desktop\Mestrado FGV\ControlesInternos")
BASE_HIST   = "https://www3.bcb.gov.br/ranking/historico.do"
HEADERS = {"User-Agent":"Mozilla/5.0 Chrome/124.0","Accept":"text/html,text/csv,*/*",
           "Accept-Language":"pt-BR","Referer":"https://www3.bcb.gov.br/ranking/"}
PAUSA = 1.2

SEGMENTOS = {
    "S1": ["ITAU","ITAÚ","HIPERCARD","REDECARD","BRADESCO","BRADESCARD","NEXT",
           "BANCO DO BRASIL","BB ","BESC","BASA","CAIXA ECONOM","CEF",
           "SANTANDER","BTG PACTUAL","BANCO PAN","NUBANK","NU BANK"],
    "S2": ["SAFRA","VOTORANTIM","INTER ","BANCO INTER","C6 BANK","C6BANK","XP ",
           "ORIGINAL","DAYCOVAL","BANRISUL","ABC BRASIL","MODAL","SICOOB","SICREDI",
           "BANPARA","PAGBANK","PAGSEGURO","NEON","WILL BANK","MERCADO PAGO"],
}
_CONG_MAP = [
    (["ITAU","ITAÚ","HIPERCARD","REDECARD"],"Itaú Unibanco"),
    (["BRADESCO","BRADESCARD","NEXT "],"Bradesco"),
    (["BANCO DO BRASIL","BB ","BB-","BESC","BASA","BANESE"],"Banco do Brasil"),
    (["CAIXA ECONOM","CEF"],"Caixa Econômica Federal"),
    (["SANTANDER"],"Santander"),
    (["BTG PACTUAL","BANCO PAN"," PAN "],"BTG Pactual"),
    (["NUBANK","NU BANK","NU PAGAMENTOS"],"Nubank"),
    (["INTER ","BANCO INTER"],"Banco Inter"),
    (["C6 BANK","C6BANK"],"C6 Bank"),
    (["XP ","XP INVEST"],"XP"),
    (["SAFRA"],"Banco Safra"),
    (["VOTORANTIM"],"Votorantim"),
    (["SICOOB"],"Sicoob"),
    (["SICREDI"],"Sicredi"),
    (["BANRISUL"],"Banrisul"),
    (["PAGBANK","PAGSEGURO"],"PagBank/PagSeguro"),
    (["MERCADO PAGO","MERCADO CRÉDITO","MERCADO CREDITO"],"Mercado Pago"),
    (["NEON"],"Neon"),
    (["ORIGINAL"],"Banco Original"),
    (["DAYCOVAL"],"Daycoval"),
    (["ABC BRASIL"],"ABC Brasil"),
    (["MODAL"],"Banco Modal"),
]

def classificar_segmento(nome):
    nu=nome.upper()
    for seg,nomes in SEGMENTOS.items():
        if any(n in nu for n in nomes): return seg
    return "Outros"

def inferir_conglomerado(nome):
    nu=nome.upper()
    for palavras,cong in _CONG_MAP:
        if any(p in nu for p in palavras): return cong
    return nome

def log(msg,n="INFO"): print(f"[{datetime.now().strftime('%H:%M:%S')}] [{n:5s}] {msg}")
def get_session():
    s=requests.Session(); s.headers.update(HEADERS); return s
def _to_float(v):
    try: return float(str(v).strip().replace(".","").replace(",","."))
    except: return None

def periodo_ja_coletado(ref):
    mp=OUTPUT_DIR/"meta.json"
    if not mp.exists(): return False
    try: return ref in json.loads(mp.read_text("utf-8")).get("periodos_coletados",[])
    except: return False

def carregar_dados_existentes():
    dp=OUTPUT_DIR/"data.json"
    if not dp.exists(): return []
    try: return json.loads(dp.read_text("utf-8")).get("dados",[])
    except: return []

def extrair_links_irr(html):
    soup=BeautifulSoup(html,"html.parser"); links=[]
    for a in soup.find_all("a",href=True):
        t=a.get_text(strip=True)
        if "Irregularidades por institui" in t and "Tabela" not in t and "Cons" not in t and "Bancos" in t:
            href=a["href"]
            url=BASE_HIST+(href if href.startswith("?") else "?"+href)
            secao=""
            for parent in a.parents:
                if not hasattr(parent,"find"): continue
                h3=parent.find(["h3","h2"])
                if h3: secao=h3.get_text(strip=True); break
            links.append({"url":url,"secao":secao})
    return links

def extrair_anos_links(html):
    soup=BeautifulSoup(html,"html.parser"); anos=[]
    for a in soup.find_all("a",href=True):
        t=a.get_text(strip=True)
        if re.match(r"^20\d{2}$",t):
            href=a["href"]
            url=BASE_HIST+(href if href.startswith("?") else "?"+href)
            anos.append({"ano":int(t),"url":url})
    return sorted(anos,key=lambda x:-x["ano"])

def inferir_ref(secao,ano):
    s=secao.lower()
    if "trimestre" in s:
        m=re.search(r"(\d)[°ºo]?\s*trimestre",s)
        return f"{ano}-T{int(m.group(1)) if m else 1}"
    if "semestre" in s:
        m=re.search(r"(\d)[°ºo]?\s*semestre",s)
        return f"{ano}-S{int(m.group(1)) if m else 1}"
    MESES={"janeiro":1,"fevereiro":2,"março":3,"marco":3,"abril":4,"maio":5,
           "junho":6,"julho":7,"agosto":8,"setembro":9,"outubro":10,"novembro":11,"dezembro":12}
    for nm,num in MESES.items():
        if nm in s: return f"{ano}-M{num:02d}"
    return f"{ano}-?"

def parse_csv(raw):
    for enc in ("latin-1","utf-8-sig","utf-8","cp1252"):
        for sep in (";",",","\t"):
            try:
                df=pd.read_csv(io.StringIO(raw.decode(enc,errors="replace")),sep=sep,dtype=str,on_bad_lines="skip")
                df.columns=df.columns.str.strip()
                if len(df.columns)>=3 and len(df)>0: return df
            except: continue
    return None

def identificar_colunas(df):
    cols={}
    for col in df.columns:
        cu=col.upper()
        if any(k in cu for k in ["IRREGULARIDADE","ASSUNTO","NOME DA IRRE"]) and "irr" not in cols: cols["irr"]=col
        if "PROCEDENTE" in cu and "proc" not in cols: cols["proc"]=col
        if "CONGLOMERADO" in cu and "cong" not in cols: cols["cong"]=col
        if any(k in cu for k in ["NOME","INSTITUICAO","INSTITUIÇÃO"]) and "nome" not in cols: cols["nome"]=col
    if "proc" not in cols:
        for col in list(df.columns)[2:]:
            if df[col].apply(_to_float).notna().sum()>len(df)*0.4: cols["proc"]=col; break
    return cols

def processar_csv(raw,ref):
    df=parse_csv(raw)
    if df is None: return pd.DataFrame()
    cols=identificar_colunas(df)
    col_nome=cols.get("nome"); col_irr=cols.get("irr"); col_proc=cols.get("proc")
    if not all([col_nome,col_irr,col_proc]): return pd.DataFrame()
    rows=[]
    for _,row in df.iterrows():
        nome=str(row.get(col_nome,"")).strip()
        assunto=str(row.get(col_irr,"")).strip()
        proc=_to_float(row.get(col_proc))
        if nome and assunto and proc is not None:
            cong_csv=str(row.get(cols.get("cong",""),"")).strip()
            cong=cong_csv if cong_csv and cong_csv not in ("-","") else inferir_conglomerado(nome)
            rows.append({"Referencia":ref,"Banco":nome,"Conglomerado":cong,
                         "Segmento":classificar_segmento(nome),"Assunto":assunto,"Qtd_Procedentes":proc})
    return pd.DataFrame(rows)

def coletar(s,anos_max,novos_only=False):
    all_frames=[]; refs_novos=[]
    def proc_pagina(html,ano):
        for lk in extrair_links_irr(html):
            ref=inferir_ref(lk["secao"],ano)
            if novos_only and periodo_ja_coletado(ref):
                log(f"  {ref} ja coletado — pulando."); continue
            sys.stdout.write(f"\r  {ref:<12s} baixando...   "); sys.stdout.flush()
            try:
                r=s.get(lk["url"],timeout=50); r.raise_for_status()
                raw=r.content
                if len(raw)<5000 or raw.lstrip()[:1]==b"<": continue
                df=processar_csv(raw,ref)
                if not df.empty:
                    all_frames.append(df); refs_novos.append(ref)
                    log(f"  {ref:<12s} OK — {df['Banco'].nunique()} bancos")
            except Exception as e: log(f"  {ref} erro: {e}","WARN")
            time.sleep(PAUSA)
    log("Carregando pagina BACEN...")
    r=s.get(BASE_HIST,timeout=30); r.raise_for_status()
    html=r.text; anos_links=extrair_anos_links(html)
    log(f"Anos: {[a['ano'] for a in anos_links]}"); print()
    ano_atual=anos_links[0]["ano"] if anos_links else datetime.now().year
    log(f"--- {ano_atual} ---"); proc_pagina(html,ano_atual); print()
    for item in anos_links[1:anos_max]:
        time.sleep(1.5); log(f"--- {item['ano']} ---")
        try:
            r2=s.get(item["url"],timeout=30); r2.raise_for_status()
            proc_pagina(r2.text,item["ano"]); print()
        except Exception as e: log(f"  Erro {item['ano']}: {e}","WARN")
    return (pd.concat(all_frames,ignore_index=True) if all_frames else pd.DataFrame()), refs_novos

def preparar_json(df):
    if df.empty: return {}
    refs_ord=sorted(df["Referencia"].unique().tolist())
    top_bancos=(df.groupby(["Banco","Conglomerado","Segmento"])["Qtd_Procedentes"]
                  .sum().sort_values(ascending=False).reset_index().head(50).to_dict("records"))
    top_assuntos=(df.groupby("Assunto")["Qtd_Procedentes"]
                    .sum().sort_values(ascending=False).reset_index().head(30).to_dict("records"))
    pivot=df.groupby(["Referencia","Banco","Conglomerado","Segmento","Assunto"])["Qtd_Procedentes"].sum().reset_index()
    return {
        "gerado_em": datetime.now().isoformat(),
        "periodos":      refs_ord,
        "bancos":        df[["Banco","Conglomerado","Segmento"]].drop_duplicates().sort_values(["Segmento","Conglomerado","Banco"]).to_dict("records"),
        "conglomerados": sorted(df["Conglomerado"].dropna().unique().tolist()),
        "assuntos":      [a["Assunto"] for a in top_assuntos],
        "top_bancos":    [{"banco":b["Banco"],"conglomerado":b.get("Conglomerado",""),"segmento":b["Segmento"],"total":round(b["Qtd_Procedentes"])} for b in top_bancos],
        "top_assuntos":  [{"assunto":a["Assunto"],"total":round(a["Qtd_Procedentes"])} for a in top_assuntos],
        "dados":         pivot.rename(columns={"Qtd_Procedentes":"qtd"}).to_dict("records"),
    }

def main():
    inicio=datetime.now()
    anos_max=4
    if "--anos" in sys.argv:
        try: anos_max=int(sys.argv[sys.argv.index("--anos")+1])
        except: pass
    s=get_session()
    log("="*60); log(f"BACEN {'[GitHub]' if GITHUB_MODE else '[Local]'} — anos={anos_max}"); log("="*60)
    df_novo,refs_novos=coletar(s,anos_max,novos_only=GITHUB_MODE)
    if df_novo.empty and GITHUB_MODE:
        log("Nenhum periodo novo. Dashboard ja esta atualizado."); return
    if GITHUB_MODE and not df_novo.empty:
        dados_exist=carregar_dados_existentes()
        if dados_exist:
            df_ex=pd.DataFrame(dados_exist).rename(columns={"qtd":"Qtd_Procedentes"})
            df_ex=df_ex[~df_ex["Referencia"].isin(refs_novos)]
            df_novo=pd.concat([df_ex,df_novo],ignore_index=True)
    dados=preparar_json(df_novo)
    OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
    jp=OUTPUT_DIR/"data.json"
    with open(jp,"w",encoding="utf-8") as f: json.dump(dados,f,ensure_ascii=False,separators=(",",":"),default=str)
    log(f"data.json: {jp} ({jp.stat().st_size//1024}KB)")
    meta={"ultima_atualizacao":datetime.now().isoformat(),"ultimo_periodo":dados.get("periodos",[""])[-1],
          "periodos_coletados":dados.get("periodos",[]),"n_bancos":len(dados.get("bancos",[])),
          "n_registros":len(dados.get("dados",[]))}
    with open(OUTPUT_DIR/"meta.json","w",encoding="utf-8") as f: json.dump(meta,f,ensure_ascii=False,indent=2)
    log(f"Concluido em {(datetime.now()-inicio).seconds}s")

if __name__=="__main__": main()
