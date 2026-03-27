#!/usr/bin/env python3
"""
gerar_dados.py — Busca dados da CVM e gera arquivos JSON estáticos.
Roda pelo GitHub Actions todo dia às 07h BRT.
Saída: docs/data/latest.json + docs/data/YYYY-MM-DD.json
"""

import urllib.request
import zipfile, io, csv, json, re, os, unicodedata
from datetime import date, timedelta

# ── URLs CVM ─────────────────────────────────────────────────────────────────
CVM_CAD_OLD = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
CVM_CAD_ZIP = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip'

def diario_url(ym):
    return f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ym}.zip'

def diario_hist_url(ym):
    return f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{ym}.zip'

# ── HTTP ──────────────────────────────────────────────────────────────────────
def fetch(url, timeout=90):
    print(f'  GET {url.split("/")[-1]}')
    req = urllib.request.Request(url, headers={'User-Agent': 'FundosBR/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def fetch_text(url):
    return fetch(url).decode('latin-1')

# ── ZIP / CSV ─────────────────────────────────────────────────────────────────
def unzip_csvs(data):
    results = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in z.namelist():
            if name.lower().endswith('.csv'):
                results.append((name, z.read(name).decode('latin-1')))
    return results

def parse_csv(text):
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    reader = csv.DictReader(io.StringIO(text), delimiter=';')
    return list(reader)

# ── CNPJ ─────────────────────────────────────────────────────────────────────
def norm(s):
    return re.sub(r'[.\-/\s]', '', s or '').zfill(14)

# ── Classificação ─────────────────────────────────────────────────────────────
def strip_acc(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

def classify(tipo, nome):
    s = strip_acc((tipo + ' ' + nome).upper())
    if re.search(r'FIA\b|FICFIA|LONG.?BIASED|LONG.?ONLY|SMALL.?CAP|DIVIDENDO|\bACAO\b|\bACOES\b', s):
        return 'acoes'
    if re.search(r'CREDITO.?PRIV|RENDA.?FIXA|DEBENTURE|HIGH.?YIELD|INFLACAO|IPCA|\bRF\b|FIRF|FIC.?RF', s):
        return 'credito'
    if re.search(r'MULTIMERCADO|\bFIM\b|FICFIM|MACRO|QUANT|ARBITRAGEM|LONG.?SHORT|TREND|HEDGE', s):
        return 'multi'
    return None

# ── Subtrai meses ─────────────────────────────────────────────────────────────
def sub_months(d, m):
    month = d.month - m
    year  = d.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    day   = min(d.day, [31,28+int((year%4==0 and year%100!=0) or year%400==0),
                         31,30,31,30,31,31,30,31,30,31][month-1])
    return date(year, month, day)

# ── Cadastro ──────────────────────────────────────────────────────────────────
def fetch_cadastro():
    print('\n[Cadastro]')
    all_rows = []

    try:
        rows = parse_csv(fetch_text(CVM_CAD_OLD))
        for r in rows:
            cnpj = norm(r.get('CNPJ_FUNDO', ''))
            if not cnpj or set(cnpj) == {'0'}: continue
            all_rows.append({
                'cnpj': cnpj,
                'nome': r.get('DENOM_SOCIAL', '').strip(),
                'tipo': r.get('TP_FUNDO', '') or r.get('CLASSE', ''),
                'sit':  r.get('SIT', ''),
            })
        print(f'  cad_fi: {len(rows)} linhas')
    except Exception as e:
        print(f'  cad_fi erro: {e}')

    try:
        csvs = unzip_csvs(fetch(CVM_CAD_ZIP))
        for name, content in csvs:
            if 'classe' not in name.lower(): continue
            rows = parse_csv(content)
            for r in rows:
                cnpj = norm(r.get('CNPJ_Classe', ''))
                if not cnpj or set(cnpj) == {'0'}: continue
                all_rows.append({
                    'cnpj': cnpj,
                    'nome': r.get('Denominacao_Social', '').strip(),
                    'tipo': r.get('Tipo_Classe', ''),
                    'sit':  r.get('Situacao', ''),
                })
            print(f'  {name}: {len(rows)} linhas')
    except Exception as e:
        print(f'  registro zip erro: {e}')

    print(f'  Total: {len(all_rows)}')
    return all_rows

# ── Cotas mensais ─────────────────────────────────────────────────────────────
def fetch_mes(ym):
    for url in [diario_url(ym), diario_hist_url(ym)]:
        try:
            csvs = unzip_csvs(fetch(url))
            if csvs:
                rows = parse_csv(csvs[0][1])
                print(f'  {ym}: {len(rows)} linhas')
                return rows
        except Exception as e:
            print(f'  {ym} erro ({url.split("/")[-2]}): {e}')
    return []

# ── Índice de cotas ───────────────────────────────────────────────────────────
def build_index(rows):
    idx = {}
    for r in rows:
        cnpj  = norm(r.get('CNPJ_FUNDO_CLASSE') or r.get('CNPJ_FUNDO') or '')
        dt    = r.get('DT_COMPTC', '')
        try:
            quota = float(r.get('VL_QUOTA', '0').replace(',', '.'))
            pl    = float(r.get('VL_PATRIM_LIQ', '0').replace(',', '.'))
            cot   = int(r.get('NR_COTST', '0') or 0)
        except:
            continue
        if not cnpj or not dt or not quota: continue
        idx.setdefault(cnpj, []).append({'dt': dt, 'q': quota, 'pl': pl, 'c': cot})
    for k in idx:
        idx[k].sort(key=lambda x: x['dt'])
    return idx

def closest_before(entries, target):
    best = None
    for e in entries:
        if e['dt'] <= target: best = e
        else: break
    return best

def find_from(entries, target):
    e = closest_before(entries, target)
    return e or next((x for x in entries if x['dt'] >= target), entries[0])

def ret(a, b):
    if not a or not b or not a['q'] or not b['q']: return None
    return round((b['q'] / a['q'] - 1) * 100, 4)

def compute(entries, base_str):
    base = closest_before(entries, base_str)
    if not base: return None
    bd   = date.fromisoformat(base_str)
    y, m = bd.year, bd.month
    return {
        'retMTD': ret(find_from(entries, f'{y}-{m:02d}-01'), base),
        'retYTD': ret(find_from(entries, f'{y}-01-01'),      base),
        'ret12m': ret(find_from(entries, sub_months(bd,12).isoformat()), base),
        'ret24m': ret(find_from(entries, sub_months(bd,24).isoformat()), base),
        'pl':      base['pl'],
        'cotistas': base['c'],
        'lastDt':  base['dt'],
    }

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Monta fund_map com fundos ativos classificados
    cad = fetch_cadastro()
    fund_map = {}
    for f in cad:
        sit = strip_acc(f['sit'].upper())
        if 'FUNCIONAMENTO' not in sit and 'ATIVO' not in sit: continue
        nome = f['nome']
        if not nome: continue
        if 'MASTER' in strip_acc(nome.upper()): continue
        cat = classify(f['tipo'], nome)
        if not cat: continue
        fund_map[f['cnpj']] = {'nome': nome, 'cat': cat}

    print(f'\n[FundMap] {len(fund_map)} fundos ativos')

    # Descobre última data disponível (mês atual e anterior)
    today = date.today()
    print('\n[Cotas]')
    all_rows = []
    months = []
    for i in range(26):
        d = sub_months(today, i)
        months.append(f'{d.year}{d.month:02d}')

    for ym in months:
        all_rows.extend(fetch_mes(ym))

    idx = build_index(all_rows)
    print(f'\n[Índice] {len(idx)} CNPJs com cotas')

    # Descobre última data disponível
    all_dates = []
    for entries in idx.values():
        if entries: all_dates.append(entries[-1]['dt'])
    ultima_data = max(all_dates) if all_dates else (today - timedelta(days=2)).isoformat()
    print(f'\n[Última data] {ultima_data}')

    # Calcula retornos
    print('\n[Calculando retornos]')
    funds = []
    matched = set(fund_map) & set(idx)
    print(f'  Match: {len(matched)} fundos')

    for cnpj in matched:
        entries = idx[cnpj]
        if len(entries) < 5: continue
        rets = compute(entries, ultima_data)
        if not rets or not rets['pl'] or rets['pl'] < 1_000_000: continue
        funds.append({
            'cnpj': cnpj,
            'nome': fund_map[cnpj]['nome'],
            'cat':  fund_map[cnpj]['cat'],
            **rets,
        })

    funds.sort(key=lambda x: x['pl'] or 0, reverse=True)
    print(f'  Resultado: {len(funds)} fundos')

    # Salva JSON
    from datetime import datetime
    payload = {
        'ultimaData':   ultima_data,
        'geradoEm':     datetime.utcnow().isoformat() + 'Z',
        'totalFundos':  len(funds),
        'funds':        funds,
    }

    os.makedirs('docs/data', exist_ok=True)
    with open('docs/data/latest.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
    with open(f'docs/data/{ultima_data}.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

    print(f'\n✓ Salvo: docs/data/latest.json e docs/data/{ultima_data}.json')

if __name__ == '__main__':
    main()
