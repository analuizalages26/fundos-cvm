#!/usr/bin/env python3
"""
gerar_dados.py — Busca dados da CVM e gera um único JSON com cotas brutas.
Estratégia de compressão:
  - Mês corrente: todos os dias úteis
  - Meses anteriores: apenas o último dia disponível de cada mês (fechamento)
O dashboard recalcula retornos no browser para qualquer data.
"""

import urllib.request
import zipfile, io, csv, json, re, os, unicodedata
from datetime import date, timedelta, datetime

CVM_CAD_OLD = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv'
CVM_CAD_ZIP = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip'

def diario_url(ym):
    return f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{ym}.zip'
def diario_hist_url(ym):
    return f'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{ym}.zip'

def fetch(url, timeout=90):
    print(f'  GET {url.split("/")[-1]}')
    req = urllib.request.Request(url, headers={'User-Agent': 'FundosBR/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def fetch_text(url):
    return fetch(url).decode('latin-1')

def unzip_csvs(data):
    results = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in z.namelist():
            if name.lower().endswith('.csv'):
                results.append((name, z.read(name).decode('latin-1')))
    return results

def parse_csv(text):
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    return list(csv.DictReader(io.StringIO(text), delimiter=';'))

def norm(s):
    return re.sub(r'[.\-/\s]', '', s or '').zfill(14)

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

def sub_months(d, m):
    month = d.month - m
    year  = d.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    day   = min(d.day, [31,28+int((year%4==0 and year%100!=0) or year%400==0),
                         31,30,31,30,31,31,30,31,30,31][month-1])
    return date(year, month, day)

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

_cota_cache = {}

def fetch_mes(ym):
    if ym in _cota_cache:
        return _cota_cache[ym]
    for url in [diario_url(ym), diario_hist_url(ym)]:
        try:
            csvs = unzip_csvs(fetch(url))
            if csvs:
                rows = parse_csv(csvs[0][1])
                print(f'  {ym}: {len(rows)} linhas')
                _cota_cache[ym] = rows
                return rows
        except Exception as e:
            print(f'  {ym} erro: {e}')
    _cota_cache[ym] = []
    return []

def main():
    today = date.today()
    current_ym = f'{today.year}{today.month:02d}'

    # 1. Cadastro
    cad = fetch_cadastro()
    fund_map = {}
    for f in cad:
        sit = strip_acc(f['sit'].upper())
        if 'FUNCIONAMENTO' not in sit and 'ATIVO' not in sit: continue
        nome = f['nome']
        if not nome: continue
        cat = classify(f['tipo'], nome)
        if not cat: continue
        fund_map[f['cnpj']] = {'nome': nome, 'cat': cat}

    print(f'\n[FundMap] {len(fund_map)} fundos ativos')

    # 2. Fetch cotas — 26 meses
    print('\n[Cotas]')
    # Separa mês corrente dos anteriores
    months_current = [current_ym]
    months_hist = []
    for i in range(1, 27):
        d = sub_months(today, i)
        months_hist.append(f'{d.year}{d.month:02d}')

    # Todos os dias do mês corrente
    rows_current = fetch_mes(current_ym)

    # Meses históricos — busca mas vamos filtrar só fechamentos
    rows_hist = []
    for ym in months_hist:
        rows_hist.extend(fetch_mes(ym))

    # 3. Build index por fundo
    # Mês corrente: todos os dias
    # Histórico: apenas último dia de cada mês (fechamento)

    # Primeiro, indexa histórico por (cnpj, ym) -> melhor dt + dados
    hist_by_month = {}  # (cnpj, ym) -> {dt, quota, pl, cot} do último dia
    for r in rows_hist:
        cnpj  = norm(r.get('CNPJ_FUNDO_CLASSE') or r.get('CNPJ_FUNDO') or '')
        dt    = r.get('DT_COMPTC', '')
        if not cnpj or not dt or cnpj not in fund_map: continue
        try:
            quota = float(r.get('VL_QUOTA', '0').replace(',', '.'))
            pl    = float(r.get('VL_PATRIM_LIQ', '0').replace(',', '.'))
            cot   = int(r.get('NR_COTST', '0') or 0)
        except: continue
        if not quota: continue
        ym = dt[:7].replace('-','')  # YYYYMM
        key = (cnpj, ym)
        # Guarda apenas o maior dt (último dia do mês com dado)
        if key not in hist_by_month or dt > hist_by_month[key][0]:
            hist_by_month[key] = (dt, round(quota, 6), round(pl, 0), cot)

    # Indexa mês corrente: todos os dias
    current_by_fund = {}  # cnpj -> list of (dt, quota, pl, cot)
    for r in rows_current:
        cnpj  = norm(r.get('CNPJ_FUNDO_CLASSE') or r.get('CNPJ_FUNDO') or '')
        dt    = r.get('DT_COMPTC', '')
        if not cnpj or not dt or cnpj not in fund_map: continue
        try:
            quota = float(r.get('VL_QUOTA', '0').replace(',', '.'))
            pl    = float(r.get('VL_PATRIM_LIQ', '0').replace(',', '.'))
            cot   = int(r.get('NR_COTST', '0') or 0)
        except: continue
        if not quota: continue
        current_by_fund.setdefault(cnpj, []).append(
            (dt, round(quota, 6), round(pl, 0), cot)
        )

    # 4. Monta lista final por fundo
    all_cnpjs = set(k[0] for k in hist_by_month) | set(current_by_fund)
    ultima_data = ''
    funds_out = []

    for cnpj in all_cnpjs:
        if cnpj not in fund_map: continue

        # Cotas históricas (fechamentos mensais), ordenadas
        hist_cotas = sorted(
            [v for k, v in hist_by_month.items() if k[0] == cnpj],
            key=lambda x: x[0]
        )

        # Cotas do mês corrente (todos os dias), ordenadas
        curr_cotas = sorted(
            current_by_fund.get(cnpj, []),
            key=lambda x: x[0]
        )

        all_cotas = hist_cotas + curr_cotas
        if len(all_cotas) < 5: continue

        # PL do último dia disponível
        last = all_cotas[-1]
        if last[2] < 1_000_000: continue
        ultima_data = max(ultima_data, last[0])

        # Serializa como [[dt, quota, pl, cotistas], ...]
        cotas_list = [list(c) for c in all_cotas]

        funds_out.append({
            'cnpj':  cnpj,
            'nome':  fund_map[cnpj]['nome'],
            'cat':   fund_map[cnpj]['cat'],
            'cotas': cotas_list,
        })

    funds_out.sort(key=lambda f: f['cotas'][-1][2] if f['cotas'] else 0, reverse=True)

    print(f'\n[Resultado] {len(funds_out)} fundos | ultima data: {ultima_data}')

    # 5. Salva
    os.makedirs('docs/data', exist_ok=True)
    payload = {
        'ultimaData':  ultima_data,
        'geradoEm':    datetime.utcnow().isoformat() + 'Z',
        'totalFundos': len(funds_out),
        'funds':       funds_out,
    }

    out_path = 'docs/data/latest.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

    size_mb = os.path.getsize(out_path) / 1e6
    print(f'\n✓ {out_path} ({size_mb:.1f} MB)')
    if size_mb > 90:
        print('⚠ AVISO: arquivo acima de 90MB, pode exceder limite do GitHub (100MB)')

if __name__ == '__main__':
    main()
