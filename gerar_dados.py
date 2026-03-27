#!/usr/bin/env python3
"""
gerar_dados.py — Busca dados da CVM e gera um único JSON com cotas brutas.
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

    # 2. Fetch cotas — 26 meses para cobrir retorno 24M
    today = date.today()
    print('\n[Cotas]')
    all_rows = []
    for i in range(27):
        d = sub_months(today, i)
        all_rows.extend(fetch_mes(f'{d.year}{d.month:02d}'))

    # 3. Build index: cnpj -> {dt -> {q, pl, c}}
    # Guardamos cotas brutas para o dashboard recalcular
    idx = {}  # cnpj -> sorted list of [dt, quota, pl, cotistas]
    for r in all_rows:
        cnpj  = norm(r.get('CNPJ_FUNDO_CLASSE') or r.get('CNPJ_FUNDO') or '')
        dt    = r.get('DT_COMPTC', '')
        try:
            quota = float(r.get('VL_QUOTA', '0').replace(',', '.'))
            pl    = float(r.get('VL_PATRIM_LIQ', '0').replace(',', '.'))
            cot   = int(r.get('NR_COTST', '0') or 0)
        except:
            continue
        if not cnpj or not dt or not quota: continue
        if cnpj not in fund_map: continue  # só fundos relevantes
        idx.setdefault(cnpj, {})[dt] = [round(quota, 8), round(pl, 2), cot]

    print(f'\n[Índice] {len(idx)} fundos com cotas')

    # 4. Filtra fundos com PL mínimo e mínimo de cotas
    ultima_data = ''
    funds_out = []
    for cnpj, cotas_dict in idx.items():
        if len(cotas_dict) < 10: continue
        # sorted dates
        dates = sorted(cotas_dict.keys())
        ultima_data = max(ultima_data, dates[-1])

        # PL do último dia
        last_pl = cotas_dict[dates[-1]][1]
        if last_pl < 1_000_000: continue

        # Serializa cotas como lista de [dt, quota, pl, cotistas] ordenada
        cotas_list = [[dt] + cotas_dict[dt] for dt in dates]

        funds_out.append({
            'cnpj': cnpj,
            'nome': fund_map[cnpj]['nome'],
            'cat':  fund_map[cnpj]['cat'],
            'cotas': cotas_list,  # [[dt, quota, pl, cotistas], ...]
        })

    # Ordena por PL do último dado
    funds_out.sort(key=lambda f: f['cotas'][-1][2] if f['cotas'] else 0, reverse=True)

    print(f'[Resultado] {len(funds_out)} fundos | última data: {ultima_data}')

    # 5. Salva
    os.makedirs('docs/data', exist_ok=True)
    payload = {
        'ultimaData':  ultima_data,
        'geradoEm':    datetime.utcnow().isoformat() + 'Z',
        'totalFundos': len(funds_out),
        'funds':       funds_out,
    }

    with open('docs/data/latest.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

    size_mb = os.path.getsize('docs/data/latest.json') / 1e6
    print(f'\n✓ docs/data/latest.json ({size_mb:.1f} MB)')

if __name__ == '__main__':
    main()
