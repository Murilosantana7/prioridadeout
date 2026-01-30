import pandas as pd
import gspread
import requests
import time
import base64
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES GERAIS ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# ATENÇÃO: Verifique se o nome da ABA lá embaixo na planilha é esse mesmo.
# O nome do ARQUIVO é "Inbound SP5", mas a aba (tab) tem que ser essa:
NOME_ABA = 'Reporte prioridade' 
INTERVALO = 'A:F' 
CAMINHO_IMAGEM = "alerta.gif"

# --- FUSO HORÁRIO ---
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')

# --- CONFIGURAÇÃO DE TURNOS E IDS ---
TURNO_PARA_IDS = {
    "Turno 1": [
        "1361341535", # Leticia Tena
        "1269340883", # Priscila Cristofaro
        "1323672252", # Leticia Tena
        "9465967606", # Fidel Lúcio
        "1268695707", # Claudio Olivatto
    ],
    "Turno 2": [
        "9382566974", # Eugênio Galvão
        "9474534910", # Murilo Santana Kaio cobrindo T2
        "1298055860", # Matheus Damas
        "1432898616", # Leonardo Caus
    ],
    "Turno 3": [
        "1210347148", # Danilo Pereira
        "9382243574", # Kaio Baldo Joao cobrindo T3
        "1499919880", # Sandor Nemes
    ]
}

# --- CONFIGURAÇÃO DE FOLGAS ---
DIAS_DE_FOLGA = {
    # TURNO 1
    "1323672252": [6, 0], # Leticia Tena
    "9465967606": [5, 6], # Fidel Lúcio
    "1268695707": [6],    # Claudio Olivatto
    "1361341535": [6, 0], # Leticia Tena
    "1269340883": [6, 0], # Priscila Cristofaro
    
    # TURNO 2
    "9382566974": [6, 0],           # Eugênio
    "1386559133": [6, 0],           # Murilo Santana
    "1432898616": [1, 2, 3, 4, 5],  # Leonardo Caus
    "1298055860": [6],              # Matheus Damas
    
    # TURNO 3
    "1210347148": [5, 6], # Danilo Pereira
    "9474534910": [6, 0], # Kaio Baldo
    "1499919880": [6],    # Sandor Nemes
}

def identificar_turno_atual(agora):
    hora = agora.hour
    if 6 <= hora < 14: return "Turno 1"
    elif 14 <= hora < 22: return "Turno 2"
    else: return "Turno 3"

def filtrar_quem_esta_de_folga(ids_do_turno, agora, turno_atual):
    data_referencia = agora
    if turno_atual == "Turno 3" and agora.hour < 6:
        data_referencia = agora - timedelta(days=1)
    
    dia_semana_referencia = data_referencia.weekday()
    ids_validos = []
    
    for uid in ids_do_turno:
        dias_off_da_pessoa = DIAS_DE_FOLGA.get(uid, [])
        if dia_semana_referencia not in dias_off_da_pessoa:
            ids_validos.append(uid)
    return ids_validos

def autenticar_google():
    """
    Autentica no Google Sheets tentando ler o JSON direto
    OU decodificando Base64 (para compatibilidade com GitHub Actions).
    """
    creds_json_str = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if not creds_json_str:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' ausente.")
        return None

    try:
        # TENTATIVA 1: Tenta ler direto (caso o segredo seja o JSON puro)
        creds_dict = json.loads(creds_json_str)
    except json.JSONDecodeError:
        # TENTATIVA 2: Se falhar, assume que está em Base64 e tenta decodificar
        try:
            print("⚠️ JSON direto falhou, tentando decodificar Base64...")
            # Decodifica de Base64 para bytes e depois para string UTF-8
            decoded_bytes = base64.b64decode(creds_json_str)
            decoded_str = decoded_bytes.decode("utf-8")
            creds_dict = json.loads(decoded_str)
        except Exception as e_b64:
            print(f"❌ Falha total na leitura das credenciais (Nem JSON, nem Base64): {e_b64}")
            return None

    try:
        return gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
    except Exception as e:
        print(f"❌ Erro ao autenticar no gspread: {e}")
        return None

def formatar_doca(doca):
    doca = str(doca).strip()
    if not doca or doca == '-': return "Doca --"
    if "EXT.OUT" in doca: return doca # Mantém original se for complexo
    if not doca.lower().startswith("doca"): return f"Doca {doca}"
    return doca

def obter_dados_expedicao(cliente, spreadsheet_id):
    if not cliente: return None, "Cliente nulo."
    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e: return None, f"Erro acesso planilha: {e}"

    if not dados or len(dados) < 2: return None, "Sem dados."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip() # Remove espaços extras dos nomes das colunas

    # AJUSTE IMPORTANTE: Mudei "TO´s" para "TO's" e adicionei "Origem"
    colunas_necessarias = ['LT', 'Nome do Motorista', 'DOCA', "TO's", 'Próximo ETA', 'Origem']
    
    for col in colunas_necessarias:
        if col not in df.columns:
            # Tenta ser flexível com o apóstrofo do TO's se falhar
            if col == "TO's" and "TO´s" in df.columns:
                df.rename(columns={"TO´s": "TO's"}, inplace=True)
            else:
                return None, f"⚠️ Coluna '{col}' não encontrada. Verifique cabeçalhos."

    df = df.fillna("")

    # FILTRO: Remove se LT vazio OU Origem vazia
    df_filtrado = df[
        (df['LT'].str.strip() != '') & 
        (df['Origem'].str.strip() != '')
    ].copy()

    if df_filtrado.empty:
        return None, "Dados filtrados (LT ou Origem vazios)."

    return df_filtrado, None

def formatar_tempo_restante(eta_datetime, agora):
    if not eta_datetime: return ""
    total_minutos = int((eta_datetime - agora).total_seconds() / 60)
    
    if total_minutos < 0:
        atraso = abs(total_minutos)
        if atraso < 60: return f"(Atrasado {atraso} min)"
        return f"(Atrasado {atraso // 60}h {atraso % 60}min)"
    elif total_minutos == 0: return "(Chegando agora)"
    else:
        if total_minutos < 60: return f"(Faltam {total_minutos} min)"
        return f"(Faltam {total_minutos // 60}h {total_minutos % 60}min)"

def montar_mensagem_alerta(df_filtrado, agora):
    if df_filtrado.empty: return None
    mensagens = ["", "⚠️ Atenção, Prioridade de descarga!⚠️", "", ""]

    for _, row in df_filtrado.iterrows():
        lt = row['LT'].strip()
        motorista = row['Nome do Motorista'].strip()
        doca = formatar_doca(row['DOCA'])
        tos = row["TO's"].strip() # Ajustado para o apóstrofo da imagem
        origem = row['Origem'].strip() # Ajustado para Origem
        eta_str = row['Próximo ETA'].strip()
        
        eta_formatado = "--:--"
        tempo_msg = ""

        if eta_str:
            try:
                try:
                    eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M:%S')
                except ValueError:
                    eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M')
                
                eta_dt = FUSO_HORARIO_SP.localize(eta_naive)
                eta_formatado = eta_dt.strftime('%d/%m %H:%M')
                tempo_msg = formatar_tempo_restante(eta_dt, agora)
                
                # Armazena datetime para ordenação se precisar (opcional aqui, pois já vem ordenado do main)
                row['_dt'] = eta_dt
            except:
                eta_formatado = f"{eta_str} (?)"

        mensagens.append(f"🚛 {lt}")
        mensagens.append(f"{doca} | Origem: {origem}")
        mensagens.append(f"Motorista: {motorista}")
        mensagens.append(f"Qntd de TO´s: {tos}")
        mensagens.append(f"ETA: {eta_formatado} {tempo_msg}")
        mensagens.append("") 

    if mensagens and mensagens[-1] == "": mensagens.pop()
    return "\n".join(mensagens)

def enviar_imagem(webhook_url):
    if not os.path.exists(CAMINHO_IMAGEM): return
    try:
        with open(CAMINHO_IMAGEM, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        requests.post(webhook_url, json={"tag": "image", "image_base64": {"content": b64}})
    except: pass

def enviar_msg(texto, webhook_url, user_ids=None):
    payload = {"tag": "text", "text": {"format": 1, "content": texto}}
    if user_ids: payload["text"]["mentioned_list"] = user_ids
    try: requests.post(webhook_url, json=payload).raise_for_status()
    except Exception as e: print(f"Erro envio: {e}")

def main():
    try:
        url = os.environ.get('SEATALK_WEBHOOK_URL')
        sheet_id = os.environ.get('SPREADSHEET_ID')
        if not url or not sheet_id: return print("Faltam variáveis de ambiente.")

        cli = autenticar_google()
        if not cli: return

        df, erro = obter_dados_expedicao(cli, sheet_id)
        if erro: return print(f"⛔ {erro}")

        agora = datetime.now(FUSO_HORARIO_SP)
        limite = agora + timedelta(hours=10)
        
        # Filtro de horário
        df_final = []
        for _, row in df.iterrows():
            eta = row['Próximo ETA']
            if not eta: continue
            try:
                try: dt = datetime.strptime(eta, '%d/%m/%Y %H:%M:%S')
                except: dt = datetime.strptime(eta, '%d/%m/%Y %H:%M')
                dt = FUSO_HORARIO_SP.localize(dt)
                
                if dt <= limite:
                    row['_sort'] = dt
                    df_final.append(row)
            except: pass

        if not df_final: return print("✅ Nada urgente.")
        
        df_envio = pd.DataFrame(df_final).sort_values(by='_sort')
        msg = montar_mensagem_alerta(df_envio, agora)

        if msg:
            turno = identificar_turno_atual(agora)
            ids = filtrar_quem_esta_de_folga(TURNO_PARA_IDS.get(turno, []), agora, turno)
            print(f"🚀 Enviando para {turno}...")
            enviar_msg(msg, url, ids)
            enviar_imagem(url)

    except Exception as e: print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    main()
