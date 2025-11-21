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
NOME_ABA = 'Reporte prioridade'
INTERVALO = 'A:F'
CAMINHO_IMAGEM = "alerta.gif"

# --- FUSO HORÁRIO ---
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')

# --- CONFIGURAÇÃO DE TURNOS E IDS ---
TURNO_PARA_IDS = {
    "Turno 1": [
        "1323672252",  # Leticia Tena
        "9465967606",  # Fidel Lúcio
        "1268695707",  # Claudio Olivatto
        "1361341535"   # Iran Castro
    ],
    "Turno 2": [
        "9260655622",  # Mariane Marquezini
        "1311194991",  # Cinara Lopes
        "1386559133",  # Murilo Santana
        "1298055860"   # Matheus Damas
    ],
    "Turno 3": [
        "1210347148",  # Danilo Pereira
        "9474534910",  # Kaio Baldo
        "1499919880"   # Sandor Nemes
    ]
}

# --- CONFIGURAÇÃO DE FOLGAS (0=Segunda ... 6=Domingo) ---
# 0=Seg, 1=Ter, 2=Qua, 3=Qui, 4=Sex, 5=Sab, 6=Dom
DIAS_DE_FOLGA = {
    # Turno 1
    "1323672252": [6, 0], # Leticia (Dom, Seg)
    "9465967606": [5, 6], # Fidel (Sab, Dom)
    "1268695707": [6],    # Claudio (Dom)
    "1361341535": [6, 0],    # Iran (Dom, Seg)

    # Turno 2
    "9260655622": [5, 6], # Mariane (Sab, Dom)
    "1311194991": [6, 0], # Cinara (Dom, Seg)
    "1386559133": [6, 0], # Murilo (Dom, Seg)
    "1298055860": [6],    # Matheus (Dom)

    # Turno 3
    "1210347148": [5, 6], # Danilo (Sab, Dom)
    "9474534910": [6, 0], # Kaio (Dom, Seg)
    "1499919880": []      # Sandor (Sem folga fixa)
}

def identificar_turno_atual(agora):
    """Identifica o turno atual baseado na hora de São Paulo."""
    hora = agora.hour
    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"

def filtrar_quem_esta_de_folga(ids_do_turno, agora):
    """Remove da lista de IDs quem tem folga no dia da semana atual."""
    dia_semana_hoje = agora.weekday() # 0=Segunda ... 6=Domingo
    ids_validos = []
    
    nomes_dias = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    print(f"📅 Hoje é {nomes_dias[dia_semana_hoje]}. Verificando escalas...")

    for uid in ids_do_turno:
        dias_off_da_pessoa = DIAS_DE_FOLGA.get(uid, [])
        
        if dia_semana_hoje in dias_off_da_pessoa:
            print(f"🏖️ ID {uid} está de folga hoje. Não será marcado.")
        else:
            ids_validos.append(uid)
            
    return ids_validos

def autenticar_google():
    """Autentica com a API do Google."""
    creds_json_str = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_json_str:
        print("❌ Erro: Variável 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
        return None
    try:
        creds_dict = json.loads(creds_json_str)
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        print("✅ Cliente autenticado.")
        return cliente
    except Exception as e:
        print(f"❌ Erro ao autenticar: {e}")
        return None

def formatar_doca(doca):
    doca = doca.strip()
    if not doca or doca == '-': return "Doca --"
    elif doca.startswith("EXT.OUT"):
        numeros = ''.join(filter(str.isdigit, doca))
        return f"Doca {numeros}"
    elif not doca.startswith("Doca"): return f"Doca {doca}"
    else: return doca

def obter_dados_expedicao(cliente, spreadsheet_id):
    if not cliente: return None, "⚠️ Cliente não autenticado."
    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e: return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2: return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    colunas_necessarias = ['LT', 'Nome do Motorista', 'DOCA', "TO´s", 'Próximo ETA']
    for col in colunas_necessarias:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada na aba '{NOME_ABA}'."

    df = df[df['LT'].str.strip() != '']
    return df, None

def formatar_tempo_restante(eta_datetime, agora):
    if not eta_datetime: return ""
    diferenca = eta_datetime - agora
    total_minutos = int(diferenca.total_seconds() / 60)

    if total_minutos < 0:
        minutos_atraso = abs(total_minutos)
        if minutos_atraso < 60: return f"(Atrasado {minutos_atraso} min)"
        else: return f"(Atrasado {minutos_atraso // 60}h {minutos_atraso % 60}min)"
    elif total_minutos == 0: return "(Chegando agora)"
    else:
        if total_minutos < 60: return f"(Faltam {total_minutos} min)"
        else: return f"(Faltam {total_minutos // 60}h {total_minutos % 60}min)"

def montar_mensagem_alerta(df_filtrado, agora):
    if df_filtrado.empty: return None
    mensagens = ["", "⚠️ Atenção, Prioridade de descarga!⚠️", "", ""]

    for _, row in df_filtrado.iterrows():
        lt = row['LT'].strip()
        motorista = row['Nome do Motorista'].strip()
        doca = formatar_doca(row['DOCA'])
        tos = row["TO´s"].strip()
        
        eta_str = row['Próximo ETA'].strip()
        eta_formatado = "--:--"
        tempo_restante_str = ""

        # Lógica de formatação robusta (com ou sem segundos)
        if eta_str:
            try:
                try:
                    eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M:%S')
                except ValueError:
                    eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M')
                
                eta_datetime = FUSO_HORARIO_SP.localize(eta_naive)
                eta_formatado = eta_datetime.strftime('%d/%m %H:%M')
                tempo_restante_str = formatar_tempo_restante(eta_datetime, agora)
            except ValueError:
                eta_formatado = f"{eta_str} (?)"
                
        mensagens.append(f"🚛 {lt}")
        mensagens.append(f"{doca}")
        mensagens.append(f"Motorista: {motorista}")
        mensagens.append(f"Qntd de TO´s: {tos}")
        mensagens.append(f"ETA: {eta_formatado} {tempo_restante_str}")
        mensagens.append("") 

    if mensagens and mensagens[-1] == "": mensagens.pop()
    return "\n".join(mensagens)

def enviar_imagem(webhook_url: str, caminho_imagem: str = CAMINHO_IMAGEM):
    if not webhook_url: return False
    try:
        with open(caminho_imagem, "rb") as f:
            raw_image_content = f.read()
            base64_encoded_image = base64.b64encode(raw_image_content).decode("utf-8")
        payload = {"tag": "image", "image_base64": {"content": base64_encoded_image}}
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Imagem enviada.")
        return True
    except Exception as e:
        print(f"❌ Erro imagem: {e}")
        return False

def enviar_webhook_com_mencao_oficial(mensagem_texto: str, webhook_url: str, user_ids: list = None):
    if not webhook_url: return
    payload = {
        "tag": "text",
        "text": { "format": 1, "content": mensagem_texto }
    }
    if user_ids:
        payload["text"]["mentioned_list"] = user_ids
        print(f"✅ Marcando IDs: {user_ids}")
    
    try:
        requests.post(webhook_url, json=payload).raise_for_status()
        print("✅ Mensagem enviada.")
    except Exception as e: print(f"❌ Erro envio msg: {e}")

def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Variáveis de ambiente faltando.")
        return

    cliente = autenticar_google()
    if not cliente: return

    agora = datetime.now(FUSO_HORARIO_SP)
    df_completo, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return
    
    if df_completo.empty:
        print("✅ Nenhuma LT encontrada.")
        return

    # --- FILTRO DE 10 HORAS COM PROTEÇÃO DE DATA ---
    df_filtrado_lista = []
    limite_alerta = agora + timedelta(hours=10) 
    print(f"🕣 Hora: {agora.strftime('%d/%m %H:%M')}. Filtro até: {limite_alerta.strftime('%d/%m %H:%M')}")

    for index, row in df_completo.iterrows():
        eta_str = row['Próximo ETA'].strip() 
        if not eta_str: continue

        try:
            # Tenta formato completo, se falhar tenta sem segundos
            try:
                eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M:%S')
            except ValueError:
                eta_naive = datetime.strptime(eta_str, '%d/%m/%Y %H:%M')
            
            eta_datetime = FUSO_HORARIO_SP.localize(eta_naive)
            
            if eta_datetime <= limite_alerta:
                df_filtrado_lista.append(row)
        except ValueError:
            print(f"⚠️ Data inválida na linha {index}: '{eta_str}'")
    
    if not df_filtrado_lista:
        print("✅ Nenhuma LT urgente encontrada.")
        return
    
    df_filtrado = pd.DataFrame(df_filtrado_lista)
    mensagem = montar_mensagem_alerta(df_filtrado, agora) 

    if mensagem:
        turno_atual = identificar_turno_atual(agora) 
        ids_brutos = TURNO_PARA_IDS.get(turno_atual, [])

        # --- APLICA FILTRO DE FOLGAS ---
        ids_para_marcar = filtrar_quem_esta_de_folga(ids_brutos, agora)
        # -------------------------------

        print(f"🕒 Turno: {turno_atual}")
        enviar_webhook_com_mencao_oficial(mensagem, webhook_url, user_ids=ids_para_marcar)
        enviar_imagem(webhook_url)

if __name__ == "__main__":
    main()
