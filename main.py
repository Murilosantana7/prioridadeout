import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
from datetime import datetime, timedelta
from pytz import timezone
import os  # Importar a biblioteca os

# --- CONSTANTES GLOBAIS ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# O caminho agora é relativo, pois o GitHub Action criará o arquivo no mesmo diretório
SERVICE_ACCOUNT_FILE = 'credentials.json' 
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'
# O SPREADSHEET_ID e o WEBHOOK_URL foram removidos daqui

def autenticar_google():
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        print("✅ Credenciais autenticadas com sucesso!")
        return creds
    except Exception as e:
        print(f"⚠️ Erro na autenticação: {e}")
        return None

def identificar_turno(hora):
    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"

# Adaptado para receber o spreadsheet_id como argumento
def obter_dados_expedicao(spreadsheet_id):
    creds = autenticar_google()
    if not creds:
        return None, "⚠️ Não foi possível autenticar as credenciais."

    try:
        cliente = gspread.authorize(creds)
        # Usa o ID recebido por argumento
        planilha = cliente.open_by_key(spreadsheet_id) 
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado na planilha."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    for col in ['Doca', 'LH Trip Number', 'Station Name', 'CPT']:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca(doca):
    doca = doca.strip()
    if not doca or doca == '-':
        return "Doca --"
    elif doca.startswith("EXT.OUT"):
        numeros = ''.join(filter(str.isdigit, doca))
        return f"Doca {numeros}"
    elif not doca.startswith("Doca"):
        return f"Doca {doca}"
    else:
        return doca

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    if df_2h.empty:
        mensagens.append("🚛 LTs pendentes:\n\n✅ Sem LT pendente para as próximas 2h.\n")
    else:
        mensagens.append("🚛 LTs pendentes:\n")
        df_2h['Hora'] = df_2h['CPT'].dt.hour

        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd_lhs = len(grupo)
            mensagens.append(f"{qtd_lhs} LH{'s' if qtd_lhs > 1 else ''} pendente{'s' if qtd_lhs > 1 else ''} às {hora:02d}h\n")
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                destino = row['Station Name'].strip()
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca = formatar_doca(row['Doca'])

                minutos = int((cpt - agora).total_seconds() // 60)
                if minutos < 0:
                    prefixo = "❗️"
                    status = "(ATRASADO)"
                elif minutos <= 10:
                    prefixo = "⚠️"
                    status = f"(FALTAM {minutos} MIN)"
                else:
                    prefixo = ""
                    status = ""

                mensagens.append(f"{prefixo} {lt} | {doca} | Destino: {destino} | CPT: {cpt_str} {status}".strip())
            mensagens.append("")

    mensagens.append("─" * 40)
    mensagens.append("LH´s pendentes para os próximos turnos:\n")

    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        mensagens.append(f"⚠️ {qtd} LH{'s' if qtd != 1 else ''} pendente{'s' if qtd != 1 else ''} no {turno}")

    return "\n".join(mensagens)

# Adaptado para receber a webhook_url como argumento
def enviar_webhook(mensagem, webhook_url):
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": mensagem
            }
        }
        # Usa a URL recebida por argumento
        response = requests.post(webhook_url, json=payload) 
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")

# Adaptado para receber e repassar a webhook_url
def enviar_em_blocos(mensagem, webhook_url, limite=3000):
    linhas = mensagem.split('\n')
    bloco = []
    for linha in linhas:
        bloco.append(linha)
        if len("\n".join(bloco)) > limite:
            bloco.pop()
            # Repassa a URL para a função enviar_webhook
            enviar_webhook("```\n" + "\n".join(bloco) + "\n```", webhook_url) 
            time.sleep(1)
            bloco = [linha]
    if bloco:
        # Repassa a URL para a função enviar_webhook
        enviar_webhook("```\n" + "\n".join(bloco) + "\n```", webhook_url) 

def main():
    # Carrega as variáveis de ambiente fornecidas pelo GitHub Actions
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    # Validação para garantir que os segredos foram carregados
    if not webhook_url or not spreadsheet_id:
        print("❌ Erro: Variáveis de ambiente SEATALK_WEBHOOK_URL e/ou SPREADSHEET_ID não definidas.")
        print("Verifique os 'Secrets' do repositório no GitHub.")
        return

    # Passa o spreadsheet_id como argumento
    df, erro = obter_dados_expedicao(spreadsheet_id) 
    if erro:
        print(erro)
        return
    
    mensagem = montar_mensagem(df)
    
    # Passa a webhook_url como argumento
    enviar_em_blocos(mensagem, webhook_url) 

if __name__ == "__main__":
    main()
