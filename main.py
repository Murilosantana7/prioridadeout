import pandas as pd
import gspread
import requests
import time
import base64
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Reporte prioridade'
INTERVALO = 'A:E'
CAMINHO_IMAGEM = "alerta.gif"

# 👥 DICIONÁRIO DE PESSOAS POR TURNO (COM IDS REAIS!)
TURNO_PARA_IDS = {
    "Turno 1": [
        "1285879030",  # Priscila Cristofaro
        "9465967606",  # Fidel Lúcio
        "1268695707"   # Claudio Olivatto
    ],
    "Turno 2": [
        "9260655622",  # Mariane Marquezini
        "1386559133",  # Murilo Santana
        "1298055860"   # Matheus Damas
    ],
    "Turno 3": [
        "1210347148",  # Fernando Aparecido da Costa
        "9474534910",  # Kaio Baldo
        "1499919880"   # Sandor Nemes
    ]
}


def identificar_turno_atual():
    """Identifica o turno atual baseado na hora de São Paulo."""
    agora = datetime.now(timezone('America/Sao_Paulo'))
    hora = agora.hour

    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"


def autenticar_google():
    """Autentica com a API do Google."""
    creds_json_str = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_json_str:
        print("❌ Erro: Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
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
    """Formata o texto da doca (função original)."""
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


def obter_dados_expedicao(cliente, spreadsheet_id):
    """Busca dados da planilha e trata as colunas da aba 'Reporte prioridade'."""
    if not cliente:
        return None, "⚠️ Cliente não autenticado."

    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    # Usando o caractere de acento agudo (´)
    colunas_necessarias = ['LT', 'Nome do Motorista', 'DOCA', "TO´s"]
    for col in colunas_necessarias:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada na aba '{NOME_ABA}'."

    df = df[df['LT'].str.strip() != '']
    return df, None


def montar_mensagem_alerta(df):
    """Monta a mensagem de alerta para TODAS as LTs na aba 'Reporte prioridade'."""
    
    if df.empty:
        return None

    mensagens = []
    
    mensagens.append(f"⚠️ Atenção Prioridade de descarga!")
    mensagens.append("")
    mensagens.append("")

    for _, row in df.iterrows():
        lt = row['LT'].strip()
        motorista = row['Nome do Motorista'].strip()
        doca = formatar_doca(row['DOCA'])
        
        # Usando o nome da coluna com acento agudo
        tos = row["TO´s"].strip()
        
        mensagens.append(f"🚛 {lt}")
        mensagens.append(f"{doca}")
        mensagens.append(f"Motorista: {motorista}")
        mensagens.append(f"Qntd de TO´s: {tos}")
        
        mensagens.append("") 

    if mensagens and mensagens[-1] == "":
        mensagens.pop()

    return "\n".join(mensagens)


def enviar_imagem(webhook_url: str, caminho_imagem: str = CAMINHO_IMAGEM):
    """Envia a imagem de alerta (função original)."""
    if not webhook_url:
        print("❌ WEBHOOK_URL não definida.")
        return False
    try:
        with open(caminho_imagem, "rb") as f:
            raw_image_content = f.read()
            base64_encoded_image = base64.b64encode(raw_image_content).decode("utf-8")
        payload = {"tag": "image", "image_base64": {"content": base64_encoded_image}}
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Imagem enviada com sucesso.")
        return True
    except FileNotFoundError:
        print(f"❌ Arquivo '{caminho_imagem}' não encontrado. Pulando imagem...")
        return False
    except Exception as e:
        print(f"❌ Erro ao enviar imagem: {e}")
        return False


def enviar_webhook_com_mencao_oficial(mensagem_texto: str, webhook_url: str, user_ids: list = None):
    """Envia a mensagem de texto e marca os usuários (função original)."""
    if not webhook_url:
        print("❌ WEBHOOK_URL não definida.")
        return

    mensagem_final = f"{mensagem_texto}"

    payload = {
        "tag": "text",
        "text": {
            "format": 1,
            "content": mensagem_final
        }
    }

    if user_ids:
        user_ids_validos = [uid for uid in user_ids if uid and uid.strip()]
        if user_ids_validos:
            payload["text"]["mentioned_list"] = user_ids_validos
            print(f"✅ Enviando menção para: {user_ids_validos}")
        else:
            print("⚠️ Nenhum ID válido para marcar.")

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem com menção OFICIAL enviada com sucesso.")
    except Exception as e:
        print(f"❌ Falha ao enviar mensagem: {e}")


def main():
    """Função principal para rodar o bot."""
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Variáveis SEATALK_WEBHOOK_URL ou SPREADSHEET_ID não definidas.")
        return

    cliente = autenticar_google()
    if not cliente:
        return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem_alerta(df)

    if mensagem:
        turno_atual = identificar_turno_atual()
        ids_para_marcar = TURNO_PARA_IDS.get(turno_atual, [])

        print(f"🕒 Turno atual: {turno_atual}")
        print(f"👥 IDs configurados para este turno: {ids_para_marcar}")

        # ✨ ALTERADO: A mensagem de texto agora é enviada PRIMEIRO.
        enviar_webhook_com_mencao_oficial(mensagem, webhook_url, user_ids=ids_para_marcar)
        
        # ✨ ALTERADO: A imagem agora é enviada DEPOIS.
        enviar_imagem(webhook_url)
    else:
        print("✅ Nenhuma LT na aba 'Reporte prioridade'. Nada enviado.")


if __name__ == "__main__":
    main()
