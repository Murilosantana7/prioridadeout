import pandas as pd
import gspread
import requests
import time
import base64
from datetime import datetime, timedelta
from pytz import timezone # Importação correta
import os
import json

# --- CONSTANTES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Reporte prioridade'
INTERVALO = 'A:F'  # CORRIGIDO: Até a Coluna F para pegar o ETA
CAMINHO_IMAGEM = "alerta.gif"

# Constantes de Fuso Horário e Formato de Data
FUSO_HORARIO_SP = timezone('America/Sao_Paulo')
# ATENÇÃO: Formato da planilha (ex: 31/10/2025 09:00)
FORMATO_ETA = '%d/%m/%Y %H:%M:%S' 

# 👥 DICIONÁRIO DE PESSOAS POR TURNO (COM IDS REAIS!)
TURNO_PARA_IDS = {
    "Turno 1": [
        "1285879030",  # Priscila Cristofaro
        "9465967606",  # Fidel Lúcio
        "1268695707"   # Claudio Olivatto
    ],
    "Turno 2": [
        "9260655622",  # Mariane Marquezini
        "1311194991",  # Cinara Lopes
        "1386559133",  # Murilo Santana
        "1298055860"   # Matheus Damas
    ],
    "Turno 3": [
        "1210347148",  # Fernando Aparecido da Costa
        "9474534910",  # Kaio Baldo
        "1499919880"   # Sandor Nemes
    ]
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

    # ALTERADO: Nome da coluna "Próximo ETA"
    colunas_necessarias = ['LT', 'Nome do Motorista', 'DOCA', "TO´s", 'Próximo ETA']
    for col in colunas_necessarias:
        if col not in df.columns:
            # Mensagem de erro específica se a coluna 'Próximo ETA' não for encontrada
            if col == 'Próximo ETA':
                return None, f"⚠️ Coluna '{col}' não encontrada. Verifique se o nome está correto na planilha e se o 'INTERVALO' (A:F) está certo."
            return None, f"⚠️ Coluna '{col}' não encontrada na aba '{NOME_ABA}'."

    df = df[df['LT'].str.strip() != '']
    return df, None


def formatar_tempo_restante(eta_datetime, agora):
    """Calcula e formata o tempo restante ou atraso."""
    if not eta_datetime:
        return "" # Se não há ETA, não retorna nada

    diferenca = eta_datetime - agora
    total_minutos = int(diferenca.total_seconds() / 60)

    if total_minutos < 0:
        # Atrasado
        minutos_atraso = abs(total_minutos)
        if minutos_atraso < 60:
            return f"(Atrasado {minutos_atraso} min)"
        else:
            horas_atraso = minutos_atraso // 60
            min_restantes = minutos_atraso % 60
            return f"(Atrasado {horas_atraso}h {min_restantes}min)"
    elif total_minutos == 0:
        return "(Chegando agora)"
    else:
        # Faltam
        if total_minutos < 60:
            return f"(Faltam {total_minutos} min)"
        else:
            horas = total_minutos // 60
            minutos = total_minutos % 60
            return f"(Faltam {horas}h {minutos}min)"


def montar_mensagem_alerta(df_filtrado, agora): # Recebe o DF já filtrado
    """Monta a mensagem de alerta para as LTs filtradas."""
    
    # Esta verificação agora é uma segurança extra
    if df_filtrado.empty:
        return None

    mensagens = []

    mensagens.append("")
    mensagens.append(f"⚠️ Atenção, Prioridade de descarga!⚠️")
    mensagens.append("")
    mensagens.append("")

    for _, row in df_filtrado.iterrows():
        lt = row['LT'].strip()
        motorista = row['Nome do Motorista'].strip()
        doca = formatar_doca(row['DOCA'])
        tos = row["TO´s"].strip()
        
        # ALTERADO: Usando a coluna 'Próximo ETA'
        eta_str = row['Próximo ETA'].strip()
        eta_datetime = None
        eta_formatado = "--:--" # Valor padrão
        tempo_restante_str = ""

        if eta_str:
            try:
                eta_naive = datetime.strptime(eta_str, FORMATO_ETA)
                eta_datetime = FUSO_HORARIO_SP.localize(eta_naive)
                eta_formatado = eta_datetime.strftime('%d/%m %H:%M')
                tempo_restante_str = formatar_tempo_restante(eta_datetime, agora)
            except ValueError:
                eta_formatado = f"{eta_str} (Formato?)"
                print(f"⚠️ Aviso: Formato de data/hora inválido para ETA: '{eta_str}'. Esperado: '{FORMATO_ETA}'")

        mensagens.append(f"🚛 {lt}")
        mensagens.append(f"{doca}")
        mensagens.append(f"Motorista: {motorista}")
        mensagens.append(f"Qntd de TO´s: {tos}")
        mensagens.append(f"ETA: {eta_formatado} {tempo_restante_str}") # Linha do ETA
        
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
        "text": { "format": 1, "content": mensagem_final }
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

    # Pega a hora atual aqui, UMA VEZ, com fuso horário
    agora = datetime.now(FUSO_HORARIO_SP)

    df_completo, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return
        
    if df_completo.empty:
        print("✅ Nenhuma LT na aba 'Reporte prioridade'. Nada enviado.")
        return

    # --- NOVO BLOCO DE FILTRO DE 10 HORAS ---
    df_filtrado_lista = []
    limite_em_horas = 10
    # Limite é a hora atual + 10 horas
    limite_alerta = agora + timedelta(hours=limite_em_horas) 

    print(f"🕣 Hora atual: {agora.strftime('%d/%m %H:%M')}. Filtrando LTs com ETA até {limite_alerta.strftime('%d/%m %H:%M')}.")

    for index, row in df_completo.iterrows():
        # ALTERADO: Usando 'Próximo ETA'
        eta_str = row['Próximo ETA'].strip() 
        if not eta_str:
            continue # Pula linhas sem ETA

        try:
            eta_naive = datetime.strptime(eta_str, FORMATO_ETA)
            eta_datetime = FUSO_HORARIO_SP.localize(eta_naive)
            
            # A CONDIÇÃO: Enviar se o ETA for HOJE/AGORA ou DENTRO das próximas 10h
            if eta_datetime <= limite_alerta:
                df_filtrado_lista.append(row)
            else:
                print(f"ℹ️ LT {row['LT']} ignorada. ETA ({eta_datetime.strftime('%H:%M')}) está fora da janela de {limite_em_horas}h.")

        except ValueError:
            print(f"⚠️ Aviso (Filtro): Formato de data/hora inválido para ETA: '{eta_str}'. Pulando linha {index}.")
    
    if not df_filtrado_lista:
        print(f"✅ Nenhuma LT encontrada com ETA dentro de {limite_em_horas} horas. Nada enviado.")
        return
    
    # Converte a lista de linhas filtradas de volta para um DataFrame
    df_filtrado = pd.DataFrame(df_filtrado_lista)
    # --- FIM DO BLOCO DE FILTRO ---


    # Passa o DataFrame JÁ FILTRADO para montar a mensagem
    mensagem = montar_mensagem_alerta(df_filtrado, agora) 

    if mensagem:
        turno_atual = identificar_turno_atual(agora) 
        ids_para_marcar = TURNO_PARA_IDS.get(turno_atual, [])

        print(f"🕒 Turno atual: {turno_atual} (Hora: {agora.strftime('%H:%M')})")
        print(f"👥 IDs configurados para este turno: {ids_para_marcar}")

        enviar_webhook_com_mencao_oficial(mensagem, webhook_url, user_ids=ids_para_marcar)
        enviar_imagem(webhook_url)
    
    # (O 'else' anterior foi removido pois o filtro já trata LTs vazias)


if __name__ == "__main__":
    main()
