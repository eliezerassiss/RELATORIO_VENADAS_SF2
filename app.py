import json
import os
import re
import pandas as pd
from datetime import datetime
import urllib.parse
from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
from io import StringIO
import tempfile
import shutil

# --- Configuração do Flask ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir() # Usa a pasta temporária do sistema
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Limite de 16MB para upload

# ----------------------------------------------------------------------
# Regex e Funções de Apoio (Mantidas do BOT2.py)
# ----------------------------------------------------------------------
regex_url = re.compile(
    r"nomeprod=(?P<produto>.+?)&.*mesa=(?P<mesa>[^&]+).*quant=(?P<quant>\d+)", re.IGNORECASE
)
regex_cadastro_mesa = re.compile(
    r"/connect\.php\?mesa=(?P<mesa>[^&]+)&id=", re.IGNORECASE
)
regex_deletado = re.compile(r"delete=(?P<delete_id>\d+)", re.IGNORECASE)

def parse_nomeprod(produto_str):
    """Extrai nome e valor unitário da string nomeprod"""
    try:
        produto_dec = urllib.parse.unquote_plus(produto_str)
        if "R$" in produto_dec:
            partes = produto_dec.split("R$")
            nome = partes[0].strip()
            valor_unit = float(partes[1].replace(".", "").replace(",", ".").replace(" ", ""))
        else:
            nome = produto_dec.strip()
            valor_unit = 0.0
        return nome, valor_unit
    except Exception:
        return produto_str, 0.0

def process_har_file(file_content, file_name):
    """Processa o conteúdo de um único arquivo HAR (string JSON)"""
    try:
        har_data = json.loads(file_content)
    except Exception:
        return [], [], []

    lancamentos = []
    mesas_cadastradas_raw = []
    itens_deletados = []

    for entry in har_data["log"]["entries"]:
        try:
            url = entry["request"]["url"]
            method = entry["request"].get("method", "")
            response_status = entry["response"]["status"]
            started_date_time = entry.get("startedDateTime", "")
            headers = {h["name"].lower(): h["value"] for h in entry["request"].get("headers", [])}
            post_data = entry["request"].get("postData", {}).get("text", "")
            response_body = entry["response"].get("content", {}).get("text", "")

            horario = ""
            if started_date_time:
                try:
                    horario = datetime.fromisoformat(started_date_time.replace("Z", "+00:00"))
                except:
                    horario = started_date_time

            # 1. Captura Lançamento de Produto
            match_lancamento = regex_url.search(url)
            if match_lancamento:
                produto_raw = match_lancamento.group("produto")
                mesa = match_lancamento.group("mesa")
                quant = int(match_lancamento.group("quant"))

                produto, valor_unit = parse_nomeprod(produto_raw)
                valor_total = quant * valor_unit

                lancamento_id = None
                if response_body:
                    response_clean = response_body.strip()
                    if response_clean.isdigit():
                        lancamento_id = response_clean
                
                lancamentos.append({
                    "request": url, "response": response_status, "produto": produto,
                    "Qtde": quant, "horario": horario, "valor unitario": valor_unit,
                    "valor total": valor_total, "mesa": mesa, "arquivo_origem": file_name,
                    "lancamento_id": lancamento_id
                })
                continue

            # 2. Captura Cadastro de Mesa
            match_cadastro = regex_cadastro_mesa.search(url)
            if match_cadastro and response_status == 200:
                mesa_nome = match_cadastro.group("mesa")
                mesa_nome_dec = urllib.parse.unquote_plus(mesa_nome).strip()
                mesas_cadastradas_raw.append({
                    "mesa": mesa_nome_dec, "horario_cadastro": horario, "request": url,
                    "response": response_status, "arquivo_origem": file_name
                })
                continue

            # 3. Captura Itens Deletados
            if "/inc/del_produtos.php" in url and method.upper() == "POST":
                match_del = regex_deletado.search(post_data)
                if match_del:
                    delete_id = match_del.group("delete_id")
                    mesa = "" 
                    if "referer" in headers and "mesa=" in headers["referer"]:
                        mesa_raw = re.search(r"mesa=([^&]+)", headers["referer"])
                        if mesa_raw:
                            mesa = urllib.parse.unquote_plus(mesa_raw.group(1))

                    itens_deletados.append({
                        "delete_id": delete_id, "mesa_del_ref": mesa, "horario": horario,
                        "status": response_status, "request": url, "arquivo_origem": file_name
                    })
        except Exception:
            continue

    return lancamentos, mesas_cadastradas_raw, itens_deletados


def process_all_files(files):
    """Função principal de processamento de múltiplos arquivos e consolidação"""
    todos_lancamentos = []
    todas_mesas_cad = []
    todos_itens_deletados = []
    
    # Processa todos os arquivos carregados
    for file in files.values():
        if file and file.filename.endswith('.har'):
            file_content = file.read().decode('utf-8')
            lanc, cad, delet = process_har_file(file_content, secure_filename(file.filename))
            todos_lancamentos.extend(lanc)
            todas_mesas_cad.extend(cad)
            todos_itens_deletados.extend(delet)

    if not todos_lancamentos and not todas_mesas_cad and not todos_itens_deletados:
        return None, None, None, None, None

    df = pd.DataFrame(todos_lancamentos)
    df_cad = pd.DataFrame(todas_mesas_cad)
    df_del = pd.DataFrame(todos_itens_deletados)
    
    FUSO_BRASILIA = 'America/Sao_Paulo'
    COLUNAS_LANCAMENTO = [
        "Nº", "request", "response", "produto", "Qtde", "Data", "Hora", 
        "deletar", "valor unitario", "valor total", "mesa", "arquivo_origem"
    ]
    
    # --- 1. Processamento e Normalização (similar ao BOT2.py) ---
    if not df.empty:
        df["horario"] = pd.to_datetime(df["horario"], errors="coerce", utc=True)
        df["horario_br"] = df["horario"].dt.tz_convert(FUSO_BRASILIA)
        df["Data"] = df["horario_br"].dt.strftime('%Y-%m-%d')
        df["Hora"] = df["horario_br"].dt.strftime('%H:%M:%S')
        df["horario_norm"] = df["horario_br"].dt.tz_localize(None).dt.floor("s")
        df = df.drop_duplicates(subset=["mesa", "produto", "Qtde", "request", "horario_norm"]).reset_index(drop=True)
        df["Nº"] = df.index + 1
        df["deletar"] = "" 
        df["Qtde"] = df["Qtde"].astype(int)
        
        # Recálculo do valor total estático para RANKING/GERAL (para fins de exibição se o Excel não calcular)
        # Na exibição HTML, este valor estático será usado.
        df["valor total"] = df["Qtde"] * df["valor unitario"]
        
        # Reordenamento e limpeza de colunas
        df_lancamentos_final = df.reindex(columns=COLUNAS_LANCAMENTO)

    else:
        df_lancamentos_final = pd.DataFrame(columns=COLUNAS_LANCAMENTO)


    if not df_cad.empty:
        df_cad["horario_cadastro"] = pd.to_datetime(df_cad["horario_cadastro"], errors="coerce", utc=True)
        df_cad["horario_cadastro"] = df_cad["horario_cadastro"].dt.tz_convert(FUSO_BRASILIA).dt.tz_localize(None)
        df_cad = df_cad.sort_values(by="horario_cadastro").drop_duplicates(subset=["mesa"], keep="first").reset_index(drop=True)
        df_cad_final = df_cad.drop(columns=["horario_cadastro"], errors='ignore')
    else:
        df_cad_final = pd.DataFrame(columns=["mesa", "request", "response", "arquivo_origem"])


    # --- 2. Itens Deletados ---
    if not df_del.empty:
        # Simplificação para exibição na web: a lógica de merge e total deletado não é crítica aqui,
        # mas o total_deletado é necessário para o GERAL.
        total_deletado = df_del["valor total"].sum() if "valor total" in df_del.columns else 0
        df_del_final = df_del.drop(columns=["horario"], errors='ignore')
    else:
        df_del_final = pd.DataFrame(columns=["delete_id", "mesa", "produto", "valor unitario", "Qtde", "valor total", "status", "request", "arquivo_origem"])
        total_deletado = 0


    # --- 3. Aba GERAL (Simplificada com as novas fórmulas) ---
    total_valor = df["valor total"].sum() if not df.empty else 0
    
    # FÓRMULAS VINDAS DO USUÁRIO
    # Comissão 6% =(B2*0,06)+140
    comissao = (total_valor * 0.06) + 140
    # Taxa 4% =B2*0,04
    taxa = total_valor * 0.04
    
    dados_geral = pd.DataFrame({
        "Valor total": [total_valor],
        "Comissão 6%": [comissao],
        "Taxa 4%": [taxa]
    })
    
    # Formatação para o HTML (simulando R$)
    for col in dados_geral.columns:
        if "Valor" in col or "Comissão" in col or "Taxa" in col:
            dados_geral[col] = dados_geral[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))


    # --- 4. Aba RANKING ---
    if not df.empty:
        df_ranking = df.groupby("mesa")["valor total"].sum().reset_index().sort_values(by="valor total", ascending=False)
        df_ranking["Posição"] = df_ranking.index + 1
        df_ranking_final = df_ranking[["Posição", "mesa", "valor total"]]
        df_ranking_final["valor total"] = df_ranking_final["valor total"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    else:
        df_ranking_final = pd.DataFrame(columns=["Posição", "mesa", "valor total"])

    # Converte os DataFrames para HTML para exibição
    return (df_lancamentos_final.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format),
            df_cad_final.to_html(classes='table table-striped table-sm', index=False),
            df_del_final.to_html(classes='table table-striped table-sm', index=False),
            dados_geral.to_html(classes='table table-bordered table-sm', index=False),
            df_ranking_final.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format))


# ----------------------------------------------------------------------
# Rotas do Flask
# ----------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Verifica se foi enviado algum arquivo
        if 'har_files' not in request.files:
            return redirect(request.url)
        
        # request.files é um MultiDict, podemos processar todos os arquivos
        files = request.files
        
        # Processa os arquivos
        lanc_html, cad_html, del_html, geral_html, ranking_html = process_all_files(files)
        
        if lanc_html is None:
             return render_template('index.html', error_message="Nenhum arquivo .har válido encontrado ou dados vazios.")

        # Renderiza o relatório
        return render_template(
            'relatorio.html',
            lancamentos=lanc_html,
            mesas_cad=cad_html,
            itens_del=del_html,
            geral=geral_html,
            ranking=ranking_html
        )
        
    # GET request: mostra o formulário de upload
    return render_template('index.html')

if __name__ == '__main__':
    # Cria a pasta de templates se não existir
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    # Se você for rodar localmente, use:
    # app.run(debug=True)
    # Se for hospedar, use:
    app.run(host='0.0.0.0', port=5000)