import json
import os
import re
import pandas as pd
from datetime import datetime
import urllib.parse
from flask import Flask, request, render_template, redirect, url_for, send_file
from werkzeug.utils import secure_filename
from io import BytesIO
import tempfile
import shutil

# --- Configuração do Flask ---
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 

# ----------------------------------------------------------------------
# Regex e Funções de Apoio (Assegurando a mesma lógica do BOT2.py)
# ----------------------------------------------------------------------
regex_url = re.compile(
    r"nomeprod=(?P<produto>.+?)&.*mesa=(?P<mesa>[^&]+).*quant=(?P<quant>\d+)", re.IGNORECASE
)
regex_cadastro_mesa = re.compile(
    r"/connect\.php\?mesa=(?P<mesa>[^&]+)&id=", re.IGNORECASE
)
regex_deletado = re.compile(r"delete=(?P<delete_id>\d+)", re.IGNORECASE)

# --- Funções Auxiliares ---

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
    """Processa o conteúdo de um único arquivo HAR"""
    try:
        har_data = json.loads(file_content)
    except Exception:
        return [], [], []

    lancamentos = []
    mesas_cadastradas_raw = []
    itens_deletados = []
    # ... (O restante da função process_har_file é mantido idêntico) ...
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


# Variável global para armazenar os DataFrames após o processamento (necessário para o download)
# Em ambiente de produção real (Gunicorn), isso pode causar problemas de concorrência se muitos usuários acessarem simultaneamente.
# Em um ambiente real, deve-se usar um banco de dados ou sessão, mas para a simplicidade do Flask, usaremos variáveis globais.
PROCESSED_DATA = {} 

def process_all_files(files):
    """Função principal de processamento de múltiplos arquivos e consolidação"""
    # ... (Corpo da função process_all_files é mantido idêntico) ...
    todos_lancamentos = []
    todas_mesas_cad = []
    todos_itens_deletados = []
    
    # Processa todos os arquivos carregados com tratamento de erro
    for file in files.values():
        try:
            if file and file.filename.endswith('.har'):
                # Resetar o ponteiro do arquivo antes de ler, para garantir que o Flask possa ler o conteúdo
                file.seek(0) 
                file_content = file.read().decode('utf-8')
                
                lanc, cad, delet = process_har_file(file_content, secure_filename(file.filename))
                todos_lancamentos.extend(lanc)
                todas_mesas_cad.extend(cad)
                todos_itens_deletados.extend(delet)
            
        except UnicodeDecodeError:
            print(f"ERRO: Não foi possível decodificar o arquivo {file.filename}. Pulando.")
            continue
        except Exception as e:
            print(f"ERRO DESCONHECIDO ao processar {file.filename}: {e}. Pulando.")
            continue

    if not todos_lancamentos and not todas_mesas_cad and not todos_itens_deletados:
        return None, None, None, None, None, None, None, None

    df = pd.DataFrame(todos_lancamentos)
    df_cad = pd.DataFrame(todas_mesas_cad)
    df_del = pd.DataFrame(todos_itens_deletados)
    
    FUSO_BRASILIA = 'America/Sao_Paulo'
    
    # NOVA ORDEM: 'request' movida para o final
    COLUNAS_LANCAMENTO = [
        "Nº", "response", "produto", "Qtde", "Data", "Hora", 
        "deletar", "valor unitario", "valor total", "mesa", "arquivo_origem", "request" 
    ]
    
    # --- 1. Processamento e Normalização ---
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
        df["valor total"] = df["Qtde"] * df["valor unitario"]
        
        # DataFrame de Lançamentos FINAL (para Exportação e HTML)
        df_lancamentos_final = df.reindex(columns=COLUNAS_LANCAMENTO)

    else:
        df_lancamentos_final = pd.DataFrame(columns=COLUNAS_LANCAMENTO)
        
    # Salva o DF principal no formato que o Excel espera (com o horário)
    df_lancamentos_excel = df.drop(columns=["horario_norm", "horario_br"], errors='ignore') if not df.empty else df

    if not df_cad.empty:
        df_cad["horario_cadastro"] = pd.to_datetime(df_cad["horario_cadastro"], errors="coerce", utc=True)
        df_cad["horario_cadastro"] = df_cad["horario_cadastro"].dt.tz_convert(FUSO_BRASILIA).dt.tz_localize(None)
        df_cad = df_cad.sort_values(by="horario_cadastro").drop_duplicates(subset=["mesa"], keep="first").reset_index(drop=True)
        df_cad_final = df_cad.drop(columns=["horario_cadastro"], errors='ignore')
    else:
        df_cad_final = pd.DataFrame(columns=["mesa", "request", "response", "arquivo_origem"])
        
    df_cad_excel = df_cad # Para o Excel, usa-se o DF com o horário de cadastro

    # --- 2. Itens Deletados ---
    if not df_del.empty:
        total_deletado = df_del["valor total"].sum() if "valor total" in df_del.columns else 0
        df_del_final = df_del.drop(columns=["horario"], errors='ignore')
        df_del_excel = df_del.drop(columns=["mesa_del_ref"], errors='ignore')
    else:
        df_del_final = pd.DataFrame(columns=["delete_id", "mesa", "produto", "valor unitario", "Qtde", "valor total", "status", "request", "arquivo_origem"])
        df_del_excel = df_del_final
        total_deletado = 0


    # --- 3. Aba GERAL ---
    total_valor = df["valor total"].sum() if not df.empty else 0
    comissao = total_valor * 0.06
    taxa = total_valor * 0.04
    
    dados_geral = pd.DataFrame({
        "Valor total": [total_valor],
        "Comissão 6%": [comissao],
        "Taxa 4%": [taxa]
    })
    
    # Prepara o DataFrame GERAL para HTML
    dados_geral_html = dados_geral.copy()
    for col in dados_geral_html.columns:
        if "Valor" in col or "Comissão" in col or "Taxa" in col:
            dados_geral_html[col] = dados_geral_html[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))


    # --- 4. Aba RANKING ---
    if not df.empty:
        df_ranking = df.groupby("mesa")["valor total"].sum().reset_index().sort_values(by="valor total", ascending=False)
        df_ranking["Posição"] = df_ranking.index + 1
        df_ranking_final = df_ranking[["Posição", "mesa", "valor total"]]
    else:
        df_ranking_final = pd.DataFrame(columns=["Posição", "mesa", "valor total"])

    # Prepara o DataFrame RANKING para HTML
    df_ranking_html = df_ranking_final.copy()
    df_ranking_html["valor total"] = df_ranking_html["valor total"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    # Converte os DataFrames para HTML para exibição
    lanc_html = df_lancamentos_final.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format)
    cad_html = df_cad_final.to_html(classes='table table-striped table-sm', index=False)
    del_html = df_del_final.to_html(classes='table table-striped table-sm', index=False)
    geral_html = dados_geral_html.to_html(classes='table table-bordered table-sm', index=False)
    ranking_html = df_ranking_html.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format)

    # Retorna o HTML para a interface e os DFs puros para o download
    return (lanc_html, cad_html, del_html, geral_html, ranking_html,
            df_lancamentos_excel, df_cad_excel, df_del_excel)


def generate_excel(df_lancamentos, df_cad, df_del):
    """Gera o arquivo Excel em memória com as abas e formatação do BOT2.py"""
    output = BytesIO()
    
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formatos
            money_format = workbook.add_format({'num_format': 'R$ #,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'}) 
            bold_format = workbook.add_format({'bold': True})
            
            # --- 1. Aba LANÇAMENTOS (Com Tabela e Fórmula) ---
            if not df_lancamentos.empty:
                df_lancamentos["Qtde"] = df_lancamentos["Qtde"].astype(int)
                
                # Prepara o DF sem o valor total estático para que a fórmula o sobrescreva
                df_to_excel = df_lancamentos.copy()
                df_to_excel["valor total"] = 0.0 # Zera para a fórmula sobrescrever
                
                df_to_excel.to_excel(writer, sheet_name="LANÇAMENTOS", index=False, startrow=0, startcol=0)
                
                worksheet = writer.sheets["LANÇAMENTOS"]
                
                # Define as colunas para a Tabela e a Fórmula
                COLUNAS_LANCAMENTO = list(df_to_excel.columns)
                table_columns = [{"header": col} for col in COLUNAS_LANCAMENTO]
                total_col_index = COLUNAS_LANCAMENTO.index("valor total")
                
                # FÓRMULA: =SI(H2="SIM";0;(E2*I2))
                table_columns[total_col_index] = {
                    'header': 'valor total',
                    'formula': '=IF([@deletar]="SIM", 0, [@Qtde]*[@[valor unitario]])' 
                }
                
                max_row = len(df_to_excel)
                max_col = len(COLUNAS_LANCAMENTO) - 1 
                
                worksheet.add_table(
                    0, 0, max_row, max_col, 
                    {
                        'columns': table_columns,
                        'name': 'TabelaLancamentos',
                        'style': 'TableStyleMedium9'
                    }
                )
                
                # Aplica formatação
                col_valor_unit_idx = COLUNAS_LANCAMENTO.index("valor unitario")
                col_valor_total_idx = COLUNAS_LANCAMENTO.index("valor total")
                col_qtde_idx = COLUNAS_LANCAMENTO.index("Qtde")
                
                worksheet.set_column(col_valor_unit_idx, col_valor_unit_idx, 15, money_format) 
                worksheet.set_column(col_valor_total_idx, col_valor_total_idx, 15, money_format) 
                worksheet.set_column(col_qtde_idx, col_qtde_idx, 10, number_format)
                
            # --- 2. Aba MESAS_CAD ---
            if not df_cad.empty:
                df_cad.to_excel(writer, sheet_name="MESAS_CAD", index=False)
                
            # --- 3. Aba ITENS_DELETADO ---
            if not df_del.empty:
                df_del.to_excel(writer, sheet_name="ITENS_DELETADO", index=False)
                
                # Adiciona o Total Deletado estático abaixo da tabela
                total_deletado = df_del["valor total"].sum() if "valor total" in df_del.columns else 0
                total_del_row = len(df_del) + 2
                
                worksheet_del = writer.sheets["ITENS_DELETADO"]
                worksheet_del.write_string(total_del_row, 0, "TOTAL DELETADO", bold_format)
                worksheet_del.write_number(total_del_row, 1, total_deletado, money_format)
                
                # Determina a célula do total para o GERAL
                TOTAL_DEL_CELL = f'B{total_del_row + 1}'
                
            else:
                TOTAL_DEL_CELL = 'B2' # Se vazio, o total estático é escrito em B2
            
            # --- 4. Aba GERAL (Apenas Fórmulas Dinâmicas) ---
            worksheet_geral = workbook.add_worksheet("GERAL")
            resumo_colunas_final = ["Valor total", "Comissão 6%", "Taxa 4%"]
            
            # Escreve o cabeçalho na linha 1 (índice 0)
            worksheet_geral.write_row('A1', resumo_colunas_final, bold_format)
            
            # FÓRMULA A2 (Valor total) =SUM(TabelaLancamentos[valor total])
            worksheet_geral.write_formula('A2', '=SUM(TabelaLancamentos[valor total])', money_format)
            
            # FÓRMULA B2 (Comissão 6%) =(A2*0,06)
            worksheet_geral.write_formula('B2', '=(A2*0.06)', money_format)
            
            # FÓRMULA C2 (Taxa 4%) =A2*0,04
            worksheet_geral.write_formula('C2', '=A2*0.04', money_format)
            
            worksheet_geral.set_column('A:C', 15, money_format) 

            # --- 5. Aba RANKING ---
            if not df_lancamentos.empty:
                df_ranking = df_lancamentos.groupby("mesa")["valor total"].sum().reset_index().sort_values(by="valor total", ascending=False)
                df_ranking["Posição"] = df_ranking.index + 1
                df_ranking = df_ranking[["Posição", "mesa", "valor total"]]
                
                df_ranking.to_excel(writer, sheet_name="RANKING", index=False)
                
                # Aplica formatação
                worksheet_ranking = writer.sheets["RANKING"]
                worksheet_ranking.set_column(2, 2, 15, money_format)

    except Exception as e:
        print(f"Erro ao gerar Excel: {e}")
        return None # Retorna None em caso de falha

    output.seek(0)
    return output


# ----------------------------------------------------------------------
# Rotas do Flask
# ----------------------------------------------------------------------

# Rota principal (Upload e Visualização)
@app.route('/', methods=['GET', 'POST'])
def upload_file():
    global PROCESSED_DATA
    if request.method == 'POST':
        files = request.files
        
        # Processa os arquivos e recebe o HTML e os DataFrames puros
        result = process_all_files(files)
        
        if result[0] is None:
             return render_template('index.html', error_message="Nenhum arquivo .har válido encontrado ou dados vazios.")

        # Armazena os DataFrames puros na variável global
        # Em produção, usaria Sessions ou cache
        PROCESSED_DATA = {
            'lancamentos': result[5], 
            'mesas_cad': result[6], 
            'itens_del': result[7]
        }
        
        # Retorna o HTML para visualização
        return render_template(
            'relatorio.html',
            lancamentos=result[0],
            mesas_cad=result[1],
            itens_del=result[2],
            geral=result[3],
            ranking=result[4]
        )
        
    return render_template('index.html')

# NOVA ROTA DE DOWNLOAD
@app.route('/download_excel', methods=['GET'])
def download_excel():
    global PROCESSED_DATA
    
    if not PROCESSED_DATA or PROCESSED_DATA.get('lancamentos').empty:
        return "Nenhum dado processado encontrado para exportar.", 404

    df_lancamentos = PROCESSED_DATA['lancamentos']
    df_cad = PROCESSED_DATA['mesas_cad']
    df_del = PROCESSED_DATA['itens_del']
    
    excel_file = generate_excel(df_lancamentos, df_cad, df_del)
    
    if excel_file:
        nome_arquivo = f"Relatorio_HAR_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=nome_arquivo
        )
    else:
        return "Erro interno ao gerar o arquivo Excel.", 500


if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')
    # Este modo é ideal para o Render
    app.run(host='0.0.0.0', port=5000)