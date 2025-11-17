import json
import os
import re
import pandas as pd
from datetime import datetime, timedelta
import urllib.parse
from flask import Flask, request, render_template, redirect, url_for, send_file, session, flash
from werkzeug.utils import secure_filename
from io import BytesIO
import pickle 
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import xlsxwriter 

# --- Configuração do Flask e Banco de Dados ---
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024 
app.secret_key = 'sua_chave_secreta_muito_longa_e_aleatoria_para_o_render' 

# Configuração do Banco de Dados: Prioriza a conexão INTERNA do Render
db_url_external = os.environ.get('DATABASE_URL')
db_url_internal = os.environ.get('INTERNAL_DATABASE_URL')

if db_url_internal:
    # 1. Prioriza a URL interna para conexões estáveis (sem necessidade de sslmode)
    db_uri = db_url_internal.replace("postgres://", "postgresql://", 1)
elif db_url_external:
    # 2. Fallback para a URL externa com correção SSL
    db_uri = db_url_external.replace("postgres://", "postgresql://", 1)
    db_uri += "?sslmode=require"
else:
    # 3. Fallback para desenvolvimento local
    db_uri = 'sqlite:///database.db'

app.config['SQLALCHEMY_DATABASE_URI'] = db_uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(weeks=3) 

db = SQLAlchemy(app)

# Configuração de Login
login_manager = LoginManager(app)
login_manager.login_view = 'login' 
login_manager.login_message = "Por favor, faça login para acessar esta página."

# ----------------------------------------------------------------------
# 1. MODELOS DO BANCO DE DADOS
# ----------------------------------------------------------------------

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128))
    is_admin = db.Column(db.Boolean, default=False)
    is_approved = db.Column(db.Boolean, default=False) 
    
    files = db.relationship('HarFile', backref='owner', lazy='dynamic')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class HarFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), nullable=False)
    data_pickle = db.Column(db.LargeBinary, nullable=False) 
    
    timestamp = db.Column(db.DateTime, default=datetime.utcnow) 
    expiration_date = db.Column(db.DateTime, default=lambda: datetime.utcnow() + timedelta(weeks=3))
    
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False) 


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))


# ----------------------------------------------------------------------
# 2. LÓGICA DE PROCESSAMENTO HAR E EXCEL (Mantida)
# ----------------------------------------------------------------------

regex_url = re.compile(r"nomeprod=(?P<produto>.+?)&.*mesa=(?P<mesa>[^&]+).*quant=(?P<quant>\d+)", re.IGNORECASE)
regex_cadastro_mesa = re.compile(r"/connect\.php\?mesa=(?P<mesa>[^&]+)&id=", re.IGNORECASE)
regex_deletado = re.compile(r"delete=(?P<delete_id>\d+)", re.IGNORECASE)

def parse_nomeprod(produto_str):
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
    todos_lancamentos = []
    todas_mesas_cad = []
    todos_itens_deletados = []
    
    for file in files.values():
        try:
            if file and file.filename.endswith('.har'):
                file.seek(0) 
                content = file.read().decode('utf-8')
                
                lanc, cad, delet = process_har_file(content, secure_filename(file.filename))
                todos_lancamentos.extend(lanc)
                todas_mesas_cad.extend(cad)
                todos_itens_deletados.extend(delet)
            
        except Exception:
            continue

    if not todos_lancamentos and not todas_mesas_cad and not todos_itens_deletados:
        return None, None, None, None, None, None, None, None

    df = pd.DataFrame(todos_lancamentos)
    df_cad = pd.DataFrame(todas_mesas_cad)
    df_del = pd.DataFrame(todos_itens_deletados)
    
    FUSO_BRASILIA = 'America/Sao_Paulo'
    COLUNAS_LANCAMENTO = [
        "Nº", "response", "produto", "Qtde", "Data", "Hora", 
        "deletar", "valor unitario", "valor total", "mesa", "arquivo_origem", "request" 
    ]
    
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
        
        df_lancamentos_final = df.reindex(columns=COLUNAS_LANCAMENTO)
    else:
        df_lancamentos_final = pd.DataFrame(columns=COLUNAS_LANCAMENTO)
        
    df_lancamentos_excel = df.drop(columns=["horario_norm", "horario_br"], errors='ignore') if not df.empty else df

    if not df_cad.empty:
        df_cad["horario_cadastro"] = pd.to_datetime(df_cad["horario_cadastro"], errors="coerce", utc=True)
        df_cad["horario_cadastro"] = df_cad["horario_cadastro"].dt.tz_convert(FUSO_BRASILIA).dt.tz_localize(None)
        df_cad = df_cad.sort_values(by="horario_cadastro").drop_duplicates(subset=["mesa"], keep="first").reset_index(drop=True)
        df_cad_final = df_cad.drop(columns=["horario_cadastro"], errors='ignore')
    else:
        df_cad_final = pd.DataFrame(columns=["mesa", "request", "response", "arquivo_origem"])
        
    df_cad_excel = df_cad

    if not df_del.empty:
        total_deletado = df_del["valor total"].sum() if "valor total" in df_del.columns else 0
        df_del_final = df_del.drop(columns=["horario"], errors='ignore')
        df_del_excel = df_del.drop(columns=["mesa_del_ref"], errors='ignore')
    else:
        df_del_final = pd.DataFrame(columns=["delete_id", "mesa", "produto", "valor unitario", "Qtde", "valor total", "status", "request", "arquivo_origem"])
        df_del_excel = df_del_final
        total_deletado = 0

    total_valor = df["valor total"].sum() if not df.empty else 0
    comissao = total_valor * 0.06
    taxa = total_valor * 0.04
    
    dados_geral = pd.DataFrame({"Valor total": [total_valor], "Comissão 6%": [comissao], "Taxa 4%": [taxa]})
    
    dados_geral_html = dados_geral.copy()
    for col in dados_geral_html.columns:
        if "Valor" in col or "Comissão" in col or "Taxa" in col:
            dados_geral_html[col] = dados_geral_html[col].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    if not df.empty:
        df_ranking = df.groupby("mesa")["valor total"].sum().reset_index().sort_values(by="valor total", ascending=False)
        df_ranking["Posição"] = df_ranking.index + 1
        df_ranking_final = df_ranking[["Posição", "mesa", "valor total"]]
    else:
        df_ranking_final = pd.DataFrame(columns=["Posição", "mesa", "valor total"])

    df_ranking_html = df_ranking_final.copy()
    df_ranking_html["valor total"] = df_ranking_html["valor total"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    lanc_html = df_lancamentos_final.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format)
    cad_html = df_cad_final.to_html(classes='table table-striped table-sm', index=False)
    del_html = df_del_final.to_html(classes='table table-striped table-sm', index=False)
    geral_html = dados_geral_html.to_html(classes='table table-bordered table-sm', index=False)
    ranking_html = df_ranking_html.to_html(classes='table table-striped table-sm', index=False, float_format='R$ {:,.2f}'.format)

    return (lanc_html, cad_html, del_html, geral_html, ranking_html,
            df_lancamentos_excel, df_cad_excel, df_del_excel)


def generate_excel(df_lancamentos, df_cad, df_del):
    """Gera o arquivo Excel em memória (Requer xlsxwriter)"""
    output = BytesIO()
    
    try:
        import xlsxwriter
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            money_format = workbook.add_format({'num_format': 'R$ #,##0.00'})
            number_format = workbook.add_format({'num_format': '#,##0'}) 
            bold_format = workbook.add_format({'bold': True})
            
            # --- 1. Aba LANÇAMENTOS ---
            if not df_lancamentos.empty:
                df_lancamentos["Qtde"] = df_lancamentos["Qtde"].astype(int)
                df_to_excel = df_lancamentos.copy()
                df_to_excel["valor total"] = 0.0
                df_to_excel.to_excel(writer, sheet_name="LANÇAMENTOS", index=False, startrow=0, startcol=0)
                
                worksheet = writer.sheets["LANÇAMENTOS"]
                COLUNAS_LANCAMENTO = list(df_to_excel.columns)
                table_columns = [{"header": col} for col in COLUNAS_LANCAMENTO]
                total_col_index = COLUNAS_LANCAMENTO.index("valor total")
                
                table_columns[total_col_index] = {
                    'header': 'valor total',
                    'formula': '=IF([@deletar]="SIM", 0, [@Qtde]*[@[valor unitario]])' 
                }
                
                max_row = len(df_to_excel)
                max_col = len(COLUNAS_LANCAMENTO) - 1 
                
                worksheet.add_table(0, 0, max_row, max_col, {'columns': table_columns, 'name': 'TabelaLancamentos', 'style': 'TableStyleMedium9'})
                
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
                total_deletado = df_del["valor total"].sum() if "valor total" in df_del.columns else 0
                total_del_row = len(df_del) + 2
                
                worksheet_del = writer.sheets["ITENS_DELETADO"]
                worksheet_del.write_string(total_del_row, 0, "TOTAL DELETADO", bold_format)
                worksheet_del.write_number(total_del_row, 1, total_deletado, money_format)
                
            # --- 4. Aba GERAL ---
            worksheet_geral = workbook.add_worksheet("GERAL")
            resumo_colunas_final = ["Valor total", "Comissão 6%", "Taxa 4%"]
            
            worksheet_geral.write_row('A1', resumo_colunas_final, bold_format)
            
            worksheet_geral.write_formula('A2', '=SUM(TabelaLancamentos[valor total])', money_format)
            worksheet_geral.write_formula('B2', '=(A2*0.06)', money_format)
            worksheet_geral.write_formula('C2', '=A2*0.04', money_format)
            
            worksheet_geral.set_column('A:C', 15, money_format) 

            # --- 5. Aba RANKING ---
            if not df_lancamentos.empty:
                df_ranking = df_lancamentos.groupby("mesa")["valor total"].sum().reset_index().sort_values(by="valor total", ascending=False)
                df_ranking["Posição"] = df_ranking.index + 1
                df_ranking = df_ranking[["Posição", "mesa", "valor total"]]
                
                df_ranking.to_excel(writer, sheet_name="RANKING", index=False)
                
                worksheet_ranking = writer.sheets["RANKING"]
                worksheet_ranking.set_column(2, 2, 15, money_format)
        
        output.seek(0)
        return output

    except Exception as e:
        print(f"Erro fatal ao gerar Excel: {e}")
        return None 


# ----------------------------------------------------------------------
# 3. ROTAS DA APLICAÇÃO (Com Autenticação e Persistência de Dados)
# ----------------------------------------------------------------------

# Rota principal (Upload e Visualização)
@app.route('/', methods=['GET', 'POST'])
@login_required 
def upload_file():
    if not current_user.is_approved:
        flash('Sua conta ainda não foi aprovada pelo administrador.', 'warning')
        return redirect(url_for('logout'))

    if request.method == 'POST':
        files = request.files
        result = process_all_files(files)
        
        if result[0] is None:
             session.pop('current_file_id', None)
             flash('Nenhum arquivo .har válido encontrado ou dados vazios.', 'danger')
             return render_template('index.html')

        # SALVA OS DFs NO BANCO (Persistência)
        try:
            data_to_store = {
                'lancamentos': result[5], 
                'mesas_cad': result[6], 
                'itens_del': result[7]
            }
            data_pickle = pickle.dumps(data_to_store)

            new_file = HarFile(
                filename=f"Relatorio_HAR_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                data_pickle=data_pickle,
                user_id=current_user.id 
            )
            db.session.add(new_file)
            db.session.commit()
            
            session['current_file_id'] = new_file.id
            
        except Exception as e:
            flash(f"Erro ao salvar dados no banco de dados: {e}", 'danger')
            return redirect(url_for('upload_file'))


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

# ROTA DE DOWNLOAD (Gera o Excel a partir do BD)
@app.route('/download_excel', methods=['GET'])
@login_required
def download_excel():
    file_id = session.get('current_file_id')
    
    if not file_id:
        flash("Nenhum arquivo recente encontrado para exportar. Por favor, faça um novo upload.", 'danger')
        return redirect(url_for('upload_file'))

    har_file = HarFile.query.filter_by(id=file_id, user_id=current_user.id).first()
    
    if not har_file:
        flash("Arquivo não encontrado, expirado ou acesso negado.", 'danger')
        session.pop('current_file_id', None) 
        return redirect(url_for('upload_file'))

    try:
        dados = pickle.loads(har_file.data_pickle)
        
        df_lancamentos = dados['lancamentos']
        df_cad = dados['mesas_cad']
        df_del = dados['itens_del']
        
    except Exception as e:
        print(f"Erro na desserialização do pickle: {e}")
        flash("Erro interno ao recuperar dados persistidos.", 'danger')
        return redirect(url_for('upload_file'))

    excel_file = generate_excel(df_lancamentos, df_cad, df_del)
    
    if excel_file:
        session.pop('current_file_id', None) 
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=har_file.filename
        )
    else:
        flash("Erro interno ao gerar o arquivo Excel.", 'danger')
        return redirect(url_for('upload_file'))


# --- ROTAS DE AUTENTICAÇÃO E GERENCIAMENTO (Mantidas) ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('upload_file'))
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            if not user.is_approved:
                flash('Sua conta aguarda aprovação do administrador.', 'warning')
                return redirect(url_for('login'))
            
            login_user(user, remember=True)
            return redirect(url_for('upload_file'))
        else:
            flash('Login inválido. Verifique o nome de usuário e senha.', 'danger')

    return render_template('login.html') 

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('upload_file'))
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if User.query.filter_by(username=username).first():
            flash('Nome de usuário já existe.', 'danger')
        elif not password:
            flash('A senha não pode ser vazia.', 'danger')
        else:
            new_user = User(username=username)
            new_user.set_password(password)
            new_user.is_approved = False
            
            db.session.add(new_user)
            db.session.commit()
            flash('Sua conta foi criada e aguarda aprovação do administrador.', 'success')
            return redirect(url_for('login'))

    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Você foi desconectado.', 'success')
    return redirect(url_for('login'))

@app.route('/admin/users', methods=['GET', 'POST'])
@login_required
def manage_users():
    if not current_user.is_admin:
        flash('Acesso Negado.', 'danger')
        return redirect(url_for('upload_file'))

    if request.method == 'POST':
        user_id = request.form.get('user_id', type=int)
        action = request.form.get('action')
        user = db.session.get(User, user_id)
        
        if user and user.id != current_user.id:
            if action == 'accept':
                user.is_approved = True
                db.session.commit()
                flash(f'Usuário {user.username} aprovado.', 'success')
            elif action == 'delete':
                HarFile.query.filter_by(user_id=user.id).delete()
                db.session.delete(user)
                db.session.commit()
                flash(f'Usuário {user.username} deletado e arquivos removidos.', 'success')
            elif action == 'reject':
                user.is_approved = False
                db.session.commit()
                flash(f'Usuário {user.username} rejeitado e movido para pendente.', 'warning')
                
        return redirect(url_for('manage_users'))

    users_pending = User.query.filter_by(is_approved=False).order_by(User.username).all()
    users_active = User.query.filter_by(is_approved=True).order_by(User.username).all()

    expired_files = HarFile.query.filter(HarFile.expiration_date < datetime.utcnow()).delete()
    db.session.commit()
    
    return render_template('manage_users.html', 
                           pending=users_pending, 
                           active=users_active,
                           expired_count=expired_files)


if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')

    with app.app_context():
        db.create_all()
        
        if not User.query.filter_by(is_admin=True).first():
            admin_user = User(username='admin', is_admin=True, is_approved=True)
            admin_user.set_password('SuaSenhaAdminSecreta123!') 
            db.session.add(admin_user)
            db.session.commit()
            print(">>> Usuário 'admin' criado com a senha: SuaSenhaAdminSecreta123! <<<")
    
    app.run(host='0.0.0.0', port=5000)