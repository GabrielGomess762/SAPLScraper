import streamlit as st
import requests
import os
import pandas as pd
from datetime import datetime
import re
import logging
import locale
import zipfile  # Para criar o arquivo ZIP
from io import BytesIO  # Para manipular arquivos na memória

# Configuração do locale para português do Brasil
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')  # Linux/Mac
except locale.Error:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')  # Windows

# Configuração do log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("diarios_boavista.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# URL base da API
BASE_API_URL = "https://publicacoes.boavista.rr.gov.br/api/v1/diarios"
PDF_FOLDER = "Diarios_PDFs"

# Função para limpar nomes de arquivos
def sanitize_filename(filename):
    invalid_chars = r'[<>:"/\\|?*]'
    filename = re.sub(invalid_chars, '', filename)
    if len(filename) > 240:
        filename = filename[:240]
    return filename.strip()

# Função para download de PDF
def download_pdf(pdf_url, folder_path, diario_info):
    try:
        if pdf_url:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(pdf_url, headers=headers)
            if response.status_code == 200:
                filename = f"Diario_{diario_info['Edicao']}_{diario_info['Data']}.pdf"
                filename = sanitize_filename(filename)
                filepath = os.path.join(folder_path, filename)
                if not os.path.exists(filepath):
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    logging.info(f"PDF baixado: {filename}")
                else:
                    logging.info(f"PDF já existe: {filename}")
                return filepath
            else:
                logging.warning(f"Erro ao baixar PDF. Status code: {response.status_code}")
        else:
            logging.warning("URL do PDF não encontrada")
    except Exception as e:
        logging.error(f"Erro ao baixar PDF: {str(e)}")
    return None

# Função para buscar os diários em uma página específica da API
def fetch_diarios(page):
    try:
        response = requests.get(f"{BASE_API_URL}?page={page}")
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Erro ao acessar API na página {page}. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Erro ao acessar API na página {page}: {str(e)}")
    return None

# Função para converter datas com suporte a diferentes formatos
def converter_data(data_str):
    """
    Tenta converter uma string de data para um objeto datetime.
    Aceita diferentes formatos de data e registra erros se a conversão falhar.
    """
    formatos = [
        '%A, %d de %B de %Y',  # Com dia da semana
        '%d de %B de %Y'       # Sem dia da semana
    ]

    for formato in formatos:
        try:
            return datetime.strptime(data_str, formato)
        except ValueError:
            pass  # Tentar o próximo formato

    logging.warning(f"Erro ao converter data: {data_str}")
    return None

# Função para verificar se a data está no intervalo especificado
def is_date_in_range(date_str, start_date, end_date):
    try:
        date = converter_data(date_str)  # Usando a função de conversão de data
        if date:
            return start_date <= date <= end_date
    except Exception as e:
        logging.warning(f"Erro ao verificar intervalo de data: {date_str}. Erro: {str(e)}")
    return False

# Função para verificar se a edição está no intervalo especificado
def is_edition_in_range(edition, start_edition, end_edition):
    try:
        edition = int(edition)
        return start_edition <= edition <= end_edition
    except ValueError:
        logging.warning(f"Edição inválida: {edition}")
        return False

# Função principal para processar os diários
def process_diarios_by_filter(start_date=None, end_date=None, start_edition=None, end_edition=None):
    os.makedirs(PDF_FOLDER, exist_ok=True)
    diarios_data = []

    page = 1
    while True:
        data = fetch_diarios(page)
        if not data or "data" not in data:
            break

        for diario in data["data"]:
            edicao = diario.get("edicao", "")
            data_publicacao = diario.get("data", "")
            pdf_url = f"https://publicacoes.boavista.rr.gov.br{diario['media']['url']}" if diario.get("media") and diario["media"].get("url") else None
            paginas = diario.get("meta", {}).get("pages", "")
            tamanho = diario.get("meta", {}).get("size", "")

            if start_date and end_date:
                if not is_date_in_range(data_publicacao, start_date, end_date):
                    continue
            if start_edition and end_edition:
                if not is_edition_in_range(edicao, start_edition, end_edition):
                    continue

            diario_info = {
                "Edicao": edicao,
                "Data": data_publicacao,
                "Paginas": paginas,
                "Tamanho": tamanho,
                "PDF_URL": pdf_url
            }
            diarios_data.append(diario_info)

            if pdf_url:
                download_pdf(pdf_url, PDF_FOLDER, diario_info)

        if not data["links"]["next"]:
            break
        page += 1

    return diarios_data

# Função para criar um arquivo ZIP com os PDFs e a planilha
def create_zip_with_results(diarios_data):
    # Criar um arquivo ZIP na memória
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # Adicionar os PDFs ao ZIP
        for pdf_file in os.listdir(PDF_FOLDER):
            pdf_path = os.path.join(PDF_FOLDER, pdf_file)
            zip_file.write(pdf_path, os.path.relpath(pdf_path, PDF_FOLDER))

        # Criar uma planilha com os dados e adicioná-la ao ZIP
        df = pd.DataFrame(diarios_data)
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False, engine="openpyxl")
        excel_buffer.seek(0)
        zip_file.writestr("diarios_boavista.xlsx", excel_buffer.read())

    zip_buffer.seek(0)
    return zip_buffer

# Interface do Streamlit
def main():
    st.title("Diários Oficiais de Boa Vista - Download e Consulta")
    st.write("Use esta interface para baixar diários oficiais com base em intervalos de datas ou edições.")

    filtro = st.radio("Escolha o tipo de filtro:", ["Intervalo de Datas", "Intervalo de Edições"])

    if filtro == "Intervalo de Datas":
        start_date = st.date_input("Data inicial:")
        end_date = st.date_input("Data final:")

        if st.button("Buscar e Baixar Diários"):
            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.max.time())
            diarios = process_diarios_by_filter(start_date=start_date, end_date=end_date)
            st.success(f"Processamento concluído! {len(diarios)} diários encontrados.")
            st.write(diarios)

            # Gerar o arquivo ZIP para download
            zip_file = create_zip_with_results(diarios)
            st.download_button(
                label="Baixar Arquivo Compactado",
                data=zip_file,
                file_name="diarios_boavista.zip",
                mime="application/zip"
            )

    elif filtro == "Intervalo de Edições":
        start_edition = st.number_input("Edição inicial:", min_value=1, step=1)
        end_edition = st.number_input("Edição final:", min_value=1, step=1)

        if st.button("Buscar e Baixar Diários"):
            diarios = process_diarios_by_filter(start_edition=start_edition, end_edition=end_edition)
            st.success(f"Processamento concluído! {len(diarios)} diários encontrados.")
            st.write(diarios)

            # Gerar o arquivo ZIP para download
            zip_file = create_zip_with_results(diarios)
            st.download_button(
                label="Baixar Arquivo Compactado",
                data=zip_file,
                file_name="diarios_boavista.zip",
                mime="application/zip"
            )

    st.write("Os PDFs baixados estão sendo salvos na pasta `Diarios_PDFs`.")

if __name__ == "__main__":
    main()
