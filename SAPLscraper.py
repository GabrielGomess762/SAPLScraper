import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm  # Para barra de progresso
import logging
import json

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", mode='a', encoding='utf-8'),  # Salvar logs em um arquivo
        logging.StreamHandler()  # Exibir logs no console
    ]
)

# Arquivo para salvar o último valor de "fim"
CONFIG_FILE = "last_execution.json"


def save_last_execution(fim):
    """
    Salva o maior valor de 'fim' no arquivo de configuração.
    """
    with open(CONFIG_FILE, 'w') as f:
        json.dump({"last_fim": fim}, f)
    logging.info(f"Último valor de 'fim' salvo: {fim}")


def load_last_execution():
    """
    Carrega o maior valor de 'fim' do arquivo de configuração.
    Retorna None se o arquivo não existir.
    """
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            return data.get("last_fim", None)
    return None


def generate_urls(inicio, fim):
    base_url = "https://sapl.boavista.rr.leg.br/norma/"
    step = 1 if inicio < fim else -1
    fim = fim + 1 if inicio < fim else fim - 1
    urls = [f"{base_url}{i}" for i in range(inicio, fim, step)]
    return urls


def extract_info(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return None  # URL inválida ou não encontrada

        soup = BeautifulSoup(response.text, 'html.parser')

        titulo_completo = soup.find('h1', class_='page-header').text.strip() if soup.find('h1', class_='page-header') else ""

        def get_field_value(field_id):
            element = soup.find('div', id=field_id)
            if element:
                value_div = element.find('div', class_='form-control-static')
                if value_div:
                    return value_div.text.strip()
            return ""

        tipo = get_field_value('div_id_tipo')
        numero = get_field_value('div_id_numero')
        ano = get_field_value('div_id_ano')
        data = get_field_value('div_id_data')

        ementa_element = soup.find('div', id='div_id_ementa')
        ementa = ""
        if ementa_element:
            dont_break = ementa_element.find('div', class_='dont-break-out')
            if dont_break:
                ementa = dont_break.text.strip()

        assuntos_element = soup.find('div', id='div_id_assuntos')
        temas = []
        if assuntos_element:
            items = assuntos_element.find_all('li')
            temas = [item.text.strip() for item in items]

        pdf_link = ""
        texto_integral = soup.find('div', id='div_id_texto_integral')
        if texto_integral:
            pdf_element = texto_integral.find('a')
            if pdf_element and pdf_element.has_attr('href'):
                pdf_link = pdf_element['href']
                if pdf_link.startswith('/'):
                    pdf_link = f"https://sapl.boavista.rr.leg.br{pdf_link}"

        data_publicacao = get_field_value('div_id_data_publicacao')
        veiculo_publicacao = get_field_value('div_id_veiculo_publicacao')

        return {
            'URL': url,
            'Tipo_Completo': titulo_completo,
            'Tipo': tipo,
            'Numero': numero,
            'Ano': ano,
            'Data': data,
            'Ementa': ementa,
            'Temas': '; '.join(temas) if temas else '',
            'PDF': pdf_link,
            'Data_Publicacao': data_publicacao,
            'Veiculo_Publicacao': veiculo_publicacao
        }

    except Exception as e:
        logging.error(f"Erro ao processar {url}: {str(e)}")
        return None


def sanitize_filename(filename):
    invalid_chars = r'[<>:"/\\|?*]'
    filename = re.sub(invalid_chars, '', filename)
    if len(filename) > 240:
        filename = filename[:240]
    return filename.strip()


def download_pdf(pdf_url, folder_path, info):
    try:
        if pdf_url:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(pdf_url, headers=headers)
            if response.status_code == 200:
                ementa_curta = info['Ementa'][:100] if len(info['Ementa']) > 100 else info['Ementa']
                filename = f"{info['Tipo']} {info['Numero']} {info['Ano']} - {ementa_curta}"
                filename = sanitize_filename(filename) + '.pdf'

                filepath = os.path.join(folder_path, filename)
                if not os.path.exists(filepath):  # Verificar duplicidade
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


def find_max_fim(start):
    """
    Pesquisa incremental para encontrar o maior valor de 'fim' válido no portal.
    """
    current = start
    while True:
        url = f"https://sapl.boavista.rr.leg.br/norma/{current}"
        response = requests.get(url)
        if response.status_code != 200:
            logging.info(f"Fim máximo encontrado: {current - 1}")
            return current - 1
        current += 1


def process_url(url, pdf_folder):
    """
    Processa uma URL: extrai informações e tenta baixar o PDF.
    """
    info = extract_info(url)
    if info:
        if info['PDF']:
            pdf_path = download_pdf(info['PDF'], pdf_folder, info)
            info['PDF_Local'] = pdf_path
        return info
    return None


def main():
    # Perguntar ao usuário se deseja executar no modo rápido
    modo_rapido = input("Deseja executar no modo rápido? (s/n): ").strip().lower() == 's'

    if modo_rapido:
        last_fim = load_last_execution()
        if last_fim is None:
            logging.error("Nenhum valor de 'fim' anterior encontrado. Execute o programa normalmente primeiro.")
            return
        inicio = last_fim + 1
        fim = find_max_fim(inicio)
    else:
        try:
            inicio = int(input("Digite o número inicial da norma: "))
            fim = int(input("Digite o número final da norma: "))
        except ValueError:
            logging.error("Por favor, insira números válidos.")
            return

    pdf_folder = f'Leis {inicio} - {fim}'
    os.makedirs(pdf_folder, exist_ok=True)

    urls = generate_urls(inicio, fim)
    dados = []

    # Barra de progresso com ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(tqdm(executor.map(lambda url: process_url(url, pdf_folder), urls), total=len(urls), desc="Processando URLs"))

    # Filtrar resultados válidos
    dados = [info for info in results if info is not None]

    # Salvar em Excel
    excel_path = os.path.join(pdf_folder, 'normas_juridicas.xlsx')
    df = pd.DataFrame(dados)
    df.to_excel(excel_path, index=False)
    logging.info(f"Dados salvos em: {excel_path}")

    # Salvar o último valor de 'fim'
    save_last_execution(fim)


if __name__ == "__main__":
    main()
