# Bibliotecas
import pandas as pd
import pyautogui
import time
import os
import datetime
import tabula
from io import StringIO
import numpy as np
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Data - copia na busca, um dia anterior ao executado
hoje = datetime.datetime.now()
dia_exec = hoje.strftime('%Y%m%d')
dia_exec = pd.to_datetime(dia_exec)  # Garantir tipo datetime

# Carrega o DataFrame inicial
abt = pd.read_csv('dados.csv', encoding='iso-8859-1')

# Lista compartilhada para armazenar os resultados de cada thread
resultados = []

# Semaphore para limitar o número de threads simultâneas
semaphore = threading.Semaphore(8)

# Função para processar um lote de índices
def processa_lote(alfas):
    semaphore.acquire()
    try:
      
        driver = webdriver.Chrome()
        
        lista_lote = []

        for alfa in alfas:
            driver.get("https://www.eneldistribuicao.com.br/ce/DesligamentoProgramado.aspx")

            # Campo de busca
            elemento = driver.find_element(By.XPATH, "/html/body/form/div[4]/div/div/div[2]/div/div[1]/div[1]/div/div[1]/div/div/input")
            elemento.clear()
            abt_text = abt['Nº da UC/ Instalação'].iloc[alfa]
            elemento.send_keys(str(abt_text))
            time.sleep(0.5)

            # Botão de submit
            botao_submit = driver.find_element(By.XPATH, "/html/body/form/div[4]/div/div/div[2]/div/div[1]/div[1]/div/div[2]/div/input")
            botao_submit.click()
            time.sleep(4)

            lista = []
            for i in range(2, 10):
                try:
                    data_1 = driver.find_element(By.XPATH, f"/html/body/form/div[4]/div/div/div[2]/div/div[1]/div[1]/div/div[3]/div/div/table/tbody/tr[{i}]/td[1]").get_attribute('innerText')
                    horario = driver.find_element(By.XPATH, f"/html/body/form/div[4]/div/div/div[2]/div/div[1]/div[1]/div/div[3]/div/div/table/tbody/tr[{i}]/td[2]").get_attribute('innerText')
                    local = driver.find_element(By.XPATH, f"/html/body/form/div[4]/div/div/div[2]/div/div[1]/div[1]/div/div[3]/div/div/table/tbody/tr[{i}]/td[3]").get_attribute('innerText')
                    lista.append({
                        'UC': abt['Nº da UC/ Instalação'].iloc[alfa],
                        'data': data_1,
                        'horario': horario,
                        'local': local
                    })
                except Exception as e:
                    print(f"Erro ao acessar linha {i} para alfa {alfa}: {e}")
                    break
            time.sleep(0.5)

            df_lista = pd.DataFrame(lista)
            lista_lote.append(df_lista)

        driver.quit()

        if lista_lote:
            df_lote = pd.concat(lista_lote, ignore_index=True)
            resultados.append(df_lote)

    finally:
        semaphore.release()

# Criação dos lotes de índices
lotes = [range(i, min(i + 10, len(abt))) for i in range(0, len(abt), 10)]

# Execução das threads
threads = []
for lote in lotes:
    thread = threading.Thread(target=processa_lote, args=(lote,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

# Consolidação final
df_consolidado = pd.concat(resultados, ignore_index=True)

# Conversão da coluna 'data' para datetime
df_consolidado['data'] = pd.to_datetime(df_consolidado['data'], dayfirst=True, errors='coerce')

# Função de classificação
def classificar_faixa(data):
    if pd.isna(data):
        return 'Data inválida'
    delta = (data - dia_exec).days
    if delta < 0:
        return 'já passou'
    elif delta <= 5:
        return 'Acontecerá nos próximos 5 dias'
    elif delta <= 10:
        return 'Acontecerá nos próximos 10 dias'
    else:
        return 'Acima de 10 dias'

# Criar nova coluna com a faixa
df_consolidado['faixa'] = df_consolidado['data'].apply(classificar_faixa)

# Criar nova coluna de observação
df_consolidado['observacao'] = df_consolidado['faixa'].apply(
    lambda x: 'verificar se carta foi enviada' if x in [
        'Acontecerá nos próximos 5 dias',
        'Acontecerá nos próximos 10 dias'
    ] else ''
)

# Salvar no Excel
df_consolidado.to_excel('dados_consolidados_hoje.xlsx', index=False)
