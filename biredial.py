import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

# import time

# Caminho para o arquivo CSV
arquivo_dados = "biredial-dados-originais.csv"

# Carregar o arquivo CSV em um DataFrame
df = pd.read_csv(arquivo_dados, sep=",")

# Dicionário de URLs específicas para substituição
urls_especificas_para_trocar = {
    "https://ainfo.cnptia.embrapa.br/digital/bitstream/doc/1157804/1/6177.pdf": "https://www.infoteca.cnptia.embrapa.br/infoteca/handle/doc/1157804",
}

# Função para substituir URLs específicas
def substitute_urls(url):
    return urls_especificas_para_trocar.get(url, url)

# Aplicando a substituição de URLs
df["ArticleURL"] = df["ArticleURL"].apply(substitute_urls)


# Dicionário de URLs para pular e o valor de retorno associado
urls_para_pular = {
    "repositorium.uminho.pt": "Pulando repositorium.uminho.pt",
    "bdm.ufpa.br": "Pulando bdm.ufpa.br",
    "repositorium.sdum.uminho.pt": "Pulando repositorium.sdum.uminho.pt",
    "http://eprints.rclis.org/": "Pulando http://eprints.rclis.org/",
    # Adicione mais URLs aqui, se necessário
}


def get_document_type(url):
    # print("\nURL", url)
    tag_to_be_searched = "DC.type"

    # Verifica se a URL está na lista de URLs para pular
    for url_para_pular, mensagem in urls_para_pular.items():
        if url_para_pular in url:
            # print(mensagem)
            # return "DOCUMENT TYPE NOT FOUND"
            # Alteração para rodar novamente buscando só os None\
            return None

    if "wp-content" in url or "cos.ufrj.br/uploadfile/" in url:
        # return "DOCUMENT TYPE NOT FOUND"
        # print("DOCUMENT TYPE NOT FOUND")
        return None

    # Verifica se contem periodico na url, já pode retornar
    if "periodico" in url:
        # print("Artigo de periódico")
        return "Artigo de periódico"

    # Ajustando caso tenha bitstream
    if "/bitstream/" in url:
        if "handle" in url:
            # Substitui "bitstream" por vazio, removendo-o
            new_url = url.replace("bitstream/", "")
        else:
            # Replace "bitstream" with "handle"
            new_url = url.replace("bitstream", "handle")
        # print("Realizado ajuste para bitstream")

        if "jspui" in new_url:
            segments = new_url.split("/jspui/")
            # jspui tem que pegar 3 depois da "/"
            url = segments[0] + "/jspui/" + "/".join(segments[1].split("/")[:3])
            # print("Realizado ajuste para jspui")
        else:
            segments = new_url.split("/handle/")
            url = segments[0] + "/handle/" + "/".join(segments[1].split("/")[:2])
            # print("Realizado ajuste para handle")
        # print(url)

    # Ajuste para Vufind
    if "vufind" in url:
        full_url = url + "#details"
        tag_to_be_searched = "format"
        # print("Realizado ajuste para vufind")
    # Ajustando a URL para incluir 'mode=full' de forma condicional
    elif "?" in url:
        full_url = url + "&mode=full"
    else:
        full_url = url + "?mode=full"

    if "www.teses.usp.br/teses/disponiveis" in url:
        # print("Realizado ajuste para www.teses.usp.br")
        # Padrão para capturar a parte específica da URL
        padrao = r"tde-\d+-\d+"
        # Encontrar a parte específica na URL original
        parte_especifica = re.search(padrao, url).group()
        # Nova URL
        full_url = "https://www.teses.usp.br/xml.php?id=" + parte_especifica

    try:
        print("Procurando", full_url)
        response = requests.get(full_url)  # Fazendo a requisição para a URL ajustada
        soup = BeautifulSoup(response.text, "html.parser")

        # Verificar se o metadado 'dc.type' existe de outra forma no HTML
        meta_tag = soup.find(
            lambda tag: tag.name == "meta" and tag.get("name") == tag_to_be_searched
        )

        if (
            not meta_tag
        ):  # Se não encontrado nos meta tags, tentar buscar no conteúdo da página
            print("Procurando no conteúdo da página")
            meta_tag = soup.find(
                "div", text=lambda text: tag_to_be_searched.lower() in text.lower()
            )

        if meta_tag:
            doc_type = (
                meta_tag["content"] if "content" in meta_tag.attrs else meta_tag.text
            )
            doc_type = doc_type.split("/")[-1].strip().upper()  # Normalizando a saída
        else:
            # doc_type = "DOCUMENT TYPE NOT FOUND"  # Valor padrão caso não encontre
            # Alteração para rodar novamente buscando só os None
            print("DOCUMENT TYPE NOT FOUND")
            doc_type = None

        print(doc_type)
        # time.sleep(5)  # Sleep for 5 seconds
        return doc_type
    except Exception as e:
        print(f"Erro ao obter o tipo de documento para {url}: {e}")
        return None


df["TIPODOCUMENTO"] = None

for index, row in df.iterrows():
    # Check if 'TIPODOCUMENTO' is None for the current row
    if pd.isna(row["TIPODOCUMENTO"]):
        # Apply the function to the 'ArticleURL' column for the current row
        print("\nindex: ", index)
        df.at[index, "TIPODOCUMENTO"] = get_document_type(row["ArticleURL"])

        # Salvar o DataFrame modificado em um arquivo CSV
        df.to_csv("output_" + str(index) + ".csv", index=False)


def mapear_tipo_oficial(tipo):
    tipos_oficiais = {
        "TRABALHO DE CONCLUSÃO DE CURSO": "Trabalho de conclusão de curso",
        "ARTICLE": "Artigo de periódico",
        "DOCUMENT TYPE NOT FOUND": "Falta Verificar",
        "DISSERTAÇÃO": "Dissertação de Mestrado",
        "TRABALHO": "Anais de evento",
        "TRABALHO DE CONCLUSÃO DE CURSO - GRADUAÇÃO - BACHARELADO": "Trabalho de conclusão de curso",
        "OTHER": "Falta Verificar",
        "CONFERENCEOBJECT": "Anais de evento",
        "LECTURE": "Poster",
        "MASTERTHESIS": "Dissertação de Mestrado",
        "ARTIGO": "Artigo de periódico",
        "CADERNO": "Periódico",
        "MODELO": "Livro",
        "TCC": "Trabalho de conclusão de curso",
        "REPORT": "Relatório",
        "WORKINGPAPER": "Anais de evento",
        "ESTUDO TÉCNICO": "Livro",
        "ARTIGO DE PERIÓDICO": "Artigo de periódico",
        "TESE": "Tese de Doutorado",
        "ARTIGO DE EVENTO": "Anais de evento",
        "OUTROS": "Falta Verificar",
        "DOCUMENTO ADMINISTRATIVO": "Livro",
        "CAPÍTULO DE LIVRO": "Capítulo de livro",
        "DISSERTATION": "Dissertação de Mestrado",
        "BACHELORTHESIS": "Trabalho de conclusão de curso",
        "MANUAL": "Livro",
        "PEDAGOGICALPUBLICATION": "Livro",
        "RELATÓRIO": "Relatório",
        "MONOGRAFIA": "Trabalho de conclusão de curso",
        "TECHNICAL MANUALS AND PROCEDURES": "Livro",
        "PAPERS PRESENTED AT EVENTS": "Anais de evento",
        "TEXT": "Falta Verificar",
        "BOOK CHAPTER": "Capítulo de livro",
        "PREPRINT": "Anais de evento",
        "LIVROS": "Livro",
    }
    return tipos_oficiais.get(tipo, "Tipo Desconhecido")


df["TipoDeDocumento"] = df["TIPODOCUMENTO"].apply(mapear_tipo_oficial)

df.to_csv("biredial_modificado.csv", index=False)

df.to_excel("biredial_modificado.xlsx", index=False)
