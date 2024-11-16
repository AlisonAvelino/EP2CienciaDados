from neo4j import GraphDatabase
import csv
import re
import ast

# Credenciais para conexão com o Neo4j
URI = "neo4j+s://aa76ea09.databases.neo4j.io"  # Altere para o endereço do seu Neo4j se necessário
USERNAME = "neo4j"
PASSWORD = "EqYNLlSsB0yVkCg4feELXlhrn0Hn3nC1HHRIWIAYQa0"  # Insira sua senha aqui

# Inicializando o driver do Neo4j
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# Função para inserir um Pokémon no banco de dados
def insert_pokemon(tx, pokemon):
    peso_str = pokemon['pokemon_peso']
    peso_match = re.search(r"(\d+(\.\d+)?)", peso_str)  # Captura o número decimal antes de qualquer unidade
    peso = float(peso_match.group(0)) if peso_match else None  # Converte para float ou None se não encontrar

    tx.run("""
        MERGE (p:Pokemon {id: $pokemon_id})
        SET p.name = $pokemon_name,
            p.altura = $pokemon_altura,
            p.peso = $pokemon_peso,
            p.url_pagina = $url_pagina
        WITH p
        UNWIND $pokemon_tipos AS tipo
        MERGE (t:Tipo {name: tipo})
        MERGE (p)-[:TEM_TIPO]->(t)
        WITH p
        UNWIND $pokemon_habilidades AS habilidade
        MERGE (h:Habilidade {name: habilidade.nome, url: habilidade.url})
        MERGE (p)-[:TEM_HABILIDADE]->(h)
        WITH p
        UNWIND $pokemon_proximas_evolucoes AS evolucao
        MERGE (e:Pokemon {id: evolucao.numero, name: evolucao.nome, url: evolucao.url})
        MERGE (p)-[:EVOLUI_PARA]->(e)
        """, 
        pokemon_id=pokemon['pokemon_id'],
        pokemon_name=pokemon['pokemon_name'],
        pokemon_altura=pokemon['pokemon_altura'],
        pokemon_peso=peso,
        url_pagina=pokemon['url_pagina'],
        pokemon_tipos=pokemon['pokemon_tipos'],
        pokemon_habilidades=pokemon['pokemon_habilidades'],
        pokemon_proximas_evolucoes=pokemon['pokemon_proximas_evolucoes']
    )

# Método de transformação das listas de strings no CSV
def process_list(campo):
    return [item.strip() for item in campo.split(',') if item.strip()]

# Método para converter uma string de lista de dicionários em uma lista de dicionários
def process_dictionaries_list(campo):
    try: 
        return ast.literal_eval(campo)
    except (SyntaxError, ValueError):
        return []

# Inserindo todos os dados no Neo4j
def insert_data_neo4j(pokemons):
    with driver.session() as session:
        for pokemon in pokemons:
            session.execute_write(insert_pokemon, pokemon)


# Lendo o arquivo CSV e inserindo os dados no Neo4j
with open('df.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    pokemons = []

    for row in reader:
        # Processa o campo de tipos e converte para uma lista de strings
        row['pokemon_tipos'] = process_list(row['pokemon_tipos'])

        # Converte as habilidades e evoluções para listas de dicionários usando ast.literal_eval
        row['pokemon_habilidades'] = process_dictionaries_list(row['pokemon_habilidades'])
        row['pokemon_proximas_evolucoes'] = process_dictionaries_list(row['pokemon_proximas_evolucoes'])

        pokemons.append(row)

    # Insere os dados no Neo4j
    insert_data_neo4j(pokemons)

# Fechando o driver após a inserção
driver.close()