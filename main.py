#pip install cassandra-driver
from cassandra.cluster import Cluster

import random

# Configuração de conexão com o ScyllaDB
cluster = Cluster(contact_points=["localhost"], port=9042)
session = cluster.connect()

disciplinas = [
    {"codigo": "CC1000", "nome": "Cálculo Diferencial e Integral"},
    {"codigo": "CC1002", "nome": "Física para Engenharia"},
    {"codigo": "CC1003", "nome": "Álgebra Linear"},
    {"codigo": "CC1004", "nome": "Mecânica dos Sólidos"},
    {"codigo": "CC1005", "nome": "Banco de Dados"},
    {"codigo": "CC1006", "nome": "Termodinâmica"},
    {"codigo": "CC1007", "nome": "Cálculo Numérico"},
    {"codigo": "CC1008", "nome": "Resistência dos Materiais"},
    {"codigo": "CC1009", "nome": "Fundamentos de Programação"}
]

alunos = [
    {"ra": "22122000", "nome": "Caio", "curso": "Ciência da Computação", "formado": True},
    {"ra": "22122000", "nome": "Caio", "curso": "Ciência da Computação", "formado": True},
    {"ra": "22122001", "nome": "Lucas Dias", "curso": "Engenharia Elétrica", "formado": False},
    {"ra": "22122002", "nome": "Lucas Rebouças", "curso": "Administração", "formado": False},
    {"ra": "22122003", "nome": "Pedro Algodão", "curso": "Engenharia de Robôs", "formado": False},
    {"ra": "22122004", "nome": "Samir Costa", "curso": "Engenharia Mecânica", "formado": True}
]


professores = [
    {"professor_id": "P001", "professor_nome": "Luciano", "departamento": "Ciência da Computação"},
    {"professor_id": "P002", "professor_nome": "Anjoletto", "departamento": "Engenharia Elétrica"},
    {"professor_id": "P003", "professor_nome": "Isaac", "departamento": "Administração de Empresas"},
    {"professor_id": "P004", "professor_nome": "Charles", "departamento": "Engenharia de Robôs"},
    {"professor_id": "P005", "professor_nome": "Destro", "departamento": "Engenharia Mecânica"}
]

session.execute("""
CREATE KEYSPACE IF NOT EXISTS modelo_faculdade
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace("modelo_faculdade")

session.execute("""
CREATE TABLE IF NOT EXISTS alunos (
    ra text PRIMARY KEY,
    nome text,
    curso_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    nota float,
    formado boolean
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS disciplinas_ministradas (
    professor_id text,
    professor_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    PRIMARY KEY (professor_id, ano, semestre, disciplina_codigo)
) WITH CLUSTERING ORDER BY (ano DESC, semestre DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS departamento_chefes (
    professor_id text,
    professor_nome text,
    departamento_nome text,
    PRIMARY KEY (departamento_nome, professor_id)
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS tccs (
    tcc_id int,
    tcc_nome text,
    professor_id text,
    professor_nome text,
    aluno_RA text,
    aluno_nome text,
    PRIMARY KEY (tcc_id, aluno_RA)
);
""")

def is_table_empty(table_name):
    rows = session.execute(f"SELECT COUNT(*) FROM {table_name}")
    return rows[0][0] == 0

if is_table_empty("alunos"):
    for aluno in alunos:
        disciplina = random.choice(disciplinas)
        
        session.execute("""
        INSERT INTO alunos (ra, nome, curso_nome, disciplina_codigo, disciplina_nome, semestre, ano, nota, formado)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            aluno['ra'], 
            aluno['nome'], 
            aluno['curso'], 
            disciplina['codigo'],
            disciplina['nome'],
            random.randint(1, 2), 
            random.randint(2020, 2024),
            random.uniform(5, 10),
            aluno['formado']
        ))
        
    session.execute("""
    INSERT INTO alunos (ra, nome, curso_nome, disciplina_codigo, disciplina_nome, semestre, ano, nota, formado)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        "RA_ALUNO_FORMADO", 
        "JONAS", 
        "CIENCIA_COMPUTACAO", 
        "CC",
        "CC",
        2, 
        2024,
        random.uniform(5, 10),
        True
    ))



if is_table_empty("disciplinas_ministradas"):
    for professor in professores:
        for disciplina in disciplinas:
            session.execute("""
            INSERT INTO disciplinas_ministradas (professor_id, professor_nome, disciplina_codigo, disciplina_nome, semestre, ano)
            VALUES (%s, %s, %s, %s, %s, %s);
            """, (professor['professor_id'], professor['professor_nome'], disciplina['codigo'], disciplina['nome'], random.randint(1, 2), random.randint(2020, 2024)))

if is_table_empty("departamento_chefes"):
    for professor in professores:
        session.execute("""
        INSERT INTO departamento_chefes (professor_id, professor_nome, departamento_nome)
        VALUES (%s, %s, %s);
        """, (professor['professor_id'], professor['professor_nome'], professor['departamento']))

if is_table_empty("tccs"):
    for i in range(1, 4):
        for aluno in alunos:
            session.execute("""
            INSERT INTO tccs (tcc_id, tcc_nome, professor_id, professor_nome, aluno_RA, aluno_nome)
            VALUES (%s, %s, %s, %s, %s, %s);
            """, (i, f"TCC {i} - Projeto de Ciência da Computação", 'P001', 'Professor A', aluno['ra'], aluno['nome']))


# 1. Histórico escolar de qualquer aluno
def get_historico_aluno(ra):
    rows = session.execute("""
    SELECT disciplina_codigo, disciplina_nome, semestre, ano, nota
    FROM alunos
    WHERE ra = %s;
    """, (ra,))
    for row in rows:
        print(row)

# 2. Histórico de disciplinas ministradas por qualquer professor
def get_historico_professor(professor_id):
    rows = session.execute("""
    SELECT disciplina_codigo, disciplina_nome, semestre, ano
    FROM disciplinas_ministradas
    WHERE professor_id = %s;
    """, (professor_id,))
    for row in rows:
        print(row)

def listar_alunos_formados(semestre, ano):
    rows = session.execute("""
    SELECT ra, nome, curso_nome
    FROM alunos
    WHERE semestre = %s AND ano = %s AND formado = true
    ALLOW FILTERING;
    """, (semestre, ano))
    for row in rows:
        print(row)

# 4. Listar professores que são chefes de departamento
def listar_professores_chefes():
    rows = session.execute("""
    SELECT professor_id, professor_nome, departamento_nome
    FROM departamento_chefes;
    """)
    for row in rows:
        print(row)

# 5. Saber quais alunos formaram um grupo de TCC e qual professor foi o orientador
def listar_tccs(tcc_id):
    rows = session.execute("""
    SELECT tcc_nome, professor_id, professor_nome, aluno_RA, aluno_nome
    FROM tccs
    WHERE tcc_id = %s;
    """, (tcc_id,))
    for row in rows:
        print(row)
        
        
print("Histórico de Aluno (22122000):")
get_historico_aluno('22122000')

print("\nHistórico de Disciplinas Ministradas pelo Professor (P001):")
get_historico_professor('P001')

print("\nAlunos Formados no Semestre 2, Ano 2024:")
listar_alunos_formados(2, 2024)

print("\nProfessores Chefes de Departamento:")
listar_professores_chefes()

print("\nTCCs Formados e Orientador:")
listar_tccs(2)

cluster.shutdown()