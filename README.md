# Projet DB Cassandra/Scylla

## Tecnologias

- **ScyllaDB**
- **Python**
- **cassandra-driver**(lib)

## Estrutura do Banco

- **alunos**: Informações sobre alunos (RA, nome, curso, disciplinas, notas).
- **disciplinas_ministradas**: Disciplinas ministradas por professores.
- **departamento_chefes**: Professores chefes de departamento.
- **tccs**: Informações sobre os TCCs dos alunos.

## Funcionalidades

1. **Histórico de um Aluno**: Exibe disciplinas, notas, semestre e ano.
2. **Histórico de Disciplinas de um Professor**: Exibe as disciplinas ministradas por um professor.
3. **Alunos Formados**: Lista alunos formados em um semestre/ano específico.
4. **Professores Chefes de Departamento**: Lista professores chefes de departamento.
5. **TCCs e Orientadores**: Exibe alunos que participaram de TCCs e seus orientadores.

## Como Usar

1. Instale o **ScyllaDB** ou **Cassandra** (COM O DOCKER COMPOSE).
2. Instale a biblioteca **cassandra-driver**:
   ```bash
   docker compose up
   pip install cassandra-driver
   ```
3. Execute o script Python, que cria as tabelas e insere dados.
4. As consultas serão exibidas no terminal.
