#!/usr/bin/env python3
"""
wpp_cache_clientes_schema.py — Cria (ou verifica) a tabela cache_clientes no SQLite.

Seguro de rodar múltiplas vezes — usa CREATE TABLE IF NOT EXISTS e migrações leves.
Rode manualmente antes de popular o cache pela primeira vez.

Uso:
  python3 wpp_cache_clientes_schema.py
"""
import os
import sqlite3

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB_PATH = os.getenv("WAPP_CTRL_DB", "/opt/camim-auth/whatsapp_cobranca.db")


def criar_schema(db_path: str = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS cache_clientes (
        -- Chave composta: idcliente + row_id + posto
        -- idcliente: contrato/família (igual para titular e dependentes)
        -- row_id   : idCliente para titular, idDependente para dependente
        -- posto    : letra do posto (A, B, etc.) — cada posto é um banco separado
        idcliente           INTEGER NOT NULL,
        row_id              INTEGER NOT NULL,
        posto               TEXT    NOT NULL,

        -- Identificação
        id_endereco         INTEGER,
        sexo                TEXT,
        matricula           INTEGER,
        nomecadastro        TEXT,
        titular_dependente  TEXT,   -- 'Titular' | 'Dependente'
        plano               TEXT,
        idade               INTEGER,
        dataadmissao        TEXT,   -- ISO YYYY-MM-DD
        nascimento          TEXT,   -- ISO YYYY-MM-DD
        canceladoans        INTEGER NOT NULL DEFAULT 0,
        tipo_fj             TEXT,

        -- Classificação (pré-calculada no ETL)
        tipo_cliente        TEXT,   -- camim | clube | edige | particular | outro
        situacao_efetiva    TEXT,   -- situação real (situacaoclube para clube, situacao para os demais)
        situacaoclube       TEXT,
        situacao            TEXT,

        -- Plano
        clubebeneficio      INTEGER NOT NULL DEFAULT 0,
        clubebeneficiojoy   INTEGER NOT NULL DEFAULT 0,
        planopremium        INTEGER NOT NULL DEFAULT 0,

        -- Contato
        cobradornome        TEXT,
        corretor            TEXT,
        bairro              TEXT,
        origem              TEXT,
        diacobranca         INTEGER,
        responsavel         TEXT,
        responsavel_tel_wpp TEXT,
        telefone_whatsapp   TEXT,
        telefone_efetivo    TEXT,   -- wpp próprio ou do responsável (pré-calculado)

        -- Controle de carga
        carregado_em        TEXT,

        PRIMARY KEY (idcliente, row_id, posto)
    );

    CREATE INDEX IF NOT EXISTS idx_cc_posto         ON cache_clientes(posto);
    CREATE INDEX IF NOT EXISTS idx_cc_idcliente     ON cache_clientes(idcliente, posto);
    CREATE INDEX IF NOT EXISTS idx_cc_dataadmissao  ON cache_clientes(dataadmissao);
    CREATE INDEX IF NOT EXISTS idx_cc_tipo_cliente  ON cache_clientes(tipo_cliente);
    CREATE INDEX IF NOT EXISTS idx_cc_situacao      ON cache_clientes(situacao_efetiva);
    CREATE INDEX IF NOT EXISTS idx_cc_tel           ON cache_clientes(telefone_efetivo);
    """)
    conn.commit()
    conn.close()
    print(f"Schema cache_clientes criado/verificado em: {db_path}")


if __name__ == "__main__":
    criar_schema()
