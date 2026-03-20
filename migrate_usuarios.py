#!/usr/bin/env python3
"""
migrate_usuarios.py
───────────────────
Migra usuários do htpasswd + postos_acl.json para o banco SQLite.

Executar UMA VEZ no servidor, após o deploy:
    cd /opt/camim-auth
    python3 migrate_usuarios.py

O script:
  1. Cria o banco (se não existir)
  2. Lê /etc/nginx/postos_acl.json
  3. Insere cada usuário com a senha já conhecida (hashed)
  4. Para cristiano@camim.com.br: gera senha temporária e exibe no terminal
  5. Usuários já existentes no banco são ignorados (idempotente)
"""

import json
import os
import secrets

from dotenv import load_dotenv
load_dotenv("/opt/relatorio_h_t/.env")

from auth_db import SessionLocal, User, UserPosto, get_user_by_email, init_db

ACL_PATH    = "/etc/nginx/postos_acl.json"
ADMIN_EMAIL = "cristiano@camim.com.br"

# Senhas conhecidas do postos_acl_login.txt
# cristiano@camim.com.br recebe senha temporária gerada aqui
SENHAS = {
    "junior@camim.com.br":                 "{camim}2026",
    "leonardo@camim.com.br":               "2027",
    "derlana@camim.com.br":                "2028",
    "deangelo@camim.com.br":               "2029",
    "viniciusgomes@camim.com.br":          "2030",
    "carne@camim.com.br":                  "2031",
    "julio@camim.com.br":                  "2032",
    "victoriautrini@camim.com.br":         "2034",
    "lcarneiro@aquitetodigital.com.br":    "2035",
    "ronald@camim.com.br":                 "2036",
}


def main():
    init_db()
    print("=== Migração de usuários ===\n")

    # Senha temporária para o admin
    admin_temp = secrets.token_urlsafe(12)

    try:
        with open(ACL_PATH) as f:
            acl: dict = json.load(f)
        print(f"ACL carregada: {len(acl)} entradas\n")
    except Exception as e:
        print(f"[AVISO] Não foi possível ler {ACL_PATH}: {e}")
        print("Usuários serão criados sem postos — configure depois no painel /admin\n")
        acl = {}

    # Todos os e-mails: união de SENHAS + ACL
    todos_emails = set(SENHAS.keys()) | set(acl.keys()) | {ADMIN_EMAIL}

    db = SessionLocal()
    try:
        for email in sorted(todos_emails):
            email = email.lower().strip()

            if get_user_by_email(db, email):
                print(f"  SKIP (já existe): {email}")
                continue

            is_admin = (email == ADMIN_EMAIL)
            senha    = admin_temp if is_admin else SENHAS.get(email, secrets.token_urlsafe(10))

            user = User(
                email    = email,
                nome     = email.split("@")[0].replace(".", " ").title(),
                is_admin = is_admin,
                ativo    = True,
            )
            user.set_senha(senha)
            db.add(user)
            db.flush()

            postos = acl.get(email, [])
            for posto in postos:
                db.add(UserPosto(user_id=user.id, posto=posto.upper()))

            flag = " ← ADMIN" if is_admin else ""
            print(f"  OK: {email} | postos: {postos or '(nenhum)'}{flag}")

        db.commit()
    finally:
        db.close()

    print("\n" + "=" * 50)
    print("Migração concluída.")
    print("\n⚠  SENHA TEMPORÁRIA DO ADMIN:")
    print(f"   E-mail : {ADMIN_EMAIL}")
    print(f"   Senha  : {admin_temp}")
    print("\n   ➜ Acesse /admin e troque a senha via painel,")
    print("     ou use /auth/reset para receber o link por e-mail.")
    print("=" * 50)


if __name__ == "__main__":
    main()
