"""
migrate_has_openai_to_page.py — Converte o flag legado users.has_openai_account
em UserPagePermission(page_key='gpt_kpi_manus').

Contexto: o seed_servicos.py criou a entrada `gpt_kpi_manus` (CAMIM Analytics,
lock=ouro) na tabela public.servicos. Este script garante que todos os usuários
que hoje têm has_openai_account=True recebam a permission correspondente, sem
duplicar (idempotente).

Após rodar e validar, a coluna users.has_openai_account pode ser removida do
model em um PR separado (cleanup). Até lá, o app.py respeita os DOIS sinais
(ver render_protected_page → has_openai).

Uso:
    python3 migrate_has_openai_to_page.py             # aplica
    python3 migrate_has_openai_to_page.py --dry-run   # mostra o que faria
"""
import argparse
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from auth_db import SessionLocal, User, UserPagePermission


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        alvos = db.query(User).filter(User.has_openai_account.is_(True)).all()
        print(f"[migrate_has_openai_to_page] {len(alvos)} usuários com has_openai_account=True")

        adicionados = 0
        ja_tem     = 0
        for u in alvos:
            ja = db.query(UserPagePermission).filter_by(
                user_id=u.id, page_key="gpt_kpi_manus"
            ).first()
            if ja:
                ja_tem += 1
                print(f"  · {u.email:35} já possui page_key=gpt_kpi_manus")
                continue
            print(f"  + {u.email:35} {'(dry)' if args.dry_run else ''} adicionando page_key=gpt_kpi_manus")
            if not args.dry_run:
                db.add(UserPagePermission(user_id=u.id, page_key="gpt_kpi_manus"))
            adicionados += 1

        if not args.dry_run:
            db.commit()
        print(f"\n[migrate_has_openai_to_page] {'DRY-RUN' if args.dry_run else 'OK'} — adicionados: {adicionados}, já tinham: {ja_tem}")
    except Exception as e:
        db.rollback()
        print(f"[migrate_has_openai_to_page] ERRO: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
