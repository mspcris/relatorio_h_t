"""
seed_alarmes_horrivel.py — Semeia os alarmes de "Horrível" pedidos em
2026-08-10: quando a PIOR campanha WhatsApp ativa de um posto chega a
Horrível (serviço wpp_campanha), notificar o gestor do posto e o Cristiano,
por e-mail e WhatsApp, 1x por dia (08:30, todos os dias), com link de
ciência (Central de Notificação de Problemas).

Idempotente: reconhece pelo NOME do alarme; rodar de novo não duplica.
Só cria alarme para posto que TEM gerente cadastrado (sem destinatário o
disparo seria em vão). Uso:
    python seed_alarmes_horrivel.py [--dry-run]
"""

import sys
import alarmes_db as adb

DRY = "--dry-run" in sys.argv
PREFIXO = "Horrível — WhatsApp campanha parada — posto "
DIRETOR_EMAIL = "cristiano@camim.com.br"


def main() -> int:
    adb.init_db()

    # Diretor Cristiano (telefone fica para preencher na tela /alarmes)
    diretores = {d["email"].strip().lower(): d for d in adb.listar_diretores(ativo=True)}
    dire = diretores.get(DIRETOR_EMAIL)
    if not dire:
        if DRY:
            print(f"[dry-run] criaria diretor {DIRETOR_EMAIL}")
            did = None
        else:
            did = adb.criar_diretor({
                "nome": "Cristiano", "email": DIRETOR_EMAIL, "telefone": "",
                "recebe_1_wpp": 0, "recebe_1_email": 1,
            })
            print(f"diretor criado: {DIRETOR_EMAIL} (id={did})")
    else:
        did = dire["id"]
        print(f"diretor já existe: {DIRETOR_EMAIL} (id={did})")

    existentes = {a["nome"] for a in adb.listar_alarmes()}
    gerentes = {g["posto"]: g for g in adb.listar_gerentes()}

    criados = 0
    for posto, g in sorted(gerentes.items()):
        nome = PREFIXO + posto
        if nome in existentes:
            print(f"já existe: {nome}")
            continue
        dados = {
            "nome": nome,
            "posto": posto,
            "servico": "wpp_campanha",
            "status_gatilho": "horrivel",
            "mensagem": (
                "Há pelo menos uma campanha ativa de WhatsApp deste posto sem "
                "enviar NADA há 5 dias ou mais. Isso normalmente significa robô "
                "travado ou campanha esquecida — cobranças e avisos deixam de "
                "chegar aos clientes. Confira o painel e acione o suporte."),
            "via_whatsapp": 1,
            "via_email": 1,
            "hora_disparo": "08:30",
            "dias_semana": "0,1,2,3,4,5,6",
            "ativo": 1,
        }
        if did:
            dados["diretores"] = [did]
        if DRY:
            print(f"[dry-run] criaria: {nome} (gerente: {g.get('email') or '?'})")
            continue
        aid = adb.criar_alarme(dados, criado_por="seed_alarmes_horrivel")
        criados += 1
        print(f"criado: {nome} (id={aid})")

    print(f"\n{'[dry-run] ' if DRY else ''}total novos: {criados} · "
          f"postos com gerente: {len(gerentes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
