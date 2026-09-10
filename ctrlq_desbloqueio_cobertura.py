# ctrlq_desbloqueio_cobertura.py
#
# Leitura do desbloqueio de agenda PELO HORÁRIO (turno) em vez de pelo cadastro
# do médico, e o resumo em português que vai embaixo de cada cartão.
#
# Por que existe (decidido com o Cristiano em 2026-09-10): num plantão de ordem
# de chegada o turno é fixo e os médicos passam por ele. Quando um sai e outro
# entra, a agenda nova não tem "antes" nenhum — o "antes" verdadeiro é quem
# cobria aquele horário. A tela antiga comparava a agenda com ela mesma e por
# isso mostrava diferença de centavos e escondia a troca de médico.
#
# TUDO AQUI É CONTA, NÃO É MODELO DE LINGUAGEM. O texto sai de template e a
# confiança sai de fatos verificáveis (achou o gatilho? tem foto anterior?
# achou a cobertura?). Cada ponto perdido é listado em `fatores`, para a tela
# poder dizer POR QUE a confiança não é 100 — mesmo padrão do medico_custo,
# onde todo selo tem de explicar a própria conta.

import re

DIAS = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
DIA_LABEL = {"Segunda": "segunda", "Terca": "terça", "Quarta": "quarta",
             "Quinta": "quinta", "Sexta": "sexta", "Sabado": "sábado",
             "Domingo": "domingo"}

# Plantonista = ordem de chegada, SEM mínimo de horas. Decidido pelo Cristiano
# em 2026-09-10 ("pediatria e clínica geral por ordem de chegada, não importando
# o número de horas"), derrubando o corte de 11h que eu havia proposto. A
# jornada continua sendo lida e mostrada na tela, mas não filtra mais nada.
HORAS_PLANTAO = 0

_RE_HORA = re.compile(r"^(\d{1,2}):(\d{2})")


def hm(v):
    """Normaliza hora. A mesma coluna devolve None, '  ' (espaços), '07:00',
    '07:00:00' e datetime.time — os quatro casos foram medidos em Realengo."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    m = _RE_HORA.match(s)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    if h > 23 or mi > 59:
        return None
    return f"{h:02d}:{mi:02d}"


def minutos(t):
    return int(t[:2]) * 60 + int(t[3:5]) if t else None


def horas_turno(ini, fim):
    """Turno que vira a madrugada (fim <= início) soma 24h, senão dá negativo."""
    a, b = minutos(ini), minutos(fim)
    if a is None or b is None:
        return None
    if b <= a:
        b += 24 * 60
    return round((b - a) / 60.0, 2)


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def linhas_do_registro(rec, prefixo=""):
    """Uma linha por dia ativo do cadastro: horário, custo, modalidade."""
    out = []
    for d in DIAS:
        if not rec.get(prefixo + d):
            continue
        ini = hm(rec.get(f"{prefixo}{d}HoraInicio"))
        fim = hm(rec.get(f"{prefixo}{d}HoraFim"))
        custo = _num(rec.get(f"{prefixo}ValorCusto{d}"))
        alm_ini = hm(rec.get(f"{prefixo}{d}Almocoinicio"))
        alm_fim = hm(rec.get(f"{prefixo}{d}AlmocoFim"))
        alm_h = horas_turno(alm_ini, alm_fim) if rec.get(f"{prefixo}{d}Almoco") else 0.0
        h = horas_turno(ini, fim)
        oc = bool(rec.get(f"{prefixo}{d}OrdemChegada"))
        www = bool(rec.get(f"{prefixo}{d}Internet"))
        tel = bool(rec.get(f"{prefixo}{d}Telefone"))
        out.append({
            "dia": d, "ini": ini, "fim": fim,
            "horas": h, "almoco_h": alm_h or 0.0,
            "horas_liq": round(h - (alm_h or 0.0), 2) if h is not None else None,
            "custo": custo,
            "qtd": rec.get(f"{prefixo}QuantidadeCusto{d}"),
            "rh": round(custo / h, 2) if (custo and h) else None,
            "oc": oc, "www": www, "tel": tel,
            "modalidade": "misto" if (oc and (www or tel)) else ("oc" if oc else "agendado"),
        })
    return out


def eh_plantao(linhas):
    """Plantonista = tem ordem de chegada em algum dia. Sem piso de horas."""
    return any(l["oc"] and (l["horas"] or 0) >= HORAS_PLANTAO for l in linhas)


def _mesmo_turno(a, b, tolerancia_min=60):
    """Dois horários são o mesmo turno se começam perto (até 1h) OU se um cobre
    mais da metade do outro. 07-19 e 08-19 são o mesmo plantão de dia coberto
    por gente diferente; 07-19 e 19-07 não são."""
    if not (a.get("ini") and b.get("ini")):
        return False
    ia, fa = minutos(a["ini"]), minutos(a["fim"]) if a.get("fim") else None
    ib, fb = minutos(b["ini"]), minutos(b["fim"]) if b.get("fim") else None
    if abs(ia - ib) <= tolerancia_min:
        return True
    if fa is None or fb is None:
        return False
    if fa <= ia:
        fa += 1440
    if fb <= ib:
        fb += 1440
    inter = min(fa, fb) - max(ia, ib)
    menor = min(fa - ia, fb - ib)
    return menor > 0 and inter / menor > 0.5


def montar_cobertura(rec, cobertura_rows):
    """Para cada dia ativo do registro, quem mais está no mesmo turno.

    `cobertura_rows` são TODAS as agendas ativas da especialidade no posto
    (inclusive a do próprio registro, que é descartada aqui pelo id).
    """
    id_self = rec.get("idEspecialidade")
    minhas = linhas_do_registro(rec)
    outras = []
    for c in cobertura_rows or []:
        if c.get("idEspecialidade") == id_self:
            continue
        for l in linhas_do_registro(c):
            l["idEspecialidade"] = c.get("idEspecialidade")
            l["medico"] = c.get("medico")
            l["idMedico"] = c.get("idMedico")
            l["data_fim"] = c.get("DataFimExibicao")
            outras.append(l)

    turnos = []
    for l in minhas:
        pares = [o for o in outras if o["dia"] == l["dia"] and _mesmo_turno(l, o)]
        pares.sort(key=lambda x: (x["ini"] or "", x["medico"] or ""))
        custo_outros = sum(p["custo"] or 0 for p in pares)
        turnos.append({
            **l,
            "acompanhantes": pares,
            "n_no_turno": len(pares) + 1,
            "custo_turno": round((l["custo"] or 0) + custo_outros, 2),
            "custo_turno_sem_este": round(custo_outros, 2),
        })

    # Custo semanal da especialidade no posto: este registro + todas as outras
    custo_sem_outros = 0.0
    for c in cobertura_rows or []:
        if c.get("idEspecialidade") == id_self:
            continue
        for l in linhas_do_registro(c):
            custo_sem_outros += l["custo"] or 0
    custo_sem_este = sum(l["custo"] or 0 for l in minhas)

    return {
        "turnos": turnos,
        "plantao": eh_plantao(minhas),
        "horas_semana": round(sum(l["horas"] or 0 for l in minhas), 2),
        "custo_semana_registro": round(custo_sem_este, 2),
        "custo_semana_especialidade": round(custo_sem_outros + custo_sem_este, 2),
        "custo_semana_sem_este": round(custo_sem_outros, 2),
        "medicos_especialidade": len({c.get("idMedico") for c in (cobertura_rows or [])}),
    }


# ── carga do médico no posto (alerta das 24h) ────────────────────────────────

LIMITE_HORAS_POSTO = 24


def carga_no_posto(rec, todas_do_posto):
    """Horas semanais do médico NO POSTO, somando todas as especialidades.

    `todas_do_posto` é a lista de registros do posto (o JSON do ETL) mais as
    linhas de cobertura — qualquer coleção que tenha idMedico e os dias.
    """
    idm = rec.get("idMedico")
    if idm is None:
        return None
    vistos, horas, custo = set(), 0.0, 0.0
    for c in todas_do_posto or []:
        if c.get("idMedico") != idm:
            continue
        ide = c.get("idEspecialidade")
        if ide in vistos:
            continue
        vistos.add(ide)
        for l in linhas_do_registro(c):
            horas += l["horas"] or 0
            custo += l["custo"] or 0
    return {"horas": round(horas, 2), "custo": round(custo, 2),
            "agendas": len(vistos), "acima_limite": horas > LIMITE_HORAS_POSTO,
            "limite": LIMITE_HORAS_POSTO}


# ── resumo em português + confiança ──────────────────────────────────────────

def _fmt_brl(v):
    if v is None:
        return "—"
    return f"R$ {v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


def _fmt_h(v):
    if v is None:
        return "—"
    return (f"{v:.0f}h" if float(v).is_integer() else f"{v:.1f}h".replace(".", ","))


def montar_resumo(rec, cob, carga=None):
    """Frases + confiança. Devolve dict pronto para a tela.

    A confiança começa em 100 e cada buraco de evidência desconta um valor
    fixo, registrado em `fatores`. Não é palpite: é a lista do que faltou.
    """
    frases = []
    fatores = []
    conf = 100

    esp = (rec.get("Especialidade") or "especialidade").strip()
    med = (rec.get("medico") or "o médico").strip()
    turnos = cob.get("turnos") or []

    # 1. o que este registro é
    if turnos:
        dias_txt = ", ".join(
            f"{DIA_LABEL[t['dia']]} {t['ini']}–{t['fim']}" if t["ini"] and t["fim"]
            else DIA_LABEL[t["dia"]]
            for t in turnos)
        frases.append(
            f"{med} atende {esp.lower()} em {dias_txt}, "
            f"somando {_fmt_h(cob.get('horas_semana'))} por semana e "
            f"{_fmt_brl(cob.get('custo_semana_registro'))} por semana neste cadastro.")
    else:
        frases.append(f"{med} não tem nenhum dia ativo neste cadastro.")
        fatores.append(("Cadastro sem dia ativo — não dá para ler o turno", -30))
        conf -= 30

    if any(t["ini"] is None or t["fim"] is None for t in turnos):
        fatores.append(("Algum dia está sem horário no cadastro", -15))
        conf -= 15

    # 2. o gatilho
    g = rec.get("aud_gatilho") or {}
    if g and g.get("estimado"):
        frases.append(
            "A auditoria não tem a linha que abre o desbloqueio, então a data do "
            f"gatilho foi estimada em data fim menos {rec.get('_dias_datafim', 8)} dias. "
            "Isso acontece quando a agenda é criada já com custo e tempo definidos.")
        fatores.append(("Gatilho estimado, não lido da auditoria", -20))
        conf -= 20
    elif g and g.get("data"):
        quem = g.get("usuario") or "usuário não identificado"
        campos = [m.get("campo") for m in (g.get("mudancas") or [])]
        campos = [c for c in campos if c and c.lower() != "datafimexibicao"]
        alt = f", alterando {', '.join(campos[:3])}" if campos else ""
        frases.append(f"O registro foi aberto por {quem}{alt}.")
        if not g.get("usuario"):
            fatores.append(("Auditoria sem o nome de quem alterou", -5))
            conf -= 5
    else:
        frases.append("Não foi possível localizar na auditoria o que abriu este registro.")
        fatores.append(("Sem auditoria para este posto", -30))
        conf -= 30

    # 3. o "antes"
    fonte = rec.get("hist_fonte")
    if fonte == "vespera_gatilho":
        pass  # melhor caso, sem desconto
    elif fonte == "reconstruido_auditoria":
        frases.append(
            "Não existe foto do cadastro anterior ao gatilho, então o estado antigo "
            "foi remontado desfazendo as alterações do ciclo.")
        fatores.append(("Estado anterior reconstruído, não fotografado", -15))
        conf -= 15
    else:
        fatores.append(("Sem foto anterior em Cad_EspecialidadeHistorico", -10))
        conf -= 10

    # 4. a cobertura do turno — o coração da leitura nova
    if cob.get("plantao"):
        sozinhos = [t for t in turnos if t["n_no_turno"] == 1]
        com_gente = [t for t in turnos if t["n_no_turno"] > 1]
        if com_gente:
            partes = []
            for t in com_gente[:3]:
                nomes = ", ".join((p["medico"] or "?").title() for p in t["acompanhantes"][:3])
                partes.append(
                    f"{DIA_LABEL[t['dia']]} {t['ini']}–{t['fim']} tem mais "
                    f"{len(t['acompanhantes'])} médico(s) no mesmo horário ({nomes}), "
                    f"e o turno inteiro custa {_fmt_brl(t['custo_turno'])}")
            frases.append("No mesmo turno: " + "; ".join(partes) + ".")
        if sozinhos:
            dias_s = ", ".join(DIA_LABEL[t["dia"]] for t in sozinhos)
            frases.append(
                f"Em {dias_s} este é o único plantonista de {esp.lower()} cadastrado no horário. "
                "Retirar a data fim aqui não deixa o turno descoberto, mas encerrá-lo deixaria.")
    else:
        frases.append(
            "Este cadastro é de agenda marcada, não de ordem de chegada, então a "
            "leitura por turno não se aplica e vale a comparação por cadastro.")

    # 5. o peso na especialidade
    tot = cob.get("custo_semana_especialidade") or 0
    meu = cob.get("custo_semana_registro") or 0
    if tot > 0 and meu > 0:
        pct = meu / tot * 100
        frases.append(
            f"{esp.title()} no posto custa {_fmt_brl(tot)} por semana somando "
            f"{cob.get('medicos_especialidade')} médicos. Este cadastro é "
            f"{pct:.0f}% disso; sem ele a especialidade cairia para "
            f"{_fmt_brl(cob.get('custo_semana_sem_este'))} por semana.")

    # 6. alerta de jornada
    if carga and carga.get("acima_limite"):
        frases.append(
            f"ATENÇÃO: somando todas as agendas dele neste posto, {med} faz "
            f"{_fmt_h(carga['horas'])} por semana, acima do limite de "
            f"{carga['limite']}h.")

    # 7. data fim vencida
    if rec.get("_vencido"):
        frases.append(
            "A data fim deste registro já passou e ele continua ativo no ERP, "
            "ou seja, continua valendo e continua custando.")

    conf = max(10, min(100, conf))
    return {
        "texto": " ".join(frases),
        "frases": frases,
        "confianca": conf,
        "fatores": [{"motivo": m, "peso": p} for m, p in fatores],
        "base": ("turno" if cob.get("plantao") else "agenda"),
    }
