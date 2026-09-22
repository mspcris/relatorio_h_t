/* medicos_regras.js — regras COMPARTILHADAS sobre o cadastro de médicos do CTRL-Q.
 *
 * Usado por ctrlq_relatorio.html (KPI Médicos · Qualidade) e por KPI_prescricao.html
 * (card "Médicos × certificado digital"). A regra mora AQUI para as duas telas
 * dizerem o mesmo número: 381 médicos numa e 381 na outra. ctrlq_pj_quadro.py é a
 * cópia em Python destas mesmas regras (avisos_gerenciais) — mudou aqui, muda lá.
 *
 * Expõe window.MedicosRegras = {
 *   MED_ALT_LIST, MED_ALT_PATTERNS, semAcento, isMedicinaAlternativa,
 *   nomeChave, crmChave, chaveMedico, canonLinhaCtrlq, medicosUnicos,
 *   certStatus, hojeISO
 * }
 */
(function () {
  'use strict';

  // Especialidades que NÃO são médico (terapias, exames, procedimentos).
  // Lista exata, em maiúsculas.
  const MED_ALT_LIST = [
    'ACUPUNTURISTA',
    'ENDOSCOPISTA',
    'DENSITOMETRISTA',
    'TERAPEUTA OCUPACIONAL ON LINE',
    'FONIATRA',
    'PSICOTERAPEUTA',
    'ENFERMEIRO',
    'AGENDAMENTO DE EXAMES',
    'FISIOTERAPEUTA',
    'FISIOTERAPEUTA ACUPUNTURISTA',
    'TERAPEUTA OCUPACIONAL',
    'NUTRICIONISTA',
    'FONOAUDIÓLOGO',
    'PSICÓLOGO',
    'PSICÓLOGO ON LINE',
    'TERAPEUTA',
    'NEUROPSICÓLOGO',
    'PSICANALISTA',
    'TÉCNICO EM RADIOLOGIA',
    'TÉCNICO EM TOMOGRAFIA',
    'ELETROENCEFALOGRAMA',
    'EMAGRECIMENTO ESTÉTICA',
    'ELETROCARDIOGRAMA',
    'MAPA',
    'ESPIROMETRIA',
    'HOLTER 24 HRS',
    'NUTRICIONISTA ON LINE',
    'PSICOPEDAGOGO',
    'NEUROPSICOPEDAGOGO',
    'PSICOMOTRICIDADE CLÍNICA',
    'PILATES',
    'NEUROPSICOPEDAGOGO ABA',
    'PSICOPEDAGOGO ABA',
    'ECOCARDIOGRAMA',
    'ULTRA-SONOGRAFISTA'
  ].map(e => e.toUpperCase().trim());

  // Cada posto grava a especialidade de um jeito (PSICÓLOGO, PSICOLOGIA,
  // PSICOLOGIA ON LINE...), então além da lista exata o filtro casa por
  // radical, com acento removido. Chamado 7ER-HUJ-L3A5: com o bit desmarcado
  // ficam SÓ médicos — sai terapia, exame e procedimento.
  const MED_ALT_PATTERNS = [
    // terapias / não médicos
    /PSIC/,               // psicologia, psicopedagogia, neuropsic..., psicanalista (PSIQUIATRIA não tem "PSIC")
    /FONOAUDI/,
    /FISIOTERAP/,
    /TERAPIA|TERAPEUTA/,  // terapia ABA, terapia alimentar, terapeuta ocupacional
    /NUTRICION|NUTRICAO/, // nutricionista, nutrição (NUTROLOGIA é médico e não casa)
    /ACUPUNT/,
    /ENFERM/,
    /PILATES/,
    /EMAGRECIMENTO/,
    /BIOLOG/,
    /LABORATOR/,
    /MEDICAMENTO/,
    // exames / equipamento — o "médico" é o aparelho ou a sala
    /ELETRO(CARDIO|ENCEFALO|NEURO)/,
    /ECOCARDIOGRAMA|ECO DOPPLER/,
    /HOLTER/,
    /\bMAPA\b/,
    /RAIO ?X|RADIOLOG|MAMOGRAFIA|DENSITOMETR/,
    /ULTRA.?SON/,
    /ENDOSCOP|COLONOSCOP/,
    /ESPIROMETRIA|RESSONANCIA|TOMOGRAF/,
    /\bTESTE\b/,          // teste ergométrico, teste e vacina alérgica
    /AGENDAMENTO/,
    /TECNICO/
  ];

  const semAcento = (s) => String(s ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toUpperCase().trim();

  function isMedicinaAlternativa(esp) {
    if (!esp) return false;
    const e = String(esp).toUpperCase().trim();
    if (MED_ALT_LIST.includes(e)) return true;
    const norm = semAcento(e);
    return MED_ALT_PATTERNS.some(rx => rx.test(norm));
  }

  // Chave de nome para cruzar cadastros que só têm o NOME do médico
  // (prescrição e atendimento saem das views com cad_medico.Nome; o CTRL-Q
  // também). Sem acento, maiúsculas, espaços colapsados.
  const nomeChave = (s) => semAcento(s).replace(/\s+/g, ' ');

  const crmChave = (crm) => String(crm ?? '').trim().replace(/\s+/g, '');

  // Mesma chave de dedupe do ctrlq_relatorio: CRM dentro do posto; sem CRM,
  // nome+especialidade dentro do posto.
  function chaveMedico(d) {
    const p = String(d.posto ?? '').trim().toLowerCase();
    const crm = crmChave(d.crm);
    if (crm) return `crm:${crm}|p:${p}`;
    const n = String(d.nome ?? '').trim().toLowerCase();
    const e = String(d.especialidade ?? '').trim().toLowerCase();
    return (n || e) ? `n:${n}|e:${e}|p:${p}` : null;
  }

  const _b01 = (v) => {
    if (v == null) return false;
    if (typeof v === 'boolean') return v;
    const s = String(v).trim().toLowerCase();
    if (s === '1' || s === 'true' || s === 'sim' || s === 'yes') return true;
    if (s === '0' || s === 'false' || s === 'não' || s === 'nao' || s === 'no') return false;
    const n = Number(v);
    return Number.isFinite(n) ? n === 1 : false;
  };

  // Linha do CTRLQ_RELATORIO_CONSOLIDADO.json → objeto canônico (subconjunto do
  // canonRow do ctrlq_relatorio: só o que o KPI Prescrições precisa).
  function canonLinhaCtrlq(r) {
    return {
      nome: String(r.medico ?? r.nome ?? '').trim(),
      crm: String(r.crm ?? '').trim(),
      especialidade: String(r.especialidade ?? '').trim(),
      cert: _b01(r.certificadodigital),
      // validade do certificado (ISO) se o ETL exportar; null se o JSON não tiver
      cert_validade: r.certificado_validade ? String(r.certificado_validade).slice(0, 10) : null,
      pj: _b01(r.pessoajuridica),
      rqe: _b01(r.rqe),
      posto: String(r.posto ?? '').trim(),
      idmedico: Number(r.idmedico) || null,
    };
  }

  // Linhas brutas de um mês (vários postos) → médicos únicos, sem medicina
  // alternativa/exames (a não ser que incluirMedAlt=true).
  function medicosUnicos(linhas, { incluirMedAlt = false } = {}) {
    const seen = new Set(); const out = [];
    for (const r of linhas) {
      const d = canonLinhaCtrlq(r);
      if (!incluirMedAlt && isMedicinaAlternativa(d.especialidade)) continue;
      const k = chaveMedico(d);
      if (!k || seen.has(k)) continue;
      seen.add(k); out.push(d);
    }
    return out;
  }

  // Situação do certificado num dia (hojeISO = 'YYYY-MM-DD'):
  //   'valido'   PFX cadastrado e validade >= hoje
  //   'vencido'  PFX cadastrado e validade < hoje
  //   'sem_data' PFX cadastrado sem data de validade (o ERP não gravou; 29 na rede
  //              em 2026-09-21) — OU JSON antigo, sem a coluna certificado_validade.
  //              Quem chama decide se isso conta: a tela deve dizer que é "sem data",
  //              nunca somar em válido nem em vencido.
  //   'nao'      sem PFX
  function certStatus(d, hojeISO) {
    if (!d.cert) return 'nao';
    if (!d.cert_validade) return 'sem_data';
    return d.cert_validade >= hojeISO ? 'valido' : 'vencido';
  }

  // 'YYYY-MM-DD' de hoje no fuso local (o do usuário; o KPI usa America/Sao_Paulo).
  function hojeISO() {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  }

  window.MedicosRegras = {
    MED_ALT_LIST, MED_ALT_PATTERNS, semAcento, isMedicinaAlternativa,
    nomeChave, crmChave, chaveMedico, canonLinhaCtrlq, medicosUnicos,
    certStatus, hojeISO
  };
})();
