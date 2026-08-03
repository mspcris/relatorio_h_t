/* Roda o CÓDIGO REAL de medico_custo.html (extraído por nome) contra fixtures,
   para conferir a lista da mediana: ordem, destaque do meio, par vs ímpar. */
import { readFileSync } from 'node:fs';

const HTML = readFileSync(process.argv[2], 'utf8');

/* Recorta do início até o fechamento na COLUNA 0 — o arquivo fecha toda função
   de topo assim, e nenhuma linha de template literal começa na coluna 0.
   Nada de lexer: regex literal (`esc`) quebraria qualquer tentativa. */
function recorta(src, cabeca, fim = '\n}\n') {
  const i = src.indexOf(cabeca);
  if (i < 0) throw new Error('não achei: ' + cabeca);
  const j = src.indexOf(fim, i);
  if (j < 0) throw new Error('não fechou: ' + cabeca);
  return src.slice(i, j + fim.length);
}

/* Helpers de formatação entram copiados (regex/Intl, sem lógica sob teste),
   mas o teste checa que a cópia ainda bate com o arquivo — se alguém mexer no
   brl, isto acusa em vez de passar em cima de um número diferente. */
const COPIA = `
const brl = (v, c = 2) => 'R$ ' + Number(v || 0).toLocaleString('pt-BR',
  { minimumFractionDigits: c, maximumFractionDigits: c });
const num = (v, c = 0) => Number(v || 0).toLocaleString('pt-BR',
  { minimumFractionDigits: c, maximumFractionDigits: c });
const esc = s => String(s ?? '').replace(/[&<>"]/g, m =>
  ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[m]));
`;
for (const linha of COPIA.trim().split('\n')) {
  if (!HTML.includes(linha)) {
    console.error('DIVERGIU do arquivo (helper copiado mudou):\n  ' + linha);
    process.exit(2);
  }
}

const codigo = [
  COPIA,
  recorta(HTML, 'const mediana = v => {', '\n};\n'),
  recorta(HTML, 'function DADOS_FATOR()', '\n'),
  recorta(HTML, 'function baseReferencia() {'),
  recorta(HTML, 'function escopoRef() {'),
  recorta(HTML, 'const chaveMed = ', '\n'),
  recorta(HTML, 'function painelMediana('),
].join('\n\n');

const carrega = new Function('ctx', `
  let DADOS = ctx.DADOS, estado = ctx.estado;
  ${codigo}
  return { painelMediana, mediana, baseReferencia };
`);

// ── fixture ────────────────────────────────────────────────────────────────
const ag = (posto, medico, esp, dia, vh, vaga) => ({
  posto, posto_nome: 'Posto ' + posto, medico, crm: medico.slice(0, 3),
  especialidade: esp, dia_semana: dia, valor_hora: vh, custo_por_vaga: vaga,
  tipo_agenda: 'plantao', recebe_por_comissao: false,
});

function roda(linhas, postosSel, esp, tipo, foco) {
  const DADOS = { linhas, parametros: { fator_fora_da_curva: 1.3, fator_custo_vaga_alto: 2 } };
  const estado = { postos: new Set(postosSel), especialidade: null, almocoPago: false };
  const api = carrega({ DADOS, estado });
  return api.painelMediana(esp, tipo, foco);
}

function linhasDoPainel(html) {
  return [...html.matchAll(/<tr class="([^"]*)">\s*<td class="num">(\d+)<\/td>\s*<td[^>]*>([^<]*)<\/td>\s*<td>([^<]*)</g)]
    .map(m => ({ cls: m[1].trim(), pos: +m[2], posto: m[3], medico: m[4] }));
}

let falhas = 0;
const ok = (cond, msg) => { console.log((cond ? '  ok   ' : '  FALHA') + ' ' + msg); if (!cond) falhas++; };

// ── 1) ímpar: 5 agendas, mediana é a 3ª ────────────────────────────────────
console.log('\n[1] 5 agendas (ímpar) — o meio é uma linha só');
{
  const ls = [
    ag('A', 'DR ANA',   'PEDIATRIA', 'Seg', 100, 10),
    ag('B', 'DR BRUNO', 'PEDIATRIA', 'Ter', 120, 12),
    ag('C', 'DR CARLA', 'PEDIATRIA', 'Qua', 130, 13),
    ag('D', 'DR DIEGO', 'PEDIATRIA', 'Qui', 175, 20),
    ag('E', 'DR ELIS',  'PEDIATRIA', 'Sex', 190, 30),
  ];
  const html = roda(ls, [], 'PEDIATRIA', 'hora', 'DR DIEGO||DR ');
  const rows = linhasDoPainel(html);
  ok(rows.length === 5, `5 linhas na tabela (veio ${rows.length})`);
  ok(rows.map(r => r.medico).join(',') === 'DR ANA,DR BRUNO,DR CARLA,DR DIEGO,DR ELIS',
     'ordenado do menor para o maior');
  const meio = rows.filter(r => r.cls.includes('meio'));
  ok(meio.length === 1 && meio[0].pos === 3, `mediana destacada na posição 3 (veio ${meio.map(m => m.pos)})`);
  ok(html.includes('Mediana <b>R$ 130,00</b>'), 'mediana = R$ 130,00 no cabeçalho');
  ok(html.includes('R$ 169,00'), 'limite = 130 × 1,30 = R$ 169,00');
  const eu = rows.filter(r => r.cls.includes('eu'));
  ok(eu.length === 1 && eu[0].medico === 'DR DIEGO', 'agenda do médico em foco marcada');
  const acima = rows.filter(r => r.cls.includes('acima')).map(r => r.medico);
  ok(acima.join(',') === 'DR DIEGO,DR ELIS', `acima do limite: DIEGO e ELIS (veio ${acima})`);
  ok(!html.includes('class="corte"'), 'sem faixa de corte quando o n é ímpar');
}

// ── 2) par: mediana é a média das duas do meio, com faixa entre elas ────────
console.log('\n[2] 4 agendas (par) — a mediana não é nenhuma linha');
{
  const ls = [
    ag('A', 'DR ANA',   'CARDIOLOGIA', 'Seg', 100),
    ag('B', 'DR BRUNO', 'CARDIOLOGIA', 'Ter', 120),
    ag('C', 'DR CARLA', 'CARDIOLOGIA', 'Qua', 140),
    ag('D', 'DR DIEGO', 'CARDIOLOGIA', 'Qui', 400),
  ];
  const html = roda(ls, [], 'CARDIOLOGIA', 'hora', null);
  const rows = linhasDoPainel(html);
  const meio = rows.filter(r => r.cls.includes('meio')).map(r => r.pos);
  ok(meio.join(',') === '2,3', `as DUAS do meio destacadas (veio ${meio})`);
  ok(html.includes('Mediana <b>R$ 130,00</b>'), 'mediana = (120+140)/2 = R$ 130,00');
  ok(html.includes('mediana = média das duas do meio = R$ 130,00'), 'faixa de corte explica a média');
  const iCorte = html.indexOf('class="corte"');
  const i120 = html.indexOf('R$ 120,00'), i140 = html.indexOf('R$ 140,00');
  ok(i120 < iCorte && iCorte < i140, 'faixa fica ENTRE a 2ª e a 3ª linha');
}

// ── 3) a base é a mesma do cálculo: só posto filtra; comissão/exame ficam fora ──
console.log('\n[3] base da lista = base da mediana');
{
  const ls = [
    ag('A', 'DR ANA',   'NEURO', 'Seg', 100),
    ag('B', 'DR BRUNO', 'NEURO', 'Ter', 200),
    ag('G', 'DR GAL',   'NEURO', 'Qua', 900),
    { ...ag('A', 'DR COMISSAO', 'NEURO', 'Qui', 5), recebe_por_comissao: true },
    { ...ag('A', 'RAIO-X',      'NEURO', 'Sex', 1), tipo_agenda: 'fora' },
    ag('A', 'DR OUTRA', 'GINECO', 'Seg', 50),
  ];
  const todos = roda(ls, [], 'NEURO', 'hora', null);
  const r1 = linhasDoPainel(todos);
  ok(r1.length === 3, `comissão e exame fora da lista (veio ${r1.length}, esperado 3)`);
  ok(!todos.includes('DR OUTRA'), 'outra especialidade não entra');
  ok(todos.includes('na rede'), 'sem filtro de posto o escopo é "na rede"');

  const soAB = roda(ls, ['A', 'B'], 'NEURO', 'hora', null);
  const r2 = linhasDoPainel(soAB);
  ok(r2.length === 2, `filtro de posto corta a base (veio ${r2.length}, esperado 2)`);
  ok(soAB.includes('Mediana <b>R$ 150,00</b>'), 'mediana de A+B = R$ 150,00 (era 200 na rede)');
  ok(soAB.includes('nos 2 postos A, B'), 'o escopo aparece no texto');
}

// ── 4) especialidade sem base ──────────────────────────────────────────────
console.log('\n[4] especialidade sem plantão remunerado');
{
  const html = roda([ag('A', 'DR ANA', 'PEDIATRIA', 'Seg', 100)], [], 'FONOAUDIOLOGIA', 'hora', null);
  ok(html.includes('sem base, não há mediana a conferir'), 'diz que não há base, não inventa número');
  ok(!html.includes('<table'), 'não desenha tabela vazia');
}

// ── 5) custo por consulta usa o outro campo e o outro fator ────────────────
console.log('\n[5] tipo "vaga" — custo por consulta');
{
  const ls = [
    ag('A', 'DR ANA',   'ORTO', 'Seg', 100, 10),
    ag('B', 'DR BRUNO', 'ORTO', 'Ter', 120, 20),
    ag('C', 'DR CARLA', 'ORTO', 'Qua', 130, 90),
  ];
  const html = roda(ls, [], 'ORTO', 'vaga', null);
  ok(html.includes('Mediana <b>R$ 20,00</b>'), 'mediana do custo por vaga = R$ 20,00');
  ok(html.includes('R$ 40,00'), 'limite = 20 × 2 = R$ 40,00');
  ok(html.includes('R$/consulta'), 'coluna nomeada R$/consulta');
  const acima = linhasDoPainel(html).filter(r => r.cls.includes('acima'));
  ok(acima.length === 1 && acima[0].medico === 'DR CARLA', 'só a de R$ 90 passa do dobro');
}

// ── 6) agenda sem o valor do tipo pedido não entra ─────────────────────────
console.log('\n[6] agenda sem custo por vaga (ordem de chegada) fica fora da lista de vaga');
{
  const ls = [
    ag('A', 'DR ANA',   'DERMA', 'Seg', 100, 10),
    ag('B', 'DR BRUNO', 'DERMA', 'Ter', 120, null),
    ag('C', 'DR CARLA', 'DERMA', 'Qua', 130, 30),
  ];
  const vaga = linhasDoPainel(roda(ls, [], 'DERMA', 'vaga', null));
  ok(vaga.length === 2, `sem vaga não entra na lista de vaga (veio ${vaga.length})`);
  const hora = linhasDoPainel(roda(ls, [], 'DERMA', 'hora', null));
  ok(hora.length === 3, `mas continua na lista de hora (veio ${hora.length})`);
}

console.log(falhas ? `\n${falhas} FALHA(S)\n` : '\nTudo passou.\n');
process.exit(falhas ? 1 : 0);
