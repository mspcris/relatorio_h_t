import express from 'express';
import cors from 'cors';

const app = express();
app.use(express.json());

// habilite CORS só se o domínio for diferente
app.use(cors({ origin: ['https://kpi.camim.com.br', 'https://teste-ia.camim.com.br'] }));

app.post('/api/ia-kpi', async (req, res) => {
  const { query, page_ctx } = req.body;
  // TODO: chamar seu serviço/LLM aqui
  const answer = `Ok. Pergunta: "${query}". Exemplo de resposta baseada nos KPIs.`;
  res.json({ answer });
});

app.listen(3000);
