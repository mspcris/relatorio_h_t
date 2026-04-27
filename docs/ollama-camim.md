# Ollama CAMIM — Manual de uso

Servidor LLM local da CAMIM. Endpoint único, autenticação por Bearer token,
modelos servidos por uma VPS dedicada (6 vCPU AMD EPYC, 11 GB RAM, sem GPU).

## Endpoint

```
URL:    https://ollama.camim.com.br
Header: Authorization: Bearer <OLLAMA_API_KEY>
```

O token e a URL ficam armazenados em `/opt/relatorio_h_t/.env` na VM principal:

```
OLLAMA_URL=https://ollama.camim.com.br
OLLAMA_API_KEY=...                # nunca commit no git
OLLAMA_MODEL=gemma3:4b            # default; pode override no projeto
```

Em um projeto novo, o padrão é exportar essas três variáveis no ambiente do
processo (cron, systemd unit, dotenv) — **nunca hardcode no código**.

## Modelos disponíveis

| Modelo | Tamanho | Quando usar |
|---|---|---|
| `gemma3:4b` | 3.3 GB | Default. PT-BR forte, segue instruções bem. ~9 tok/s. |
| `qwen2.5-coder:7b` | 4.7 GB | Tarefas de código (geração, refactor, explicação). Mais lento. |

Pra adicionar outro modelo: SSH na VM e `ollama pull <nome>`.

## Capacidade real (por VM, sem GPU)

- ~9 tokens/seg sustentado em prompts curtos
- ~5–7 segundos para um resumo de 60 tokens
- Ollama serializa requests por padrão; pra paralelismo, configure
  `OLLAMA_NUM_PARALLEL=2` no service (custa RAM)
- Backlog típico: ~3.000 prompts/dia se rodar 24/7

Se precisar mais throughput num projeto, troque `gemma3:4b` por `gemma3:1b`
(3–4× mais rápido, qualidade ligeiramente menor).

---

## Receitas Python

### 1) Stdlib pura (`urllib`) — sem dependências externas

```python
import json, os, urllib.request

URL   = os.environ["OLLAMA_URL"]
KEY   = os.environ["OLLAMA_API_KEY"]
MODEL = os.environ.get("OLLAMA_MODEL", "gemma3:4b")


def gemma(prompt: str, *, max_tokens: int = 200, temperature: float = 0.2) -> str:
    body = json.dumps({
        "model":   MODEL,
        "prompt":  prompt,
        "stream":  False,
        "options": {"num_predict": max_tokens, "temperature": temperature},
    }).encode()
    req = urllib.request.Request(
        f"{URL}/api/generate",
        data=body,
        headers={
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {KEY}",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.load(r)["response"].strip()


if __name__ == "__main__":
    print(gemma("Resuma em 1 frase: clínica X teve queda de 12% em consultas em abril."))
```

### 2) SDK da OpenAI (Ollama é compatível em `/v1/`)

Pra reaproveitar código existente que já usa `openai`. Útil em projetos novos
que querem trocar entre OpenAI/Ollama mexendo só na `base_url`.

```python
from openai import OpenAI
import os

client = OpenAI(
    base_url=f"{os.environ['OLLAMA_URL']}/v1",
    api_key=os.environ["OLLAMA_API_KEY"],
)

resp = client.chat.completions.create(
    model="gemma3:4b",
    messages=[
        {"role": "system", "content": "Você é um analista financeiro da CAMIM."},
        {"role": "user",   "content": "Analise: receita caiu 12% em abril/2026."},
    ],
    temperature=0.2,
    max_tokens=300,
)
print(resp.choices[0].message.content)
```

### 3) Streaming token-a-token (`requests`)

Útil pra UI que mostra resposta sendo gerada, ou pra cortar geração quando
detectar uma sequência indesejada.

```python
import json, os, requests

URL = os.environ["OLLAMA_URL"]
KEY = os.environ["OLLAMA_API_KEY"]


def gemma_stream(prompt: str):
    with requests.post(
        f"{URL}/api/generate",
        headers={"Authorization": f"Bearer {KEY}"},
        json={"model": "gemma3:4b", "prompt": prompt, "stream": True},
        stream=True,
        timeout=120,
    ) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if not line:
                continue
            chunk = json.loads(line)
            if chunk.get("done"):
                break
            yield chunk.get("response", "")


for tok in gemma_stream("Conte uma piada curta sobre médico."):
    print(tok, end="", flush=True)
print()
```

### 4) Vision — Gemma 3 aceita imagem como input

```python
import base64, json, os, urllib.request

URL = os.environ["OLLAMA_URL"]
KEY = os.environ["OLLAMA_API_KEY"]

with open("recibo.jpg", "rb") as f:
    img_b64 = base64.b64encode(f.read()).decode()

body = json.dumps({
    "model":  "gemma3:4b",
    "stream": False,
    "messages": [{
        "role":    "user",
        "content": "Extraia data, valor e fornecedor desse recibo. Responda em JSON.",
        "images":  [img_b64],
    }],
}).encode()

req = urllib.request.Request(
    f"{URL}/api/chat",
    data=body,
    headers={"Content-Type": "application/json", "Authorization": f"Bearer {KEY}"},
)
with urllib.request.urlopen(req, timeout=180) as r:
    print(json.load(r)["message"]["content"])
```

### 5) Embeddings (busca semântica)

Gemma 3 não gera embeddings; pra isso, peça pro admin instalar
`nomic-embed-text` (`ollama pull nomic-embed-text`) e use:

```python
import json, os, urllib.request

req = urllib.request.Request(
    f"{os.environ['OLLAMA_URL']}/api/embeddings",
    data=json.dumps({"model": "nomic-embed-text",
                     "prompt": "consulta médica clínica geral"}).encode(),
    headers={"Content-Type": "application/json",
             "Authorization": f"Bearer {os.environ['OLLAMA_API_KEY']}"},
)
with urllib.request.urlopen(req, timeout=30) as r:
    vec = json.load(r)["embedding"]    # lista de floats
print(len(vec), "dims")
```

---

## Boas práticas

1. **Configurar timeout generoso (120 s+).** A primeira request com um modelo
   "frio" carrega o modelo na RAM (1×) e demora ~10 s a mais. As seguintes são
   rápidas. Não use timeout de 5 s, vai falhar em produção.
2. **Não bombardeie em paralelo.** Ollama serializa por padrão; mandar 50
   requests simultâneos só enche fila e estoura timeout. Se o projeto precisar
   processar lote, processe sequencialmente ou em pequenos batches (2–3).
3. **`temperature=0.2`** é bom default pra extração/análise factual.
   `0.7` pra texto criativo. Modelos pequenos alucinam menos com temperatura
   baixa.
4. **Limite `max_tokens`/`num_predict`.** Sem limite, o modelo enrola e gasta
   minutos. Pra resumos de 1 frase, 80 tokens basta.
5. **Reuse a mesma sessão HTTP** se for fazer várias chamadas — economiza
   handshake TLS.
6. **Trate erros de rede** (timeout, 5xx). Em projeto persistente, salve em
   fila e reprocesse depois.

## Como abrir um IP novo na VM Ollama

Hoje a VM aceita conexão de qualquer IP que apresente o token (porta 443 é
pública). Se quiser endurecer pra um projeto sensível, peça ao admin pra
adicionar uma regra UFW de IP whitelist:

```
ssh root@82.197.67.144 "ufw allow from <IP_DO_PROJETO> to any port 443 proto tcp"
```

## Como verificar se está no ar

```
curl -s -o /dev/null -w 'HTTP %{http_code}\n' \
  -H "Authorization: Bearer $OLLAMA_API_KEY" \
  https://ollama.camim.com.br/api/tags
```

Esperado: `HTTP 200`. Qualquer outra coisa, ver `/var/log/nginx/ollama.error.log`
na VM Ollama (`ssh root@82.197.67.144`).
