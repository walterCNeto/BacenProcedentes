# BACEN Dashboard — Reclamações por Assunto (Live)

Dashboard público e auto-atualizável com os dados de irregularidades do ranking de reclamações do Banco Central do Brasil.

## Como publicar (passo a passo)

### 1. Criar repositório no GitHub
- Acesse github.com → New repository
- Nome: `bacen-dashboard` (ou qualquer nome)
- Visibilidade: **Public**
- Não inicializar com README

### 2. Fazer upload dos arquivos
```bash
git init
git add .
git commit -m "feat: dashboard BACEN inicial"
git remote add origin https://github.com/SEU_USUARIO/bacen-dashboard.git
git push -u origin main
```

### 3. Ativar GitHub Pages
- No repositório: Settings → Pages
- Source: **Deploy from a branch**
- Branch: `main` / pasta: `/docs`
- Save → aguardar ~2min

### 4. Popular os dados inicialmente
- No repositório: Actions → "Atualizar Dashboard BACEN" → Run workflow
- Aguardar ~5min para o scraper rodar
- O dashboard ficará disponível em: `https://SEU_USUARIO.github.io/bacen-dashboard`

### 5. Atualizações automáticas
O GitHub Actions roda todo segunda-feira às 7h UTC e detecta automaticamente
se o BACEN publicou novos dados. Se sim, atualiza o data.json e faz commit.

## Rodar localmente

```bash
pip install -r requirements.txt

# Popular dados pela primeira vez (4 anos de histórico)
python scripts/bacen_setor.py --anos 4

# Abrir docs/index.html no browser
# ATENÇÃO: para fetch() funcionar localmente precisa de um servidor HTTP:
python -m http.server 8080 --directory docs
# Abrir: http://localhost:8080
```

## Estrutura
```
bacen-dashboard/
├── .github/workflows/update.yml  ← automação semanal
├── scripts/bacen_setor.py        ← scraper BACEN
├── docs/
│   ├── index.html               ← dashboard (GitHub Pages)
│   ├── data.json                ← dados (atualizado automaticamente)
│   └── meta.json                ← metadados da última atualização
└── requirements.txt
```
