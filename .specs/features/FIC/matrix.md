# FIC — Campaign Matrix

> Cada linha = uma célula `(layer × ErrorKind × outcome)`. Um teste deve existir para cada linha antes que a lane feche.
> Status: ⚪ pending · 🟡 in-progress · ✅ passing · ❌ failing

| Scenario ID | Layer  | ErrorKind         | Fault injected                                      | Expected outcome                                              | Test file                                              | Status     |
|-------------|--------|-------------------|-----------------------------------------------------|---------------------------------------------------------------|--------------------------------------------------------|------------|
| FIC.3       | Bronze | schema_drift      | Adiciona coluna extra ao parquet Bronze             | fix aplicado, retry bem-sucedido, runs COMPLETED              | fault_campaign/test_b1_single_kind.py                  | ⚪ pending |
| FIC.4       | Silver | regex_break       | Substitui message_body por formato desconhecido     | regex regenerado, override persistido, segundo run usa cache  | fault_campaign/test_b1_single_kind.py                  | ⚪ pending |
| FIC.5       | Bronze | partition_missing | Deleta diretório de partição Bronze                 | F1 ingest re-roda via fix, partição recriada, identidade ok   | fault_campaign/test_b1_single_kind.py                  | ⚪ pending |
| FIC.6       | Silver | out_of_range      | Append de linha com valor negativo                  | quarantine contado, last_fix_kind=acknowledge_quarantine, COMPLETED | fault_campaign/test_b1_single_kind.py             | ⚪ pending |
| FIC.7       | Bronze | unknown           | Bytes corrompidos (arquivo truncado)                | UNKNOWN, sem fix, escalação imediata, run FAILED              | fault_campaign/test_b1_single_kind.py                  | ⚪ pending |
| FIC.8       | Silver | regex_break       | Fix sempre falha (broken fix)                       | exatamente retry_budget rows + 1 escalação por kind           | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.9       | Silver | schema_drift      | Batch A escala, Batch B tem runners limpos          | A escalado, B completa todas as 3 layers                      | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.10      | Silver | out_of_range      | Mesma kind em Silver e Gold rastreados separadamente | budgets independentes por layer                              | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.11      | Bronze | schema_drift      | schema_drift + partition_missing na camada Bronze   | ambos diagnosticados e resolvidos independentemente           | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.12      | Silver | regex_break       | regex_break + out_of_range + schema_drift propagado | cada kind tratado na ordem de ocorrência                      | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.13      | Gold   | schema_drift      | schema_drift + falha de agregação Gold              | fix de schema aplicado, agregação re-tenta                    | fault_campaign/test_b2_budget_layers.py                | ⚪ pending |
| FIC.14      | Silver | schema_drift      | schema_drift + out_of_range no mesmo batch          | resolução sequencial, nenhum double-count                     | fault_campaign/test_b3_compound_conc.py                | ⚪ pending |
| FIC.15      | Silver | regex_break       | Segunda kind diferente surge durante retry          | segunda kind diagnosticada, sem double-count                  | fault_campaign/test_b3_compound_conc.py                | ⚪ pending |
| FIC.16      | —      | —                 | Segundo runner tenta adquirir lock ocupado          | AgentBusyError lançado, sem linha duplicada em agent_runs     | fault_campaign/test_b3_compound_conc.py                | ⚪ pending |
| FIC.17      | —      | —                 | Lock com PID morto + mtime antigo                   | takeover atômico em ≤1 tentativa                              | fault_campaign/test_b3_compound_conc.py                | ⚪ pending |
| FIC.18      | —      | —                 | SIGINT lançado mid-loop                             | agent_runs.status=INTERRUPTED, evento loop_stopped emitido    | fault_campaign/test_b3_compound_conc.py                | ⚪ pending |
| FIC.19      | Silver | regex_break       | agent_failures gravados, ManifestDB reaberta        | counters preservados, budget continua do ponto anterior       | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
| FIC.20      | Bronze | schema_drift      | schema_drift injetado duas vezes                    | fix idempotente, bytes idênticos, sem linha nova              | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
| FIC.21      | Silver | regex_break       | Override salvo, agente reiniciado                   | override carregado, zero chamadas LLM, recovery imediato      | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
| FIC.22      | Silver | regex_break       | Padrão determinístico presente                      | fake_llm_client.calls == 0                                    | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
| FIC.23      | Silver | unknown           | 11 unknowns — LLM budget cap = 10                   | 11ª chamada curto-circuita sem LLM                            | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
| FIC.24      | Silver | unknown           | LLM retorna JSON malformado                         | classify retorna UNKNOWN, sem crash                           | fault_campaign/test_b4_persistence_diag.py             | ⚪ pending |
