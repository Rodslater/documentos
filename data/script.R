library(dplyr)
library(tidyr)
library(rio)
library(downloader)
library(lubridate)

datas <- c()

ano_corrente <- year(Sys.Date())

for (mes in 1:12) {
  ultimo_dia <- days_in_month(ymd(paste(ano_corrente, mes, 1, sep = "-")))
  for (dia in 1:ultimo_dia) {
    data <- ymd(paste(ano_corrente, mes, dia, sep = "-"))
    numero <- as.numeric(format(data, "%Y%m%d"))
    datas <- c(datas, numero)
  }
}

for(i in seq_along(datas)) {
  try({url <- paste0('https://portaldatransparencia.gov.br/download-de-dados/despesas/', datas[i]) #Try passa pra o próximo em caso de erro.
  arquivo <- sprintf("dataset_%s.zip", datas[i])
  download(url, dest=arquivo, mode="wb") 
  unzip (arquivo)
  file.remove(arquivo) #remover arquivo baixado
  arquivos_csv <- list.files(pattern = "\\.csv$", full.names = TRUE) # Listar todos os arquivos CSV na pasta
  padroes_mantidos <- c(".*Despesas_Empenho\\.csv$", ".*Despesas_Liquidacao\\.csv$", ".*Despesas_Pagamento\\.csv$") # Definir padrões de nomes de arquivos a serem mantidos
  arquivos_remover <- arquivos_csv[!grepl(paste(padroes_mantidos, collapse = "|"), arquivos_csv)] # Filtrar os arquivos CSV a serem removidos
  file.remove(arquivos_remover) # Remover os arquivos
  })
}

empenho <- import_list(dir(pattern = ".*Despesas_Empenho\\.csv$"), encoding = "Latin-1", rbind = TRUE)
liquidacao <- import_list(dir(pattern = ".*Despesas_Liquidacao\\.csv$"), encoding = "Latin-1", rbind = TRUE)
pagamento <- import_list(dir(pattern = ".*Despesas_Pagamento\\.csv$"), encoding = "Latin-1", rbind = TRUE)

empenho <- empenho |> 
  select(6,3,62,4,7,11:14,17:19, 31,33,35,37,42,47,49,53:55) 


liquidacao <- liquidacao |> 
  select(5,2,3,8:11, 14:16,18,20,24,26)

pagamento <- pagamento |> 
  select(5,2,33,3,6,10:13,16:19,21,23,27,29)


colnames(empenho) <- c("tipo_documento", "cod_empenho", "valor", "data", "tipo_empenho", "cod_orgao", 'orgao', "cod_ug", "ug", "cod_favorecido", "favorecido", "obs", "funcao", "subfuncao", "programa", "acao", "plano_orcamentario", "cat_despesa", "grupo_despesa", "elemento_despesa", "processo", "licitacao")
colnames(liquidacao) <- c("tipo_documento", "cod_liquidacao", "data", "cod_orgao", "orgao", "cod_ug", "ug", "cod_favorecido", "favorecido", "obs", "cat_despesa", "grupo_despesa", "elemento_despesa", "plano_orcamentario")
colnames(pagamento) <- c("tipo_documento", "cod_pagamento", "valor", "data", "tipo_ob", "cod_orgao", "orgao", "cod_ug", "ug", "cod_favorecido", "favorecido", "obs", "processo","cat_despesa", "grupo_despesa", "elemento_despesa", "plano_orcamentario")

empenho <- empenho |> 
  filter(cod_orgao == '26423') |> 
  mutate(valor = as.numeric(str_replace(valor, ",", "."))) |> 
  mutate(data=dmy(data))
  
liquidacao <- liquidacao |>
  filter(cod_orgao == '26423') |> 
  mutate(data=dmy(data))
  
pagamento <- pagamento |> 
  filter(cod_orgao == '26423') |> 
  mutate(valor = as.numeric(str_replace(valor, ",", "."))) |> 
  mutate(data=dmy(data))

arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)

saveRDS(empenho, 'dados/empenho.rds')
saveRDS(liquidacao, 'dados/liquidacao.rds')
saveRDS(pagamento, 'dados/pagamento.rds')
