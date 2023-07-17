library(dplyr)
library(tidyr)
library(stringr)
library(data.table)
library(downloader)
library(lubridate)
library(httr)
library(doParallel)

num_cores <- parallel::detectCores()
registerDoParallel(cores = num_cores)

# Crie um vetor vazio para armazenar as datas
datas <- c()

# Defina a data inicial como 01/01/2021
data_inicial <- as.Date("2021-01-01")

# Defina a data final como 31/12/2021
data_final <- as.Date("2021-12-31")

# Gere as datas de 01/01/2022 a 31/12/2022
datas <- seq(data_inicial, data_final, by = "day")

# Converta as datas para o formato numérico YYYYMMDD
datas <- as.numeric(format(datas, "%Y%m%d"))


# Função para baixar e processar um arquivo
download_and_process <- function(data) {
  url <- paste0('https://portaldatransparencia.gov.br/download-de-dados/despesas/', data, '.zip')
  arquivo <- sprintf("dataset_%s.zip", data)
  
  # Verifica se o arquivo existe antes de fazer o download
  response <- tryCatch(
    {
      HEAD(url)
    },
    error = function(e) {
      return(NULL)
    }
  )
  
  # Se a resposta é NULL, o arquivo não existe, então retorna
  if (is.null(response)) {
    message(paste("Arquivo não encontrado:", arquivo))
    return(NULL)
  }
  
  tryCatch(
    {
      download.file(url, destfile = arquivo, mode = "wb")
      unzip(arquivo)
      file.remove(arquivo)
      arquivos_csv <- list.files(pattern = "\\.csv$", full.names = TRUE)
      padroes_mantidos <- c(".*Despesas_Empenho\\.csv$", ".*Despesas_Liquidacao\\.csv$", ".*Despesas_Pagamento\\.csv$")
      arquivos_remover <- arquivos_csv[!grepl(paste(padroes_mantidos, collapse = "|"), arquivos_csv)]
      file.remove(arquivos_remover)
    },
    error = function(e) {
      message(paste("Erro ao baixar o arquivo:", arquivo))
      return(NULL)
    }
  )
}

# Número de processos paralelos a serem executados
num_cores <- parallel::detectCores()

# Executa o download e processamento em paralelo
results <- foreach(data = datas, .packages = c("httr", "lubridate"), .combine = c) %dopar% {
  download_and_process(data)
}

# Verifica se algum arquivo retornou NULL (não encontrado ou erro)
null_results <- which(sapply(results, is.null))
if (length(null_results) > 0) {
  message("Alguns arquivos não foram encontrados ou ocorreram erros.")
}




empenho <- list.files(pattern = ".*Despesas_Empenho\\.csv$", full.names = TRUE) 

colunas_empenho <- c('Tipo Documento','Código Empenho Resumido','Valor do Empenho Convertido pra R$','Data Emissão','Tipo Empenho','Código Órgão','Órgão','Código Unidade Gestora','Unidade Gestora','Código Favorecido','Favorecido','Observação','Função','SubFunção','Programa','Ação','Plano Orçamentário','Categoria de Despesa','Grupo de Despesa','Elemento de Despesa','Modalidade de Licitação','Processo')

empenho <- rbindlist(lapply(empenho, function(file) {
  fread(file, select = colunas_empenho, encoding = "Latin-1", colClasses = "character")
}))

empenho <- empenho |> 
  filter(`Código Órgão` == '26423') |> 
  rename(Valor = `Valor do Empenho Convertido pra R$`) |> 
  mutate(Valor = as.numeric(str_replace(Valor, ",", "."))) |> 
  arrange(desc(`Data Emissão`), desc(`Código Empenho Resumido`))

saveRDS(empenho, 'data/empenho.rds')

rm(list = ls())
Sys.sleep(5)
gc()




liquidacao <- list.files(pattern = ".*Despesas_Liquidacao\\.csv$", full.names = TRUE) 

colunas_liquidacao <- c('Tipo Documento','Código Liquidação Resumido','Data Emissão','Código Órgão','Código Unidade Gestora','Unidade Gestora','Código Favorecido','Favorecido','Observação','Categoria de Despesa','Grupo de Despesa','Elemento de Despesa','Plano Orçamentário')

liquidacao <- rbindlist(lapply(liquidacao, function(file) {
  fread(file, select = colunas_liquidacao, encoding = "Latin-1", colClasses = "character")
}))

liquidacao <- liquidacao |>
  filter(`Código Órgão` == '26423') |> 
  arrange(desc(`Data Emissão`), desc(`Código Liquidação Resumido`))

saveRDS(liquidacao, 'data/liquidacao.rds')

rm(list = ls())
Sys.sleep(5)
gc()




pagamento <- list.files(pattern = ".*Despesas_Pagamento\\.csv$", full.names = TRUE) 

colunas_pagamento <- c('Tipo Documento','Código Pagamento Resumido','Valor do Pagamento Convertido pra R$','Data Emissão','Tipo OB','Código Órgão','Código Unidade Gestora','Unidade Gestora','Código Favorecido','Favorecido','Observação','Processo','Categoria de Despesa','Grupo de Despesa','Elemento de Despesa','Plano Orçamentário')

pagamento <- rbindlist(lapply(pagamento, function(file) {
  fread(file, select = colunas_pagamento, encoding = "Latin-1", colClasses = "character")
}))

pagamento <- pagamento |> 
  filter(`Código Órgão` == '26423') |> 
  rename(Valor = `Valor do Pagamento Convertido pra R$`) |> 
  mutate(Valor = as.numeric(str_replace(Valor, ",", "."))) |>  
  arrange(desc(`Data Emissão`), desc(`Código Pagamento Resumido`))

saveRDS(pagamento, 'data/pagamento.rds')

rm(list = ls())
Sys.sleep(5)
gc()



arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)
