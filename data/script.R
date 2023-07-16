library(dplyr)
library(tidyr)
library(stringr)
library(data.table)
library(downloader)
library(lubridate)
library(httr)
library(doParallel)

memory.limit(24576)
num_cores <- parallel::detectCores()
registerDoParallel(cores = num_cores)

# Crie um vetor vazio para armazenar as datas
datas <- c()

# Defina a data inicial como o primeiro dia do ano há 1 ano atrás
data_inicial <- ymd(paste(year(Sys.Date()) - 1, "01-01", sep = "-"))

# Obtenha a data atual
data_atual <- Sys.Date()

# Gere as datas desde o primeiro dia do ano há 1 ano atrás até a data atual
datas <- seq(data_inicial, data_atual, by = "day")
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



# Função para ler e combinar os dados de um arquivo
read_and_combine <- function(file) {
  data <- fread(file, encoding = "Latin-1", colClasses = "character")
  return(data)
}

# Listar os arquivos CSV na pasta
empenho <- list.files(pattern = ".*Despesas_Empenho\\.csv$", full.names = TRUE) 
# Lista para armazenar os dados combinados
combined_empenho <- list()
# Loop para ler e combinar os dados de cada arquivo de empenho
for (file in empenho) {
  data <- read_and_combine(file)
  combined_empenho <- c(combined_empenho, list(data))
  rm(data)  # Liberar memória ocupada pelo arquivo
}

# Combina os dados de todos os arquivos
empenho <- rbindlist(combined_empenho)

objects_to_remove <- ls()  # Obtém uma lista de todos os objetos no ambiente
objects_to_remove <- objects_to_remove[!sapply(objects_to_remove, function(obj) is.data.frame(get(obj)))]  # Filtra apenas os objetos que não são data frames
rm(list = objects_to_remove)  # Remove os objetos selecionados
Sys.sleep(5)
gc()

empenho <- empenho |> 
  select(6,3,62,4,7,11:14,17:19,31,33,35,37,42,47,49,53,55,54) 

empenho <- empenho |> 
  filter(`Código Órgão` == '26423') |> 
  rename(Valor = `Valor do Empenho Convertido pra R$`) |> 
  mutate(Valor = as.numeric(str_replace(Valor, ",", "."))) |> 
  arrange(desc(`Data Emissão`), desc(`Código Empenho Resumido`))

saveRDS(empenho, 'data/empenho.rds')
rm(list=ls())
Sys.sleep(5)
gc()




liquidacao <- list.files(pattern = ".*Despesas_Liquidacao\\.csv$", full.names = TRUE) 

# Função para ler e combinar os dados de um arquivo
read_and_combine <- function(file) {
  data <- fread(file, encoding = "Latin-1", colClasses = "character")
  return(data)
}

# Lista para armazenar os dados combinados
combined_liquidacao <- list()
# Loop para ler e combinar os dados de cada arquivo de liquidação
for (file in liquidacao) {
  data <- read_and_combine(file)
  combined_liquidacao <- c(combined_liquidacao, list(data))
  rm(data)  # Liberar memória ocupada pelo arquivo
}
liquidacao <- rbindlist(combined_liquidacao)

objects_to_remove <- ls()  # Obtém uma lista de todos os objetos no ambiente
objects_to_remove <- objects_to_remove[!sapply(objects_to_remove, function(obj) is.data.frame(get(obj)))]  # Filtra apenas os objetos que não são data frames
rm(list = objects_to_remove)  # Remove os objetos selecionados
Sys.sleep(5)
gc() 

liquidacao <- liquidacao |> 
  select(5,2,3,8,10,11,14:16,18,20,24,26)

liquidacao <- liquidacao |>
  filter(`Código Órgão` == '26423') |> 
  arrange(desc(`Data Emissão`), desc(`Código Liquidação Resumido`))


saveRDS(liquidacao, 'data/liquidacao.rds')
rm(list=ls())
Sys.sleep(5)
gc()





pagamento <- list.files(pattern = ".*Despesas_Pagamento\\.csv$", full.names = TRUE) 

# Função para ler e combinar os dados de um arquivo
read_and_combine <- function(file) {
  data <- fread(file, encoding = "Latin-1", colClasses = "character")
  return(data)
}


combined_pagamento <- list()

# Loop para ler e combinar os dados de cada arquivo de pagamento
for (file in pagamento) {
  data <- read_and_combine(file)
  combined_pagamento <- c(combined_pagamento, list(data))
  rm(data)  # Liberar memória ocupada pelo arquivo
}

pagamento <- rbindlist(combined_pagamento)

objects_to_remove <- ls()  # Obtém uma lista de todos os objetos no ambiente
objects_to_remove <- objects_to_remove[!sapply(objects_to_remove, function(obj) is.data.frame(get(obj)))]  # Filtra apenas os objetos que não são data frames
rm(list = objects_to_remove)  # Remove os objetos selecionados
Sys.sleep(5)
gc()

pagamento <- pagamento |> 
  select(5,2,33,3,6,10,12,13,16:19,21,23,27,29)

pagamento <- pagamento |> 
  filter(`Código Órgão` == '26423') |> 
  rename(Valor = `Valor do Pagamento Convertido pra R$`) |> 
  mutate(Valor = as.numeric(str_replace(Valor, ",", "."))) |>  
  arrange(desc(`Data Emissão`), desc(`Código Pagamento Resumido`))

saveRDS(pagamento, 'data/pagamento.rds')
rm(list=ls())
Sys.sleep(5)
gc()


arquivos_csv <- dir(pattern = ".csv")
file.remove(arquivos_csv)
