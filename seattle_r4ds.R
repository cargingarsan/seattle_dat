#usethis::create_from_github(
#  "https://github.com/cargingarsan/seattle_dat.git",
#  destdir = ("E:/proyectos/rstudio_git/bigdata")
#)


dir.create("data", showWarnings = FALSE)

curl::multi_download(
  "https://r4ds.s3.us-west-2.amazonaws.com/seattle-library-checkouts.csv",
  "data/seattle-library-checkouts.csv",
  resume = TRUE
)


library(tidyverse)
library(arrow)
library(dbplyr, warn.conflicts = FALSE)
library(duckdb)
#> Loading required package: DBI


#abrir el conjunto de datos
library(tictoc)
tic()
seattle_csv <- open_dataset(
sources = "data/seattle-library-checkouts.csv", 
col_types = schema(ISBN = string()),
format = "csv"
)
toc()

seattle_csv

seattle_csv |> glimpse()

#numero total de pagos al año
tic()
seattle_csv |> 
  group_by(CheckoutYear) |> 
  summarise(Checkouts = sum(Checkouts)) |> 
  arrange(CheckoutYear) |> 
  collect()
toc()


#reescribir los datos en parquet
pq_path <- "data/seattle-library-checkouts" #crea directorio para guardar el archivo

seattle_csv |>
  group_by(CheckoutYear) |>
  write_dataset(path = pq_path, format = "parquet")

#abriendo propiedades de nuevos archivos parquet

tibble(
  files = list.files(pq_path, recursive = TRUE),
  size_MB = file.size(file.path(pq_path, files)) / 1024^2
)


#Usando dplyr con flecha

seattle_pq <- open_dataset(pq_path)

#agrupar por libro por año especifico y por mes
query <- seattle_pq |> 
  filter(CheckoutYear >= 2018, MaterialType == "BOOK") |>
  group_by(CheckoutYear, CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(CheckoutYear, CheckoutMonth)

query

#Y podemos obtener los resultados llamando a collect():
query |> collect()

#calculamos año 2021, con archivo csv
seattle_csv |> 
  filter(CheckoutYear == 2021, MaterialType == "BOOK") |>
  group_by(CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutMonth)) |>
  collect() |> 
  system.time()
#>    user  system elapsed, deasde el ej. del libro
#>  11.951   1.297  11.387
#user  system elapsed , desde mi pc
#1.77    0.11   46.73 

#el mismo calculo usando los archivos parquet
seattle_pq |> 
  filter(CheckoutYear == 2021, MaterialType == "BOOK") |>
  group_by(CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutMonth)) |>
  collect() |> 
  system.time()
#>    user  system elapsed 
#>   0.263   0.058   0.063
#>    user  system elapsed, desde mi pc
#0.08    0.01    1.65 

#La aceleración de ~100 veces el rendimiento se puede atribuir a dos factores: 
#la partición de varios archivos y el formato de los archivos individuales:


#¡Esta enorme diferencia en el rendimiento es la razón por la que vale la pena
#convertir archivos CSV grandes en parquet!


# pasando una base de datos de parquet a duckdb  
seattle_pq |> 
to_duckdb() |>
  filter(CheckoutYear >= 2018, MaterialType == "BOOK") |>
  group_by(CheckoutYear) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutYear)) |>
  collect()

#libro mas vendido
query1 <- seattle_pq |> 
  filter(CheckoutYear >= 2005, MaterialType == "BOOK") |>
  group_by(CheckoutYear, Title) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutYear)) |>
  collect() |>
  slice(1)

#libro mas vendido de cada año
query1 <- seattle_pq |> 
  filter(CheckoutYear >= 2020, MaterialType == "BOOK") |>
  group_by(CheckoutYear, Title) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(TotalCheckouts)) |>
  collect() |>
  slice(1)

#Titulo mas vendido cada año
query2 <- seattle_pq |> 
  filter(CheckoutYear > 2005, MaterialType =="BOOK") |>
  group_by(CheckoutYear, Title) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(TotalCheckouts)) |>
  collect() |>
  slice(1)

#artista mas leido en libreria de seattle
query3 <- seattle_pq |> 
  filter(CheckoutYear >= 2005, MaterialType =="BOOK") |>
  group_by(CheckoutYear, Creator) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc( TotalCheckouts)) |>
  collect() |>
  slice(1)



  
  
  
  






