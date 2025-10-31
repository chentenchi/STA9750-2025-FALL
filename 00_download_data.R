if(!dir.exists(file.path("data", "mp01"))){
  dir.create(file.path("data", "mp01"), showWarnings=FALSE, recursive=TRUE)
}

GLOBAL_TOP_10_FILENAME <- file.path("data", "mp01", "global_top10_alltime.csv")

if(!file.exists(GLOBAL_TOP_10_FILENAME)){
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv", 
                destfile=GLOBAL_TOP_10_FILENAME)
}

COUNTRY_TOP_10_FILENAME <- file.path("data", "mp01", "country_top10_alltime.csv")

if(!file.exists(COUNTRY_TOP_10_FILENAME)){
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv", 
                destfile=COUNTRY_TOP_10_FILENAME)
}