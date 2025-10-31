# ===== Step A — Setup (clean session friendly) =====
if(!dir.exists(file.path("data", "mp02"))){
  dir.create(file.path("data", "mp02"), showWarnings = FALSE, recursive = TRUE)
}

library <- function(pkg){
  pkg <- as.character(substitute(pkg))
  options(repos = c(CRAN = "https://cloud.r-project.org"))
  if(!require(pkg, character.only = TRUE, quietly = TRUE)) install.packages(pkg)
  stopifnot(require(pkg, character.only = TRUE, quietly = TRUE))
}

# Core packages
library(tidyverse)   # includes dplyr, readr, purrr, stringr, etc.
library(dplyr)       # attach explicitly to ensure verbs are on the path
library(glue)
library(readxl)
library(tidycensus)

# One-time before first ACS call:
# tidycensus::census_api_key("YOUR_KEY_HERE", install = TRUE); readRenviron("~/.Renviron")

# Helper to download/cache ACS 1-year data across years
get_acs_all_years <- function(variable, geography = "cbsa",
                              start_year = 2009, end_year = 2023){
  fname <- glue::glue("{variable}_{geography}_{start_year}_{end_year}.csv")
  fname <- file.path("data", "mp02", fname)
  
  if(!file.exists(fname)){
    YEARS <- setdiff(seq(start_year, end_year), 2020)  # skip 2020 ACS1
    ALL_DATA <- purrr::map(YEARS, function(yy){
      tidycensus::get_acs(geography, variable, year = yy, survey = "acs1") |>
        dplyr::mutate(year = yy) |>
        dplyr::select(-moe, -variable) |>
        dplyr::rename(!!variable := estimate)
    }) |>
      dplyr::bind_rows()
    readr::write_csv(ALL_DATA, fname)
  }
  
  readr::read_csv(fname, show_col_types = FALSE)
}

# --- Pull the four ACS tables ---
INCOME <- get_acs_all_years("B19013_001") |>
  dplyr::rename(household_income = B19013_001)

RENT <- get_acs_all_years("B25064_001") |>
  dplyr::rename(monthly_rent = B25064_001)

POPULATION <- get_acs_all_years("B01003_001") |>
  dplyr::rename(population = B01003_001)

HOUSEHOLDS <- get_acs_all_years("B11001_001") |>
  dplyr::rename(households = B11001_001)

