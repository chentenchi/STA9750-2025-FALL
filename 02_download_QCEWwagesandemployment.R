# ===== Step D — QCEW annual averages (try-many-URLs download + cache) =====
library(dplyr); library(readr); library(stringr); library(purrr); library(glue); library(httr2)

dir.create("data/mp02", recursive = TRUE, showWarnings = FALSE)

.qcew_url_candidates <- function(yy){
  # Try both hosts and both filename patterns (with and without 'qcew_')
  base <- c("https://www.bls.gov", "https://data.bls.gov")
  paths <- c(
    glue("/cew/data/files/{yy}/csv/{yy}_qcew_annual_singlefile.zip"),
    glue("/cew/data/files/{yy}/csv/{yy}_annual_singlefile.zip")
  )
  as.vector(outer(base, paths, paste0))
}

.download_zip_try <- function(url, dest){
  req <- request(url) |>
    req_headers(
      `User-Agent`      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      `Accept`          = "application/zip,application/octet-stream,*/*",
      `Accept-Language` = "en-US,en;q=0.9",
      `Referer`         = "https://www.bls.gov/cew/downloadable-data-files.htm",
      `Connection`      = "keep-alive"
    ) |>
    req_retry(max_tries = 4, backoff = ~ runif(1, 0.5, 1.5), is_transient = function(resp) resp_status(resp) >= 500)
  
  resp <- try(req_perform(req), silent = TRUE)
  if (inherits(resp, "try-error")) return(FALSE)
  if (resp_status(resp) != 200) return(FALSE)
  
  writeBin(resp_body_raw(resp), dest)
  # crude size check to avoid HTML error pages saved as .zip
  file.exists(dest) && file.info(dest)$size > 1e6
}

get_bls_qcew_annual_averages <- function(start_year = 2009, end_year = 2023){
  out_csv <- file.path("data", "mp02", glue("bls_qcew_{start_year}_{end_year}.csv.gz"))
  YEARS   <- setdiff(seq(start_year, end_year), 2020)
  
  if (!file.exists(out_csv)) {
    one_year <- function(yy){
      zip_path <- file.path("data", "mp02", glue("{yy}_qcew_annual_singlefile.zip"))
      if (!file.exists(zip_path) || file.info(zip_path)$size < 1e6) {
        message("Downloading QCEW ", yy, " …")
        ok <- FALSE
        tried <- character(0)
        for (u in .qcew_url_candidates(yy)) {
          tried <- c(tried, u)
          if (.download_zip_try(u, zip_path)) { ok <- TRUE; break }
        }
        if (!ok) stop("No working QCEW URL for ", yy, ". Tried:\n", paste0("  - ", tried, collapse = "\n"))
      }
      
      tmpdir <- tempfile(pattern = paste0("qcew_", yy, "_")); dir.create(tmpdir)
      files <- utils::unzip(zip_path, exdir = tmpdir)
      csvs  <- files[grepl("\\.csv$", files, ignore.case = TRUE)]
      if (!length(csvs)) stop("No CSV found inside QCEW ZIP for ", yy, ".")
      
      readr::read_csv(csvs[1], show_col_types = FALSE) |>
        dplyr::select(area_fips, industry_code, annual_avg_emplvl, total_annual_wages) |>
        dplyr::filter(stringr::str_starts(area_fips, "C"),
                      nchar(industry_code) <= 5,
                      !stringr::str_detect(industry_code, "-")) |>
        dplyr::mutate(
          YEAR        = yy,
          FIPS        = area_fips,
          INDUSTRY    = as.integer(industry_code),
          EMPLOYMENT  = as.integer(annual_avg_emplvl),
          TOTAL_WAGES = as.numeric(total_annual_wages),
          AVG_WAGE    = dplyr::if_else(EMPLOYMENT > 0, TOTAL_WAGES / EMPLOYMENT, NA_real_)
        ) |>
        dplyr::select(FIPS, YEAR, INDUSTRY, EMPLOYMENT, TOTAL_WAGES, AVG_WAGE) |>
        dplyr::filter(INDUSTRY != 10)
    }
    
    ALL <- purrr::map_dfr(YEARS, one_year)
    readr::write_csv(ALL, out_csv)
  }
  
  dat <- readr::read_csv(out_csv, show_col_types = FALSE)
  missing <- setdiff(YEARS, unique(dat$YEAR))
  if (length(missing)) stop(
    "QCEW missing years: ", paste(missing, collapse = ", "),
    ". If a ZIP is corrupted, delete it from data/mp02 and re-run."
  )
  dat
}

# define ↑ then call ↓
WAGES <- get_bls_qcew_annual_averages()

# quick sanity
dplyr::glimpse(WAGES)
WAGES |> count(YEAR) |> arrange(YEAR)
WAGES |> summarise(rows = n(), n_cbsa = n_distinct(FIPS), n_ind = n_distinct(INDUSTRY))


