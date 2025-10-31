# ===== Step C — BLS NAICS industry codes (robust fetch + cache) =====
library(dplyr); library(readr); library(stringr); library(purrr)
library(rvest);  library(httr2);  library(glue);  library(tibble)
dir.create("data/mp02", recursive = TRUE, showWarnings = FALSE)

get_bls_industry_codes <- function(){
  out_csv <- file.path("data", "mp02", "bls_industry_codes.csv")
  if (!file.exists(out_csv)) {
    url <- "https://www.bls.gov/cew/classifications/industry/industry-titles.htm"
    
    # Try a simple HTML fetch first (often bypasses 403)
    page <- tryCatch(
      rvest::read_html(url),
      error = function(e) NULL
    )
    
    # Fallback: httr2 with fuller headers + retry
    if (is.null(page)) {
      resp <- httr2::request(url) |>
        httr2::req_headers(
          `User-Agent`      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
          `Accept`          = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          `Accept-Language` = "en-US,en;q=0.9",
          `Referer`         = "https://www.bls.gov/cew/classifications/industry/"
        ) |>
        httr2::req_retry(max_tries = 5) |>
        httr2::req_perform()
      httr2::resp_check_status(resp)
      page <- httr2::resp_body_html(resp)
    }
    
    # Parse the table and build the leveled lookup
    raw_tbl <- page |>
      rvest::html_element("#naics_titles") |>
      rvest::html_table() |>
      tibble::as_tibble() |>
      rename(title = `Industry Title`) |>
      mutate(depth = if_else(nchar(Code) <= 5, nchar(Code) - 1L, NA_integer_)) |>
      filter(!is.na(depth))
    
    lvl4 <- raw_tbl |>
      filter(depth == 4) |>
      rename(level4_title = title) |>
      mutate(
        level1_code = str_sub(Code, end = 2),
        level2_code = str_sub(Code, end = 3),
        level3_code = str_sub(Code, end = 4)
      )
    
    lvl1 <- raw_tbl |> select(Code, title) |> rename(level1_code = Code, level1_title = title)
    lvl2 <- raw_tbl |> select(Code, title) |> rename(level2_code = Code, level2_title = title)
    lvl3 <- raw_tbl |> select(Code, title) |> rename(level3_code = Code, level3_title = title)
    
    naics <- lvl4 |>
      left_join(lvl1, by = "level1_code") |>
      left_join(lvl2, by = "level2_code") |>
      left_join(lvl3, by = "level3_code") |>
      transmute(
        level1_code, level1_title,
        level2_code, level2_title,
        level3_code, level3_title,
        level4_code = Code,
        level4_title
      )
    
    readr::write_csv(naics, out_csv)
  }
  
  readr::read_csv(out_csv, show_col_types = FALSE)
}

INDUSTRY_CODES <- get_bls_industry_codes()
glimpse(INDUSTRY_CODES)
dir("data/mp02", full.names = TRUE)  # should now include bls_industry_codes.csv

