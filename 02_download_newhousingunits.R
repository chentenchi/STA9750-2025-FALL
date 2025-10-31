# --- prereqs (safe to run again) ---
library(dplyr); library(readr); library(readxl)
library(glue);  library(stringr); library(purrr)
dir.create("data/mp02", recursive = TRUE, showWarnings = FALSE)

# ===== Step B — Building permits (new housing units) — ROBUST DOWNLOADER =====
get_building_permits <- function(start_year = 2009, end_year = 2023){
  out_csv <- file.path("data", "mp02", glue("housing_units_{start_year}_{end_year}.csv"))
  
  if(!file.exists(out_csv)){
    
    # ---------- 1) Historical TXT (<= 2018) ----------
    hist_years <- seq(start_year, min(2018, end_year))
    HIST <- map(hist_years, function(yy){
      url <- glue("https://www.census.gov/construction/bps/txt/tb3u{yy}.txt")
      lines <- readLines(url, warn = FALSE)[-(1:11)]  # drop header lines
      cbsa_lines <- str_detect(lines, "^\\d{1}:\\d{1,}")
      CBSA <- as.integer(str_sub(lines[cbsa_lines], 5, 10))
      permit_lines <- str_detect(str_sub(lines, 48, 53), "^\\d{1,}")
      PERMITS <- as.integer(str_sub(lines[permit_lines], 48, 53))
      tibble(CBSA = CBSA, new_housing_units_permitted = PERMITS, year = yy)
    }) |> bind_rows()
    
    # ---------- 2) Annual Excel files (2019+) ----------
    cur_years <- if (end_year >= 2019) seq(2019, end_year) else integer(0)
    
    # helper: try to read as xlsx first, then xls; return NULL on failure
    read_excel_safely <- function(path){
      x <- try(read_xlsx(path, skip = 5), silent = TRUE)
      if (inherits(x, "try-error")) {
        x <- try(read_xls(path,  skip = 5), silent = TRUE)
      }
      if (inherits(x, "try-error")) NULL else x
    }
    
    # helper: attempt several plausible URLs; only accept if we can read it as Excel
    candidates_for <- function(yy){
      c(
        glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}99.xls"),
        glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}99.xlsx"),
        glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}.xls"),
        glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}.xlsx")
      )
    }
    
    try_one_year <- function(yy){
      urls <- candidates_for(yy)
      dat <- NULL
      for (u in urls){
        tmp <- tempfile(fileext = ".bin")
        status <- try(utils::download.file(u, tmp, mode = "wb", quiet = TRUE), silent = TRUE)
        # require successful download, non-tiny file, and readable Excel
        if (!inherits(status, "try-error") && file.exists(tmp) && file.info(tmp)$size > 5000){
          dat <- read_excel_safely(tmp)
          if (!is.null(dat)) {  # success!
            break
          }
        }
      }
      if (is.null(dat)) stop("Could not locate a valid MSA annual Excel for ", yy, ".")
      dat |>
        tidyr::drop_na() |>
        select(CBSA, Total) |>
        mutate(year = yy) |>
        rename(new_housing_units_permitted = Total)
    }
    
    CUR <- map(cur_years, try_one_year) |> bind_rows()
    
    ALL <- bind_rows(HIST, CUR)
    write_csv(ALL, out_csv)
  }
  
  read_csv(out_csv, show_col_types = FALSE)
}

# define ↑ then call ↓
PERMITS <- get_building_permits()
glimpse(PERMITS)
dir("data/mp02", full.names = TRUE)


