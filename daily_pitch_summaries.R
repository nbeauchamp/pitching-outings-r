# daily_pitch_summaries.R
# v1: prior-day MLB pitching outings -> one Slack message (or prints if no webhook set)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(httr)
  library(jsonlite)
  library(baseballr)
})

timestamp_msg <- function(msg) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
}

WARN_SLOW_FETCH_SEC <- 20  # warn if the Statcast download takes longer than this

CENTRAL <- "America/Chicago"
MIN_BATTERS_FACED <- 3L

# --- Helpers ---
yday_dates_central <- function() {
  now_ct <- with_tz(Sys.time(), tzone = CENTRAL)
  yday <- as_date(now_ct - days(1))
  rep(format(yday, "%Y-%m-%d"), 2)
}

fetch_statcast_day <- function() {
  library(httr); library(readr); library(lubridate)
  
  CENTRAL <- "America/Chicago"
  yday <- format(as_date(with_tz(Sys.time(), CENTRAL) - days(1)), "%Y-%m-%d")
  
  url <- "https://baseballsavant.mlb.com/statcast_search/csv"
  q <- list(
    player_type = "pitcher",
    game_date_gt = yday,
    game_date_lt = yday
  )
  
  ua <- httr::user_agent("Twins Pitching Outings Bot (R)")
  
  # Retry up to 4 times, with pauses between tries; 75s timeout on each try
  resp <- httr::RETRY(
    "GET", url,
    query = q,
    ua,
    times = 4,            # total attempts
    pause_min = 2,        # seconds before first retry
    pause_cap = 8,        # max pause between retries
    terminate_on = c(400, 401, 403, 404),  # don't retry permanent errors
    httr::timeout(75)
  )
  
  httr::stop_for_status(resp)
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  
  df <- readr::read_csv(txt, show_col_types = FALSE)
  if (!nrow(df)) return(NULL)
  df
}

compute_game_metrics <- function(df) {
  # Normalize common column names across baseballr versions
  nm <- names(df)
  has <- function(x) x %in% nm
  col_or <- function(primary, fallback) if (has(primary)) sym(primary) else sym(fallback)
  
  pitcher_col <- if (has("pitcher")) "pitcher" else "pitcher_id"
  name_col    <- if (has("player_name")) "player_name" else if (has("pitcher_name")) "pitcher_name" else "player_name"
  desc_col    <- if (has("description")) "description" else "descr"
  events_col  <- if (has("events")) "events" else if (has("event")) "event" else "events"
  
  # Sets
  csw_descriptions <- c("called_strike","swinging_strike","swinging_strike_blocked","foul_tip")
  whiff_desc <- c("swinging_strike","swinging_strike_blocked","foul_tip")
  inplay_outs <- c("field_out","force_out","grounded_into_double_play","strikeout_double_play",
                   "triple_play","fielders_choice_out","other_out")
  walk_events <- c("walk","hit_by_pitch")
  
  df %>%
    mutate(game_date = as_date(.data[["game_date"]])) %>%
    group_by(.data[["game_date"]], .data[["game_pk"]], .data[[pitcher_col]], .data[[name_col]]) %>%
    summarise(
      pitches = n(),
      tbf = n_distinct(.data[["at_bat_number"]], na.rm = TRUE),
      outs = sum(replace_na(.data[["outs_on_pitch"]], 0)) + sum(.data[[events_col]] %in% inplay_outs, na.rm = TRUE),
      ip = round(outs / 3, 1),
      k = sum(.data[[events_col]] == "strikeout", na.rm = TRUE),
      bb = sum(.data[[events_col]] %in% walk_events, na.rm = TRUE),
      hr = sum(.data[[events_col]] == "home_run", na.rm = TRUE),
      csw = round(mean(.data[[desc_col]] %in% csw_descriptions, na.rm = TRUE) * 100, 1),
      whiffs = sum(.data[[desc_col]] %in% whiff_desc, na.rm = TRUE),
      .mix = list({
        pm <- count(cur_data(), pitch_type, name = "n")
        if (nrow(pm)) mutate(pm, pct = round(n / sum(n) * 100, 1)) %>% arrange(desc(pct))
        else tibble(pitch_type = character(), pct = numeric())
      }),
      .vels = list({
        vs <- group_by(cur_data(), pitch_type) |>
          summarise(velo = round(mean(release_speed, na.rm = TRUE), 1), .groups = "drop")
        if (nrow(vs)) vs else tibble(pitch_type = character(), velo = numeric())
      }),
      .groups = "drop"
    ) %>%
    filter(tbf >= MIN_BATTERS_FACED)
}

render_line <- function(row) {
  # row is a one-row tibble
  name <- row[[4]]
  ip <- row[["ip"]]; k <- row[["k"]]; bb <- row[["bb"]]
  csw <- row[["csw"]]; whiffs <- row[["whiffs"]]
  
  mix <- row[[".mix"]][[1]]
  top2 <- if (nrow(mix)) head(mix, 2) else tibble(pitch_type = character(), pct = numeric())
  mix_txt <- if (nrow(top2)) paste(paste0(replace_na(top2$pitch_type, "UNK"), " ", top2$pct, "%"), collapse = ", ") else ""
  
  tag <- if (!is.na(csw) && csw >= 32) " — stock up" else if (!is.na(csw) && csw <= 25) " — watch command/shape" else ""
  
  base <- sprintf("%s (%.1f IP, %d/%d, CSW %.1f%%, Whiffs %d)", name, ip, k, bb, csw, whiffs)
  if (nzchar(mix_txt)) paste0(base, " ", mix_txt, tag) else paste0(base, tag)
}

post_to_slack <- function(text) {
  webhook <- Sys.getenv("SLACK_WEBHOOK_URL", unset = "")
  if (!nzchar(webhook)) {
    cat("\n[DRY RUN: SLACK_WEBHOOK_URL not set]\n")
    cat(text, "\n")
    return(invisible(FALSE))
  }
  resp <- httr::POST(webhook, body = list(text = text), encode = "json", timeout(15))
  if (httr::http_error(resp)) {
    warning("Slack POST failed: ", httr::status_code(resp))
  }
  invisible(TRUE)
}

main <- function() {
  timestamp_msg("Starting script...")
  
  timestamp_msg("Fetching Statcast data...")
  t0 <- Sys.time()
  df <- fetch_statcast_day()
  fetch_secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  timestamp_msg(sprintf("Fetch completed in %.1f sec", fetch_secs))
  if (fetch_secs > WARN_SLOW_FETCH_SEC) {
    timestamp_msg(sprintf("WARNING: Fetch exceeded %d sec (network or Savant slow). This isn’t typical.", WARN_SLOW_FETCH_SEC))
  }
  
  if (is.null(df) || !nrow(df)) {
    post_to_slack("*Pitching Outings — Yesterday*\n_No MLB games or data unavailable._")
    return(invisible())
  }
  
  timestamp_msg("Computing per-outing metrics...")
  t1 <- Sys.time()
  outings <- compute_game_metrics(df)
  timestamp_msg(sprintf("Metrics computed in %.1f sec", as.numeric(difftime(Sys.time(), t1, units="secs"))))
  
  if (!nrow(outings)) {
    post_to_slack("*Pitching Outings — Yesterday*\n_No qualifying outings._")
    return(invisible())
  }
  
  timestamp_msg("Rendering summaries...")
  t2 <- Sys.time()
  lines <- apply(outings, 1, function(r) render_line(as_tibble_row(r)))
  timestamp_msg(sprintf("Rendering completed in %.1f sec", as.numeric(difftime(Sys.time(), t2, units="secs"))))
  
  header <- paste0("*Pitching Outings — ", yday_dates_central()[1], "*")
  msg <- paste(c(header, sort(lines)), collapse = "\n")
  
  timestamp_msg("Posting to Slack (or printing DRY RUN)...")
  post_to_slack(msg)
  
  timestamp_msg("Script complete.")
}


if (interactive()) main()
