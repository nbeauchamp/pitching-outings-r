# daily_pitch_summaries.R — GitHub Actions friendly (no baseballr)

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(httr)
  library(jsonlite)
  library(readr)
})

# ---------- small logger ----------
timestamp_msg <- function(msg) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
}
WARN_SLOW_FETCH_SEC <- 20

# ---------- time helpers ----------
CENTRAL <- "America/Chicago"
yday_dates_central <- function() {
  yday <- as_date(with_tz(Sys.time(), tzone = CENTRAL) - days(1))
  rep(format(yday, "%Y-%m-%d"), 2)
}

# ---------- fetch from Savant CSV (robust) ----------
fetch_statcast_day <- function() {
  rng <- yday_dates_central()
  yday <- rng[1]

  url <- "https://baseballsavant.mlb.com/statcast_search/csv"
  q <- list(
    player_type = "pitcher",
    game_date_gt = yday,
    game_date_lt = yday
  )

  timestamp_msg("Calling Savant CSV endpoint (RETRY enabled)...")
  t0 <- Sys.time()
  resp <- httr::RETRY(
    "GET", url,
    query = q,
    httr::user_agent("Pitching Outings Bot (R)"),
    times = 4, pause_min = 2, pause_cap = 8,
    terminate_on = c(400,401,403,404),
    httr::timeout(75)
  )
  httr::stop_for_status(resp)
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  df <- readr::read_csv(txt, show_col_types = FALSE)
  timestamp_msg(sprintf("Downloaded CSV with %d rows and %d cols", nrow(df), ncol(df)))
  tsec <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  timestamp_msg(sprintf("Fetch completed in %.1f sec", tsec))
  if (tsec > WARN_SLOW_FETCH_SEC) timestamp_msg(sprintf("WARNING: Fetch exceeded %d sec", WARN_SLOW_FETCH_SEC))
  if (!nrow(df)) return(NULL)
  df
}

# ---------- metrics ----------
MIN_BATTERS_FACED <- 3L

compute_game_metrics <- function(df) {
  nm <- names(df)
  has <- function(x) x %in% nm
  events_col <- if (has("events")) "events" else if (has("event")) "event" else "events"
  desc_col   <- if (has("description")) "description" else "descr"

  csw_descriptions <- c("called_strike","swinging_strike","swinging_strike_blocked","foul_tip")
  whiff_desc <- c("swinging_strike","swinging_strike_blocked","foul_tip")
  inplay_outs <- c("field_out","force_out","grounded_into_double_play","strikeout_double_play",
                   "triple_play","fielders_choice_out","other_out")
  walk_events <- c("walk","hit_by_pitch")

  df %>%
    mutate(game_date = as_date(.data[["game_date"]])) %>%
    filter(!is.na(pitcher), !is.na(player_name)) %>%
    group_by(.data[["game_date"]], .data[["game_pk"]], .data[["pitcher"]], .data[["player_name"]]) %>%
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
      .groups = "drop"
    ) %>%
    filter(tbf >= MIN_BATTERS_FACED)
}

render_line <- function(row) {
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

post_to_slack <- function(text, channel = NULL) {
  webhook <- Sys.getenv("SLACK_WEBHOOK_URL", unset = "")
  if (!nzchar(webhook)) {
    cat("\n[DRY RUN: SLACK_WEBHOOK_URL not set]\n", text, "\n")
    return(invisible(FALSE))
  }
  payload <- if (is.null(channel)) list(text = text) else list(text = text, channel = channel)
  resp <- httr::POST(webhook, body = payload, encode = "json", httr::timeout(15))
  if (httr::http_error(resp)) warning("Slack POST failed: ", httr::status_code(resp))
  invisible(TRUE)
}

main <- function() {
  timestamp_msg("Starting script...")

  timestamp_msg("Fetching Statcast data...")
  t0 <- Sys.time()
  df <- fetch_statcast_day()
  fetch_secs <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  if (is.null(df) || !nrow(df)) {
    post_to_slack("*Pitching Outings — Yesterday*\n_No MLB games or data unavailable._")
    timestamp_msg(sprintf("No data. Fetch time: %.1f sec", fetch_secs))
    timestamp_msg("Script complete.")
    return(invisible())
  }

  timestamp_msg("Computing per-outing metrics...")
  outings <- compute_game_metrics(df)
  if (!nrow(outings)) {
    post_to_slack("*Pitching Outings — Yesterday*\n_No qualifying outings._")
    timestamp_msg("No qualifying outings. Script complete.")
    return(invisible())
  }

  timestamp_msg("Rendering summaries...")
  lines <- apply(outings, 1, function(r) render_line(as_tibble_row(r)))
  header <- paste0("*Pitching Outings — ", yday_dates_central()[1], "*")
  msg <- paste(c(header, sort(lines))), collapse = "\n")

  timestamp_msg("Posting to Slack...")
  post_to_slack(msg)
  timestamp_msg("Script complete.")
}

if (interactive()) main()
