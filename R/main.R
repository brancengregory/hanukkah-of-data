library(readr)
library(here)
library(stringr)
library(dplyr)
library(purrr)

customers <- read_csv(here("data/noahs-customers.csv"))
orders <- read_csv(here("data/noahs-orders.csv"))
items <- read_csv(here("data/noahs-orders_items.csv"))
products <- read_csv(here("data/noahs-products.csv"))

check_phone_string <- function(s, p) {
	key <- list(
		"2" = c("a", "b", "c"),
		"3" = c("d", "e", "f"),
		"4" = c("g", "h", "i"),
		"5" = c("j", "k", "l"),
		"6" = c("m", "n", "o"),
		"7" = c("p", "q", "r", "s"),
		"8" = c("t", "u", "v"),
		"9" = c("w", "x", "y", "z")
	)

	string_letters <- strsplit(s, "")[[1]] |> tolower()
	phone_letters <- strsplit(p, "")[[1]]
	all(imap(phone_letters, \(d, i) {
		string_letters[i] %in% key[[d]]
	}) |> unlist())
}

problem1 <- customers |>
	select(name, phone) |>
	mutate(
		phone_string = str_remove_all(phone, "-"),
		last_name = str_split_i(name, " ", 2)
	) |>
	mutate(
		matches = map2_lgl(last_name, phone_string, check_phone_string)
	) |>
	filter(matches) |>
	pull(phone)



