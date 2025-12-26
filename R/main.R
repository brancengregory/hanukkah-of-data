library(readr)
library(here)
library(stringr)
library(dplyr)
library(purrr)
library(lubridate)
library(tidyr)

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

name_to_initials <- function(name) {
	splits <- str_split(name, " ")
	map(splits, \(s) str_split_i(s, "", 1)) |>
		unlist() |>
		str_flatten()
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

problem2 <- customers |>
	mutate(
		initials = map_chr(name, name_to_initials)
	) |>
	filter(initials == "JP") |>
	left_join(orders, by = "customerid") |>
	filter(year(ordered) == 2017) |>
	left_join(items, by = "orderid") |>
	left_join(products, by = "sku") |>
	filter(
		.by = customerid,
		str_detect(desc, "(?i)coffee|bagel")
	) |>
	distinct(phone) |>
	pull()

is_cancer <- function(birthdate) {
	birth_month <- month(birthdate)
	birth_day <- day(birthdate)
	if (birth_month == 6) {
		birth_day >= 21
	} else if (birth_month == 7) {
		birth_day <= 22
	} else {
		FALSE
	}
}

problem3 <- customers |>
	mutate(
		birth_year = year(birthdate),
		year_of_rabbit = birth_year %in% seq(from = 2023, to = 1900, by = -12),
		cancer = map_lgl(birthdate, is_cancer)
	) |>
	filter(
		year_of_rabbit & cancer
	) |>
	left_join(orders, by = "customerid") |>
	left_join(items, by = "orderid") |>
	left_join(products, by = "sku") |>
	filter(str_detect(desc, "(?i)rug\\b"))

problem4 <- orders |>
	arrange(ordered) |>
	mutate(time = hms::as_hms(ordered)) |>
	filter(
		time < hms::as_hms("05:00:00"),
		time > hms::as_hms("04:00:00")
	) |>
	left_join(items, by = "orderid") |>
	left_join(products, by = "sku") |>
	mutate(cat = str_extract(sku, "^[A-Z]{3}")) |>
	filter(cat == "BKY") |>
	left_join(customers, by = "customerid") |>
	summarise(
		.by = c(name, customerid, phone),
		n = n()
	) |>
	slice_max(order_by = n, n = 1) |>
	pull(phone)

problem5 <- products |>
	mutate(
		cat = str_extract(sku, "^[A-Z]{3}")
	) |>
	filter(cat == "PET", str_detect(desc, "(?i)cat"), str_detect(desc, "(?i)senior")) |>
	left_join(items, by = "sku") |>
	left_join(orders, by = "orderid") |>
	left_join(customers, by = "customerid") |>
	filter(str_detect(citystatezip, "(?i)staten")) |>
	summarise(
		.by = c(customerid, name, phone),
		n = n()
	) |>
	slice_max(order_by = n, n = 1) |>
	pull(phone)

problem6 <- orders |>
	left_join(items, by = "orderid") |>
	left_join(products, by = "sku") |>
	left_join(customers, by = "customerid") |>
	summarise(
		.by = c(customerid, name, phone),
    total = sum(qty * unit_price) |> round(2),
		wholesaletotal = sum(qty * wholesale_cost) |> round(2)
	) |>
	mutate(margin = (total - wholesaletotal) / wholesaletotal) |>
	slice_min(order_by = margin, n = 1) |>
	pull(phone)

problem7 <- orders |>
	filter(customerid == 4167) |>
	mutate(ordered_date = floor_date(ordered, "day")) |>
	left_join(items, by = "orderid") |>
	left_join(products, by = "sku") |>
	filter(str_detect(desc, "(?i)\\(")) |>
	mutate(clean_desc = str_extract(desc, "(.*)(?=) \\(.*\\)", group = 1)) |>
	inner_join(
		orders |>
			mutate(ordered_date = floor_date(ordered, "day")) |>
			left_join(items, by = "orderid") |>
			left_join(products, by = "sku") |>
			filter(str_detect(desc, "(?i)\\(")) |>
			mutate(clean_desc = str_extract(desc, "(.*)(?=) \\(.*\\)", group = 1)),
		by = c("ordered_date", "clean_desc")
	) |>
	filter(customerid.x != customerid.y) |>
	left_join(customers, by = c("customerid.y" = "customerid")) |>
	distinct(name, phone)

collectibles <- products |> filter(str_detect(sku, "^COL")) |> pull(sku)

problem8 <- items |>
	left_join(
		orders,
		by = "orderid"
	) |>
	select(customerid, sku) |>
	nest(.by = customerid) |>
	mutate(
		has_all = map_lgl(
			data,
			\(d) {
				all(collectibles %in% d$sku)
			}
		)
	) |>
	filter(has_all) |>
	left_join(customers, by = "customerid") |>
	pull(phone)

