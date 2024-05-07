library(stringr)
library(dplyr)
library(sf)
library(DBI)
library(RPostgres)
library(keyring)
library(tidyverse)

library(dbplyr)

library(mapview)

source("https://raw.githubusercontent.com/Region-Dalarna/funktioner/main/func_GIS.R")

# setwd("C:/Users/SEERYA/OneDrive - Sweco AB/Documents/R-scripts/region_dalarna_r_skript/")

con <- dbConnect(RPostgres::Postgres(),
                 dbname = "geodata",
                 host = "WFALMITVS526.ltdalarna.se",
                 port = 5432,
                 user = key_list(service = "rd_geodata")$username,
                 password = key_get("rd_geodata", key_list(service = "rd_geodata")$username)
)



# SKAPA VÄGNÄTVERK -----------------------------------------------------

# lista alla scheman i databasen
scheman <- dbGetQuery(con, "SELECT nspname FROM pg_catalog.pg_namespace")
print(scheman)
schema_nr <- 11
# lista alla tabeller i schema (något i df scheman)
tabeller <- dbGetQuery(con,
                       paste0("SELECT table_name FROM information_schema.tables
                            WHERE table_schema='", scheman[schema_nr,], "'"))
print(tabeller)
#
# # koppla upp för att köra dplyr mot en databas, även i ett annat schema Lägg i obj test
tbl(con, in_schema(scheman[schema_nr,], tabeller[1,])) %>% head()
tbl(con, in_schema(scheman[schema_nr,], tabeller[1,])) %>% glimpse()
# lista kolumnnamn i test
tbl(con, in_schema(scheman[schema_nr,], tabeller[1,])) %>% colnames()


# Connect to your database using the existing 'con' object
# and assuming 'scheman' and 'tabeller' are your schema and table names stored in vectors

# Specify the columns you want to select here
columns_to_select <- c("geom", "Gatunamn_Namn", "Vagnummer_Huvudnummer_Gast1", "Vagnummer_Huvudnummer_Gast2", 
                       "Hastighetsgrans_HogstaTillatnaHastighet_F", 
                       "FunkVagklass_Klass", "Vagtrafiknat_Vagtrafiknattyp", 
                       "BegrBruttovikt_HogstaTillatnaBruttovikt_F",
                       "Trafik_ADT_fordon", "Vaghallare_Vaghallartyp", 
                       "Antal_korfalt2_Korfaltsantal", "Vagnummer_Europavag",
                       "Vagnummer_Lanstillhorighet", "Vagbredd_Bredd",
                       "Vagkategori_kategori", "Vagnummer_Undernummer", "Vagnummer_Huvudnummer_Vard", "Ovrigt_vagnamn_Namn")

# Example of hardcoding schema and table names
sverigepaket_tp_subset <- tbl(con, in_schema("nvdb", "SverigepaketTP")) %>%
  select(all_of(columns_to_select)) %>%
  filter(Trafik_ADT_fordon >= 1) %>%
  collect()

# Check the structure of the data frame to confirm the presence and type of 'geom' column
str(sverigepaket_tp_subset)

r <- sverigepaket_tp_subset

# Now the columns riktning_med and riktning_mot have been updated.
# If the geom column is of class pq_geometry, attempt conversion to sfc without specifying WKB
if ("pq_geometry" %in% class(r$geom)) {
  r <- r %>%
    mutate(geom = st_as_sfc(geom))
}

# After ensuring geom is an sfc object, try converting the data frame to an sf object
r_sf <- st_as_sf(r, sf_column_name = "geom")

# mapview(r_sf, zcol = "Trafik_ADT_fordon")

lan <- hamta_karta(karttyp = "lan", regionkoder = 20)

lan_buf <- lan %>% 
  st_buffer(30000)
# mapview(lan_buf)+
#   mapview(lan)

vagar_dalarna <- st_intersection(r_sf, lan_buf)

vagar_dalarna <- vagar_dalarna %>% 
  select(gatu_namn = Gatunamn_Namn,
         vagnr_under = Vagnummer_Undernummer, 
         vagnr_vard = Vagnummer_Huvudnummer_Vard, 
         vagnamn_ovrigt = Ovrigt_vagnamn_Namn,
         vag_nr_1 = Vagnummer_Huvudnummer_Gast1, 
         vag_nr_2 = Vagnummer_Huvudnummer_Gast2, 
         hastighet = Hastighetsgrans_HogstaTillatnaHastighet_F, 
         funk_vägklass = FunkVagklass_Klass, 
         bruttovikt = BegrBruttovikt_HogstaTillatnaBruttovikt_F, 
         arsdygnstrafik = Trafik_ADT_fordon, 
         vaghallare = Vaghallare_Vaghallartyp, 
         antal_korfalt = Antal_korfalt2_Korfaltsantal, 
         vagnr_europavag = Vagnummer_Europavag, 
         lannr_vag = Vagnummer_Lanstillhorighet, 
         vagbredd = Vagbredd_Bredd, 
         vagkategori = Vagkategori_kategori)
         

vag_66 <- vagar_dalarna %>%
  filter(vagnr_vard == 66 | vag_nr_1 == 66)


# Filtering the dataset based on specified conditions
vasalopps_vagen <- vagar_dalarna %>%
  filter(vagnr_vard == 1024 | vagnr_vard == 1025)

vag_70 <- vagar_dalarna %>% 
  filter(vagnr_vard == 70 | vag_nr_1 == 70)

E_16 <- vagar_dalarna %>% 
  filter(vagnr_vard ==16)

kommun <- hamta_karta(karttyp = "kommun", regionkoder = 2023)

vag_66_x10 <- vag_66 %>%
  mutate(arsdygnstrafik = arsdygnstrafik * 10) %>% 
  st_intersection(kommun) %>%
  select(
    -c(kod98_98, kod97_97, kod95_96, kod92_94, kod83_91, kod80_82, kod77_79, urnamn, knbef96, landareakm, knkod_int, lanskod_tx, knnamn, knkod)
  )

vasalopps_x10 <- vasalopps_vagen %>% 
  mutate(arsdygnstrafik = arsdygnstrafik * 10)

# Assuming all these datasets are already loaded and contain the column 'arsdygnstrafik'
all_traffic <- rbind(vag_66, vasalopps_vagen, vagar_dalarna, vag_70, E_16, vag_66_x10, vasalopps_x10) %>%
  pull(arsdygnstrafik)

global_min <- min(all_traffic, na.rm = TRUE)
global_max <- max(all_traffic, na.rm = TRUE)


datasets <- list(vag_66 = vag_66, vasalopps_vagen = vasalopps_vagen, vagar_dalarna = vagar_dalarna, 
                 vag_70 = vag_70, E_16 = E_16, vag_66_x10 = vag_66_x10, vasalopps_x10 = vasalopps_x10)

scaled_datasets <- lapply(datasets, function(df) {
  tryCatch({
    df <- df %>%
      dplyr::mutate(
        scaled_traffic = (arsdygnstrafik - global_min) / (global_max - global_min),
        line_width = scaled_traffic * 10  # Assuming max width is 10, adjust as needed
      )
    df  # Return the modified dataframe
  }, error = function(e) {
    cat("Error in dataset transformation: ", e$message, "\n")
    return(NULL)  # Return NULL if there's an error
  })
})

# Check again if any transformed dataset is NULL
lapply(scaled_datasets, function(df) {
  if (is.null(df)) {
    cat("Transformed dataset is NULL\n")
  } else {
    cat("Transformed dataset has data\n")
  }
})

# Print summaries to check values in scaled datasets
lapply(scaled_datasets, summary)



# Assuming the order in the list matches the named datasets
vag_66 <- scaled_datasets[[1]]
vasalopps_vagen <- scaled_datasets[[2]]
vagar_dalarna <- scaled_datasets[[3]]
vag_70 <- scaled_datasets[[4]]
E_16 <- scaled_datasets[[5]]
vag_66_x10 <- scaled_datasets[[6]]  # If there's a sixth element, assuming order matches
vasalopps_x10 <- scaled_datasets[[7]]


reds <- c("pink", "red", "darkred")
blues <- c("lightblue", "blue", "darkblue")
greens <- c("lightgreen", "green", "darkgreen")
yellows <- c("lightyellow", "yellow", "orange")
greys <- c("lightgrey", "grey", "darkgrey")

mapview(vag_66, zcol = "arsdygnstrafik", lwd = "line_width", color = reds) +
  mapview(vasalopps_vagen, zcol = "arsdygnstrafik", lwd = "line_width", color = blues) +
  mapview(vagar_dalarna, zcol = "arsdygnstrafik", lwd = "line_width", color = greys) +
  mapview(vag_70, zcol = "arsdygnstrafik", lwd = "line_width", color = greens) +
  mapview(E_16, zcol = "arsdygnstrafik", lwd = "line_width", color = yellows) +
  mapview(vag_66_x10, zcol = "arsdygnstrafik", lwd = "line_width", color = reds)+  # Assuming you want this styled similarly
  mapview(vasalopps_x10, zcol = "arsdygnstrafik", lwd = "line_width", color = blues)+
  mapview(kommun, col.regions = "grey", alpha.regions = 0.3)

mapview(vasalopps_x10, zcol = "arsdygnstrafik", lwd = "line_width")+
mapview(vasalopps_vagen, zcol = "arsdygnstrafik", lwd = "line_width")
summary(vasalopps_vagen)

mapview(vag_66_x10, zcol = "arsdygnstrafik", lwd = "line_width", color = reds)+
  mapview(vag_66, zcol = "arsdygnstrafik", lwd = "line_width", color = reds)
