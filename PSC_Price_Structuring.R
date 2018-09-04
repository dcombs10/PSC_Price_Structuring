library(tidyverse)
library(lubridate)
library(data.table)
library(broom)
library(tictoc)
library(rio)

options(scipen = 999)

# User-Defined Functions:

## Customer Deciles
custDeciles <- function(rank) {
  decile <- 0
  
  if((1 - rank >= 0) & (1 - rank <= 0.10)){
    decile <- 1
  }
  if((1 - rank > 0.10) & (1 - rank <= 0.20)){
    decile <- 2
  }
  if((1 - rank > 0.20) & (1 - rank <= 0.30)){
    decile <- 3
  }
  if((1 - rank > 0.30) & (1 - rank <= 0.40)){
    decile <- 4
  }
  if((1 - rank > 0.40) & (1 - rank <= 0.50)){
    decile <- 5
  }
  if((1 - rank > 0.50) & (1 - rank <= 0.60)){
    decile <- 6
  }
  if((1 - rank > 0.60) & (1 - rank <= 0.70)){
    decile <- 7
  }
  if((1 - rank > 0.70) & (1 - rank <= 0.80)){
    decile <- 8
  }
  if((1 - rank > 0.80) & (1 - rank <= 0.90)){
    decile <- 9
  }
  if((1 - rank > 0.90) & (1 - rank <= 1)){
    decile <- 10
  }
  return(decile)
}

## Weighted Mean (w/ NAs ommitted)
weighted_mean <- function(x, w, ..., na.rm = TRUE){
  
  if(na.rm){
    
    df_omit <- na.omit(data.frame(x, w))
    
    return(weighted.mean(df_omit$x, df_omit$w, ...))
    
  } 
  
  weighted.mean(x, w, ...)
}

tally_above <- function(x, y) {
  if_else(!is.na(x) & !is.na(y),
          if_else(x > y, 1, 0),
          0)
}

tally_below <- function(x, y) {
  if_else(!is.na(x) & !is.na(y),
          if_else(x < y, 1, 0),
          0)
}

tic("Import PSC Sales Data")
path <- "C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/Sales Data"
files_list <- list.files(path)
files_list <- paste0(path, "/", files_list)
if(exists("PSC_Sales")){
  rm(PSC_Sales)
}
PSC_Sales <- do.call(rbind, lapply(files_list, fread))
toc()

tic("Import PSC Product Data")
PSC_Product_Details <- fread("C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/phocas_product.txt", sep = "|")
# PSC_Product_Details <- read_excel("C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/Prod_Cat_No.xlsx")
toc()

tic("Structure Sales Data")
str(PSC_Sales)
names(PSC_Sales)[12] <- "SELL_BR"
# Character classes
PSC_Sales$CUST_NO <- str_trim(PSC_Sales$CUST_NO, side = "both")
PSC_Sales$PRODUCT_NO <- str_trim(toupper(PSC_Sales$PRODUCT_NO), side = "both")
PSC_Sales$ORD_TYP <- str_trim(PSC_Sales$ORD_TYP, side = "both")
PSC_Sales$WRT_BY <- str_trim(PSC_Sales$WRT_BY, side = "both")
PSC_Sales$SELL_BR <- str_trim(as.character(PSC_Sales$SELL_BR), side = "both")
PSC_Sales$ZIP <- str_trim(PSC_Sales$ZIP, side = "both")
PSC_Sales$TYPE <- str_trim(PSC_Sales$TYPE, side = "both")

# Numeric classes
PSC_Sales$QTY <- as.numeric(as.character(PSC_Sales$QTY))

# Date classes
PSC_Sales$DATE <- mdy(as.character(PSC_Sales$DATE))

# Filter to legitimate B2B transactions
PSC_Sales <- PSC_Sales %>% 
  filter(!grepl("LABOR|labor|Labor", PRODUCT_NO), !grepl("9999", CUST_NO), EXT_SALES > 0, QTY > 0, TYPE == "SLS") %>% 
  select(-`ORDER#`, -FRTCOST, -MM, -YY, -SBR, -EXT_AVG_COST, -REQ_DATE, -TYPE, -SV_CODE, -SLS_NO)

toc()

tic("Structure Product Data")
str(PSC_Product_Details)
names(PSC_Product_Details)[1] <- "PRODUCT_NO"
names(PSC_Product_Details)[3] <- "CAT_NO"
PSC_Product_Details <- PSC_Product_Details %>% select(PRODUCT_NO, CAT_NO)
PSC_Product_Details$PRODUCT_NO <- str_trim(toupper(PSC_Product_Details$PRODUCT_NO), side = "both")
PSC_Product_Details$CAT_NO <- str_trim(PSC_Product_Details$CAT_NO, side = "both")
toc()

tic("Assign Customer Ranks")
# Add in Cust Rank and remerge
temp <- PSC_Sales %>% 
  group_by(CUST_NO) %>% 
  summarise(Rev = sum(EXT_SALES, na.rm = T))

Rev_ECDF <- ecdf(temp$Rev)
temp$CUST_RANK <- Rev_ECDF(temp$Rev)
setDT(temp)
setDT(PSC_Sales)
PSC_Sales <- merge(x = PSC_Sales, y = temp[ , c("CUST_NO", "CUST_RANK")], by = "CUST_NO", all.x = T)
rm(temp)

PSC_Sales$CUST_DECILE <- sapply(PSC_Sales$CUST_RANK, custDeciles)
toc()

tic("Merge Relevant Product Details")
setDT(PSC_Sales)
setDT(PSC_Product_Details)
PSC_Sales <- merge(x = PSC_Sales, y = PSC_Product_Details, by = "PRODUCT_NO", all.x = T)
toc()

# Add GM_Perc to PSC_Sales:
PSC_Sales$GM_Perc <- (PSC_Sales$EXT_SALES - PSC_Sales$EXT_COST_REB) / PSC_Sales$EXT_SALES

# OPTIONAL: Summary of Revenue contributions by each decile:
CUST_DECILE_Revs <- PSC_Sales %>% 
  filter(year(DATE) == 2018) %>% 
  group_by(CUST_DECILE) %>% 
  summarise(Revenue = sum(EXT_SALES, na.rm = T),
            n = n()) %>% 
  arrange(desc(Revenue))

# Separate Missouri (MO) and Illinois (IL) branches

PSC_Sales_MO <- PSC_Sales %>% filter(!SELL_BR %in% c(6, 10, 15))
PSC_Sales_IL <- PSC_Sales %>% filter(SELL_BR %in% c(6, 10, 15))

# Perform Price Structuring Analytics

## Weighted Average Price Structuring (v1 - group by CUST_DECILE first)
PSC_Sales <- PSC_Sales %>%
  group_by(CAT_NO, CUST_DECILE) %>% 
  mutate(weighted_GM1 = weighted_mean(GM_Perc, QTY))

## Weighted Average Price Structuring (v2 - perform weighting irrespective of CUST_DECILE)
PSC_Sales <- PSC_Sales %>%
  group_by(CAT_NO) %>% 
  mutate(weighted_GM2 = weighted_mean(GM_Perc, QTY))

## Best Customer Approach
PSC_orig <- PSC_Sales %>% select(CUST_DECILE, CAT_NO, QTY, GM_Perc)
PSC_agg <- aggregate(QTY ~ CUST_DECILE + CAT_NO, PSC_orig, max)
setDT(PSC_orig)
setDT(PSC_agg)
PSC_floor <- merge(PSC_agg, PSC_orig)
PSC_floor <- PSC_floor %>% group_by(CUST_DECILE, CAT_NO) %>% summarise(floorGM = max(GM_Perc))
temp_floor <- PSC_floor # store replica of PSC_floor to compare to after the following changes

tic("Bubbling floorGMs")
PSC_floor <- group_by(PSC_floor, CAT_NO)

PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))

while (any(PSC_floor$floorGM < PSC_floor$lag, na.rm = T)) {
  PSC_floor <- mutate(PSC_floor, floorGM = ifelse(!is.na(lag), ifelse(floorGM < lag, lag, floorGM), floorGM))
  PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))
}
toc()

names(PSC_floor)[3] <- "BCA_GM"
PSC_floor$lag <- NULL

setDT(PSC_floor)
setDT(PSC_Sales)
PSC_Sales <- merge(x = PSC_Sales, y = PSC_floor, by = c("CUST_DECILE", "CAT_NO"), all.x = T)

# Add in decile GMs, CAT_NO total rev, total QTY, total Costs, Break Even GM for reference

PSC_Sales <- PSC_Sales %>% 
  group_by(CAT_NO) %>% 
  mutate(minGM = min(GM_Perc, na.rm = T),
         maxGM = max(GM_Perc, na.rm = T),
         GM50 = quantile(GM_Perc, 0.50, na.rm = T),
         GM55 = quantile(GM_Perc, 0.55, na.rm = T),
         GM60 = quantile(GM_Perc, 0.60, na.rm = T),
         GM65 = quantile(GM_Perc, 0.65, na.rm = T),
         GM70 = quantile(GM_Perc, 0.70, na.rm = T),
         GM75 = quantile(GM_Perc, 0.75, na.rm = T),
         GM80 = quantile(GM_Perc, 0.80, na.rm = T),
         GM85 = quantile(GM_Perc, 0.85, na.rm = T),
         GM90 = quantile(GM_Perc, 0.90, na.rm = T),
         GM95 = quantile(GM_Perc, 0.95, na.rm = T),
         GM_range = maxGM - minGM,
         CAT_Revenue = sum(EXT_SALES, na.rm = T),
         CAT_Costs = sum(EXT_COST_REB, na.rm = T),
         CAT_QTY = sum(QTY, na.rm = T),
         BEven_GM = (CAT_Revenue - CAT_Costs) / CAT_Revenue) %>% 
  arrange(CAT_NO, CUST_DECILE)

# Summarize PSC_Sales dataframe at the CAT_NO, CUST_DECILE level

PSC_Sales1 <- PSC_Sales %>% 
  group_by(CAT_NO, CUST_DECILE) %>% 
  summarise(CAT_Revenue = mean(CAT_Revenue, na.rm = T),
            CAT_Costs = mean(CAT_Costs, na.rm = T),
            CAT_QTY = mean(CAT_QTY, na.rm = T),
            GM_range = mean(GM_range, na.rm = T),
            minGM = mean(minGM, na.rm = T),
            maxGM = mean(maxGM, na.rm = T),
            GM50 = mean(GM50, na.rm = T),
            GM55 = mean(GM55, na.rm = T),
            GM60 = mean(GM60, na.rm = T),
            GM65 = mean(GM65, na.rm = T),
            GM70 = mean(GM70, na.rm = T),
            GM75 = mean(GM75, na.rm = T),
            GM80 = mean(GM80, na.rm = T),
            GM85 = mean(GM85, na.rm = T),
            GM90 = mean(GM90, na.rm = T),
            GM95 = mean(GM95, na.rm = T),
            BCA_GM = mean(BCA_GM, na.rm = T),
            Weighted_GM1 = mean(weighted_GM1, na.rm = T),
            Weighted_GM2 = mean(weighted_GM2, na.rm = T),
            BEven_GM = mean(BEven_GM, na.rm = T))

# Determine Recommended GM target (output to be formally reviewed)

setDT(PSC_Sales1)
PSC_Sales1[maxGM - minGM <= 0.05 & maxGM > 0, Rec_GM := maxGM]
PSC_Sales1[is.na(Rec_GM) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_GM := BCA_GM]
PSC_Sales1[is.na(Rec_GM) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_GM := Weighted_GM1]
PSC_Sales1[is.na(Rec_GM) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_GM := Weighted_GM2]
PSC_Sales1[is.na(Rec_GM) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_GM := GM95]
PSC_Sales1[is.na(Rec_GM) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_GM := GM90]
PSC_Sales1[is.na(Rec_GM) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_GM := GM85]
PSC_Sales1[is.na(Rec_GM) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_GM := GM80]
PSC_Sales1[is.na(Rec_GM) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_GM := GM75]
PSC_Sales1[is.na(Rec_GM) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_GM := GM70]
PSC_Sales1[is.na(Rec_GM) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_GM := GM65]
PSC_Sales1[is.na(Rec_GM) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_GM := GM60]
PSC_Sales1[is.na(Rec_GM) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_GM := GM55]
PSC_Sales1[is.na(Rec_GM) & GM50 >= BEven_GM & GM50 > 0, Rec_GM := GM50]
PSC_Sales1[is.na(Rec_GM) & BEven_GM > 0, Rec_GM := BEven_GM]
PSC_Sales1[is.na(Rec_GM) & BEven_GM <= 0, Rec_GM := 0.05]

# Create a tracker to know which Recommended GM logic was used

PSC_Sales1[maxGM - minGM <= 0.05 & maxGM > 0, Rec_Logic := "maxGM"]
PSC_Sales1[is.na(Rec_Logic) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_Logic := "BCA_GM"]
PSC_Sales1[is.na(Rec_Logic) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_Logic := "Weighted_GM1"]
PSC_Sales1[is.na(Rec_Logic) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_Logic := "Weighted_GM2"]
PSC_Sales1[is.na(Rec_Logic) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_Logic := "GM95"]
PSC_Sales1[is.na(Rec_Logic) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_Logic := "GM90"]
PSC_Sales1[is.na(Rec_Logic) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_Logic := "GM85"]
PSC_Sales1[is.na(Rec_Logic) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_Logic := "GM80"]
PSC_Sales1[is.na(Rec_Logic) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_Logic := "GM75"]
PSC_Sales1[is.na(Rec_Logic) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_Logic := "GM70"]
PSC_Sales1[is.na(Rec_Logic) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_Logic := "GM65"]
PSC_Sales1[is.na(Rec_Logic) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_Logic := "GM60"]
PSC_Sales1[is.na(Rec_Logic) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_Logic := "GM55"]
PSC_Sales1[is.na(Rec_Logic) & GM50 >= BEven_GM & GM50 > 0, Rec_Logic := "GM50"]
PSC_Sales1[is.na(Rec_Logic) & BEven_GM > 0, Rec_Logic := "BEven_GM"]
PSC_Sales1[is.na(Rec_Logic) & BEven_GM <= 0, Rec_Logic := "Leftovers"]

tic("Bubbling Rec_GMs")
PSC_Sales1 <- PSC_Sales1 %>% group_by(CAT_NO) %>% arrange(CAT_NO, CUST_DECILE)

PSC_Sales1 <- mutate(PSC_Sales1, lag = lag(Rec_GM))

while (any(PSC_Sales1$Rec_GM < PSC_Sales1$lag, na.rm = T)) {
  PSC_Sales1 <- mutate(PSC_Sales1, Rec_GM = ifelse(!is.na(lag), ifelse(Rec_GM < lag, lag, Rec_GM), Rec_GM))
  PSC_Sales1 <- mutate(PSC_Sales1, lag = lag(Rec_GM))
}

PSC_Sales1$lag <- NULL

toc()

temp <- PSC_Sales1 %>% group_by(Rec_Logic) %>% summarise(n = n()) %>% arrange(desc(n))

# Create a new data frame that ensures every unique product in PSC_Sales has a Customer Decile
# Without doing this, we may have some CAT_NOs that only have a few customer deciles defined

PSC_Matrix <- expand(PSC_Sales1, CAT_NO, CUST_DECILE)
setDT(PSC_Matrix)
PSC_Matrix <- merge(x = PSC_Matrix, y = PSC_Sales1, 
                          by = c("CAT_NO", "CUST_DECILE"), all.x = T)

PSC_Matrix <- PSC_Matrix %>% arrange(CAT_NO, CUST_DECILE) %>% group_by(CAT_NO)

PSC_Matrix <- PSC_Matrix %>% mutate(lag = lag(Rec_GM),
                                    lead = lead(Rec_GM))

while (any(is.na(PSC_Matrix$Rec_GM))) {
  PSC_Matrix <- mutate(PSC_Matrix, Rec_GM = ifelse(!is.na(lag), lag, ifelse(!is.na(lead), lead, Rec_GM)))
  PSC_Matrix <- mutate(PSC_Matrix, lag = lag(Rec_GM), lead = lead(Rec_GM))
}

PSC_Matrix$lag <- NULL
PSC_Matrix$lead <- NULL

# Need logic to ensure that lower decile customers never receive better GM targets
setDT(PSC_Matrix)
PSC_Matrix[order(CAT_NO, CUST_DECILE)]
PSC_Matrix[Rec_GM < shift(Rec_GM, n = 1, type = "lag"), Rec_GM := shift(Rec_GM, n = 1, type = "lag"), by = CAT_NO]

######################################################################

# Perform Price Structuring Analytics - Missouri Sales

## Weighted Average Price Structuring (v1 - group by CUST_DECILE first)
PSC_Sales_MO <- PSC_Sales_MO %>%
  group_by(CAT_NO, CUST_DECILE) %>% 
  mutate(weighted_GM1 = weighted_mean(GM_Perc, QTY))

## Weighted Average Price Structuring (v2 - perform weighting irrespective of CUST_DECILE)
PSC_Sales_MO <- PSC_Sales_MO %>%
  group_by(CAT_NO) %>% 
  mutate(weighted_GM2 = weighted_mean(GM_Perc, QTY))

## Best Customer Approach
PSC_orig <- PSC_Sales_MO %>% select(CUST_DECILE, CAT_NO, QTY, GM_Perc)
PSC_agg <- aggregate(QTY ~ CUST_DECILE + CAT_NO, PSC_orig, max)
setDT(PSC_orig)
setDT(PSC_agg)
PSC_floor <- merge(PSC_agg, PSC_orig)
PSC_floor <- PSC_floor %>% group_by(CUST_DECILE, CAT_NO) %>% summarise(floorGM = max(GM_Perc))
temp_floor <- PSC_floor # store replica of PSC_floor to compare to after the following changes

tic("Bubbling floorGMs")
PSC_floor <- group_by(PSC_floor, CAT_NO)

PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))

while (any(PSC_floor$floorGM < PSC_floor$lag, na.rm = T)) {
  PSC_floor <- mutate(PSC_floor, floorGM = ifelse(!is.na(lag), ifelse(floorGM < lag, lag, floorGM), floorGM))
  PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))
}
toc()

names(PSC_floor)[3] <- "BCA_GM"
PSC_floor$lag <- NULL

setDT(PSC_floor)
setDT(PSC_Sales_MO)
PSC_Sales_MO <- merge(x = PSC_Sales_MO, y = PSC_floor, by = c("CUST_DECILE", "CAT_NO"), all.x = T)

# Add in decile GMs for reference

PSC_Sales_MO <- PSC_Sales_MO %>% 
  group_by(CAT_NO) %>% 
  mutate(minGM = min(GM_Perc, na.rm = T),
         maxGM = max(GM_Perc, na.rm = T),
         GM50 = quantile(GM_Perc, 0.50, na.rm = T),
         GM55 = quantile(GM_Perc, 0.55, na.rm = T),
         GM60 = quantile(GM_Perc, 0.60, na.rm = T),
         GM65 = quantile(GM_Perc, 0.65, na.rm = T),
         GM70 = quantile(GM_Perc, 0.70, na.rm = T),
         GM75 = quantile(GM_Perc, 0.75, na.rm = T),
         GM80 = quantile(GM_Perc, 0.80, na.rm = T),
         GM85 = quantile(GM_Perc, 0.85, na.rm = T),
         GM90 = quantile(GM_Perc, 0.90, na.rm = T),
         GM95 = quantile(GM_Perc, 0.95, na.rm = T),
         GM_range = maxGM - minGM,
         CAT_Revenue = sum(EXT_SALES, na.rm = T),
         CAT_Costs = sum(EXT_COST_REB, na.rm = T),
         CAT_QTY = sum(QTY, na.rm = T),
         BEven_GM = (CAT_Revenue - CAT_Costs) / CAT_Revenue)

PSC_Sales1_MO <- PSC_Sales_MO %>% 
  group_by(CAT_NO, CUST_DECILE) %>% 
  summarise(CAT_Revenue = mean(CAT_Revenue, na.rm = T),
            CAT_Costs = mean(CAT_Costs, na.rm = T),
            CAT_QTY = mean(CAT_QTY, na.rm = T),
            GM_range = mean(GM_range, na.rm = T),
            minGM = mean(minGM, na.rm = T),
            maxGM = mean(maxGM, na.rm = T),
            GM50 = mean(GM50, na.rm = T),
            GM55 = mean(GM55, na.rm = T),
            GM60 = mean(GM60, na.rm = T),
            GM65 = mean(GM65, na.rm = T),
            GM70 = mean(GM70, na.rm = T),
            GM75 = mean(GM75, na.rm = T),
            GM80 = mean(GM80, na.rm = T),
            GM85 = mean(GM85, na.rm = T),
            GM90 = mean(GM90, na.rm = T),
            GM95 = mean(GM95, na.rm = T),
            BCA_GM = mean(BCA_GM, na.rm = T),
            Weighted_GM1 = mean(weighted_GM1, na.rm = T),
            Weighted_GM2 = mean(weighted_GM2, na.rm = T),
            BEven_GM = mean(BEven_GM, na.rm = T))

# Determine Recommended GM target (output to be formally reviewed)

setDT(PSC_Sales1_MO)
PSC_Sales1_MO[maxGM - minGM <= 0.05 & maxGM > 0, Rec_GM := maxGM]
PSC_Sales1_MO[is.na(Rec_GM) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_GM := BCA_GM]
PSC_Sales1_MO[is.na(Rec_GM) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_GM := Weighted_GM1]
PSC_Sales1_MO[is.na(Rec_GM) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_GM := Weighted_GM2]
PSC_Sales1_MO[is.na(Rec_GM) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_GM := GM95]
PSC_Sales1_MO[is.na(Rec_GM) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_GM := GM90]
PSC_Sales1_MO[is.na(Rec_GM) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_GM := GM85]
PSC_Sales1_MO[is.na(Rec_GM) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_GM := GM80]
PSC_Sales1_MO[is.na(Rec_GM) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_GM := GM75]
PSC_Sales1_MO[is.na(Rec_GM) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_GM := GM70]
PSC_Sales1_MO[is.na(Rec_GM) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_GM := GM65]
PSC_Sales1_MO[is.na(Rec_GM) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_GM := GM60]
PSC_Sales1_MO[is.na(Rec_GM) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_GM := GM55]
PSC_Sales1_MO[is.na(Rec_GM) & GM50 >= BEven_GM & GM50 > 0, Rec_GM := GM50]
PSC_Sales1_MO[is.na(Rec_GM) & BEven_GM > 0, Rec_GM := BEven_GM]
PSC_Sales1_MO[is.na(Rec_GM) & BEven_GM <= 0, Rec_GM := 0.05]

# Create a tracker to know which Recommended GM logic was used

PSC_Sales1_MO[maxGM - minGM <= 0.05 & maxGM > 0, Rec_Logic := "maxGM"]
PSC_Sales1_MO[is.na(Rec_Logic) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_Logic := "BCA_GM"]
PSC_Sales1_MO[is.na(Rec_Logic) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_Logic := "Weighted_GM1"]
PSC_Sales1_MO[is.na(Rec_Logic) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_Logic := "Weighted_GM2"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_Logic := "GM95"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_Logic := "GM90"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_Logic := "GM85"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_Logic := "GM80"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_Logic := "GM75"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_Logic := "GM70"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_Logic := "GM65"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_Logic := "GM60"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_Logic := "GM55"]
PSC_Sales1_MO[is.na(Rec_Logic) & GM50 >= BEven_GM & GM50 > 0, Rec_Logic := "GM50"]
PSC_Sales1_MO[is.na(Rec_Logic) & BEven_GM > 0, Rec_Logic := "BEven_GM"]
PSC_Sales1_MO[is.na(Rec_Logic) & BEven_GM <= 0, Rec_Logic := "Leftovers"]

tic("Bubbling Rec_GMs - MO")
PSC_Sales1_MO <- PSC_Sales1_MO %>% group_by(CAT_NO) %>% arrange(CAT_NO, CUST_DECILE)

PSC_Sales1_MO <- mutate(PSC_Sales1_MO, lag = lag(Rec_GM))

while (any(PSC_Sales1_MO$Rec_GM < PSC_Sales1_MO$lag, na.rm = T)) {
  PSC_Sales1_MO <- mutate(PSC_Sales1_MO, Rec_GM = ifelse(!is.na(lag), ifelse(Rec_GM < lag, lag, Rec_GM), Rec_GM))
  PSC_Sales1_MO <- mutate(PSC_Sales1_MO, lag = lag(Rec_GM))
}

PSC_Sales1_MO$lag <- NULL

toc()

temp <- PSC_Sales1_MO %>% group_by(Rec_Logic) %>% summarise(n = n()) %>% arrange(desc(n))

# Create a new data frame that ensures every unique product in PSC_Sales has a Customer Decile
# Without doing this, we may have some CAT_NOs that only have a few customer deciles defined

tic("Create Full PSC Price Matrix - MO")
PSC_Matrix_MO <- expand(PSC_Sales1_MO, CAT_NO, CUST_DECILE)
setDT(PSC_Matrix_MO)
PSC_Matrix_MO <- merge(x = PSC_Matrix_MO, y = PSC_Sales1_MO, 
                    by = c("CAT_NO", "CUST_DECILE"), all.x = T)

PSC_Matrix_MO <- PSC_Matrix_MO %>% arrange(CAT_NO, CUST_DECILE) %>% group_by(CAT_NO)

PSC_Matrix_MO <- PSC_Matrix_MO %>% mutate(lag = lag(Rec_GM),
                                    lead = lead(Rec_GM))

while (any(is.na(PSC_Matrix_MO$Rec_GM))) {
  PSC_Matrix_MO <- mutate(PSC_Matrix_MO, Rec_GM = ifelse(!is.na(lag), lag, ifelse(!is.na(lead), lead, Rec_GM)))
  PSC_Matrix_MO <- mutate(PSC_Matrix_MO, lag = lag(Rec_GM), lead = lead(Rec_GM))
}

PSC_Matrix_MO$lag <- NULL
PSC_Matrix_MO$lead <- NULL

toc()

######################################################################

# Perform Price Structuring Analytics - Illinois Sales

## Weighted Average Price Structuring (v1 - group by CUST_DECILE first)
PSC_Sales_IL <- PSC_Sales_IL %>%
  group_by(CAT_NO, CUST_DECILE) %>% 
  mutate(weighted_GM1 = weighted_mean(GM_Perc, QTY))

## Weighted Average Price Structuring (v2 - perform weighting irrespective of CUST_DECILE)
PSC_Sales_IL <- PSC_Sales_IL %>%
  group_by(CAT_NO) %>% 
  mutate(weighted_GM2 = weighted_mean(GM_Perc, QTY))

## Best Customer Approach
PSC_orig <- PSC_Sales_IL %>% select(CUST_DECILE, CAT_NO, QTY, GM_Perc)
PSC_agg <- aggregate(QTY ~ CUST_DECILE + CAT_NO, PSC_orig, max)
setDT(PSC_orig)
setDT(PSC_agg)
PSC_floor <- merge(PSC_agg, PSC_orig)
PSC_floor <- PSC_floor %>% group_by(CUST_DECILE, CAT_NO) %>% summarise(floorGM = max(GM_Perc))
temp_floor <- PSC_floor # store replica of PSC_floor to compare to after the following changes

tic("Bubbling floorGMs")
PSC_floor <- group_by(PSC_floor, CAT_NO)

PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))

while (any(PSC_floor$floorGM < PSC_floor$lag, na.rm = T)) {
  PSC_floor <- mutate(PSC_floor, floorGM = ifelse(!is.na(lag), ifelse(floorGM < lag, lag, floorGM), floorGM))
  PSC_floor <- mutate(PSC_floor, lag = lag(floorGM))
}
toc()

names(PSC_floor)[3] <- "BCA_GM"
PSC_floor$lag <- NULL

setDT(PSC_floor)
setDT(PSC_Sales_IL)
PSC_Sales_IL <- merge(x = PSC_Sales_IL, y = PSC_floor, by = c("CUST_DECILE", "CAT_NO"), all.x = T)

# Add in decile GMs for reference

PSC_Sales_IL <- PSC_Sales_IL %>% 
  group_by(CAT_NO) %>% 
  mutate(minGM = min(GM_Perc, na.rm = T),
         maxGM = max(GM_Perc, na.rm = T),
         GM50 = quantile(GM_Perc, 0.50, na.rm = T),
         GM55 = quantile(GM_Perc, 0.55, na.rm = T),
         GM60 = quantile(GM_Perc, 0.60, na.rm = T),
         GM65 = quantile(GM_Perc, 0.65, na.rm = T),
         GM70 = quantile(GM_Perc, 0.70, na.rm = T),
         GM75 = quantile(GM_Perc, 0.75, na.rm = T),
         GM80 = quantile(GM_Perc, 0.80, na.rm = T),
         GM85 = quantile(GM_Perc, 0.85, na.rm = T),
         GM90 = quantile(GM_Perc, 0.90, na.rm = T),
         GM95 = quantile(GM_Perc, 0.95, na.rm = T),
         GM_range = maxGM - minGM,
         CAT_Revenue = sum(EXT_SALES, na.rm = T),
         CAT_Costs = sum(EXT_COST_REB, na.rm = T),
         CAT_QTY = sum(QTY, na.rm = T),
         BEven_GM = (CAT_Revenue - CAT_Costs) / CAT_Revenue)

PSC_Sales1_IL <- PSC_Sales_IL %>% 
  group_by(CAT_NO, CUST_DECILE) %>% 
  summarise(CAT_Revenue = mean(CAT_Revenue, na.rm = T),
            CAT_Costs = mean(CAT_Costs, na.rm = T),
            CAT_QTY = mean(CAT_QTY, na.rm = T),
            GM_range = mean(GM_range, na.rm = T),
            minGM = mean(minGM, na.rm = T),
            maxGM = mean(maxGM, na.rm = T),
            GM50 = mean(GM50, na.rm = T),
            GM55 = mean(GM55, na.rm = T),
            GM60 = mean(GM60, na.rm = T),
            GM65 = mean(GM65, na.rm = T),
            GM70 = mean(GM70, na.rm = T),
            GM75 = mean(GM75, na.rm = T),
            GM80 = mean(GM80, na.rm = T),
            GM85 = mean(GM85, na.rm = T),
            GM90 = mean(GM90, na.rm = T),
            GM95 = mean(GM95, na.rm = T),
            BCA_GM = mean(BCA_GM, na.rm = T),
            Weighted_GM1 = mean(weighted_GM1, na.rm = T),
            Weighted_GM2 = mean(weighted_GM2, na.rm = T),
            BEven_GM = mean(BEven_GM, na.rm = T))

# Determine Recommended GM target (output to be formally reviewed)

setDT(PSC_Sales1_IL)
PSC_Sales1_IL[maxGM - minGM <= 0.05 & maxGM > 0, Rec_GM := maxGM]
PSC_Sales1_IL[is.na(Rec_GM) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_GM := BCA_GM]
PSC_Sales1_IL[is.na(Rec_GM) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_GM := Weighted_GM1]
PSC_Sales1_IL[is.na(Rec_GM) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_GM := Weighted_GM2]
PSC_Sales1_IL[is.na(Rec_GM) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_GM := GM95]
PSC_Sales1_IL[is.na(Rec_GM) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_GM := GM90]
PSC_Sales1_IL[is.na(Rec_GM) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_GM := GM85]
PSC_Sales1_IL[is.na(Rec_GM) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_GM := GM80]
PSC_Sales1_IL[is.na(Rec_GM) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_GM := GM75]
PSC_Sales1_IL[is.na(Rec_GM) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_GM := GM70]
PSC_Sales1_IL[is.na(Rec_GM) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_GM := GM65]
PSC_Sales1_IL[is.na(Rec_GM) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_GM := GM60]
PSC_Sales1_IL[is.na(Rec_GM) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_GM := GM55]
PSC_Sales1_IL[is.na(Rec_GM) & GM50 >= BEven_GM & GM50 > 0, Rec_GM := GM50]
PSC_Sales1_IL[is.na(Rec_GM) & BEven_GM > 0, Rec_GM := BEven_GM]
PSC_Sales1_IL[is.na(Rec_GM) & BEven_GM <= 0, Rec_GM := 0.05]

# Create a tracker to know which Recommended GM logic was used

PSC_Sales1_IL[maxGM - minGM <= 0.05 & maxGM > 0, Rec_Logic := "maxGM"]
PSC_Sales1_IL[is.na(Rec_Logic) & BCA_GM >= Weighted_GM1 & BCA_GM >= Weighted_GM2 & BCA_GM >= GM50 & BCA_GM >= BEven_GM & BCA_GM > 0 | BCA_GM >= 1, Rec_Logic := "BCA_GM"]
PSC_Sales1_IL[is.na(Rec_Logic) & Weighted_GM1 >= BCA_GM & Weighted_GM1 >= Weighted_GM2 & Weighted_GM1 >= GM50 & Weighted_GM1 >= BEven_GM & Weighted_GM1 > 0, Rec_Logic := "Weighted_GM1"]
PSC_Sales1_IL[is.na(Rec_Logic) & Weighted_GM2 >= BCA_GM & Weighted_GM2 >= Weighted_GM1 & Weighted_GM2 >= GM50 & Weighted_GM2 >= BEven_GM & Weighted_GM2 > 0, Rec_Logic := "Weighted_GM2"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM95 - GM50 <= 0.05 & GM95 >= BEven_GM & GM95 > 0, Rec_Logic := "GM95"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM90 - GM50 <= 0.05 & GM90 >= BEven_GM & GM90 > 0, Rec_Logic := "GM90"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM85 - GM50 <= 0.06 & GM85 >= BEven_GM & GM85 > 0, Rec_Logic := "GM85"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM80 - GM50 <= 0.06 & GM80 >= BEven_GM & GM80 > 0, Rec_Logic := "GM80"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM75 - GM50 <= 0.07 & GM75 >= BEven_GM & GM75 > 0, Rec_Logic := "GM75"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM70 - GM50 <= 0.07 & GM70 >= BEven_GM & GM70 > 0, Rec_Logic := "GM70"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM65 - GM50 <= 0.08 & GM65 >= BEven_GM & GM65 > 0, Rec_Logic := "GM65"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM60 - GM50 <= 0.09 & GM60 >= BEven_GM & GM60 > 0, Rec_Logic := "GM60"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM55 - GM50 <= 0.10 & GM55 >= BEven_GM & GM55 > 0, Rec_Logic := "GM55"]
PSC_Sales1_IL[is.na(Rec_Logic) & GM50 >= BEven_GM & GM50 > 0, Rec_Logic := "GM50"]
PSC_Sales1_IL[is.na(Rec_Logic) & BEven_GM > 0, Rec_Logic := "BEven_GM"]
PSC_Sales1_IL[is.na(Rec_Logic) & BEven_GM <= 0, Rec_Logic := "Leftovers"]

tic("Bubbling Rec_GMs - IL")
PSC_Sales1_IL <- PSC_Sales1_IL %>% group_by(CAT_NO) %>% arrange(CAT_NO, CUST_DECILE)

PSC_Sales1_IL <- mutate(PSC_Sales1_IL, lag = lag(Rec_GM))

while (any(PSC_Sales1_IL$Rec_GM < PSC_Sales1_IL$lag, na.rm = T)) {
  PSC_Sales1_IL <- mutate(PSC_Sales1_IL, Rec_GM = ifelse(!is.na(lag), ifelse(Rec_GM < lag, lag, Rec_GM), Rec_GM))
  PSC_Sales1_IL <- mutate(PSC_Sales1_IL, lag = lag(Rec_GM))
}

PSC_Sales1_IL$lag <- NULL

toc()

temp <- PSC_Sales1_IL %>% group_by(Rec_Logic) %>% summarise(n = n()) %>% arrange(desc(n))

# Create a new data frame that ensures every unique product in PSC_Sales has a Customer Decile
# Without doing this, we may have some CAT_NOs that only have a few customer deciles defined

tic("Create Full PSC Price Matrix - IL")
PSC_Matrix_IL <- expand(PSC_Sales1_IL, CAT_NO, CUST_DECILE)
setDT(PSC_Matrix_IL)
PSC_Matrix_IL <- merge(x = PSC_Matrix_IL, y = PSC_Sales1_IL, 
                       by = c("CAT_NO", "CUST_DECILE"), all.x = T)

PSC_Matrix_IL <- PSC_Matrix_IL %>% arrange(CAT_NO, CUST_DECILE) %>% group_by(CAT_NO)

PSC_Matrix_IL <- PSC_Matrix_IL %>% mutate(lag = lag(Rec_GM),
                                          lead = lead(Rec_GM))

while (any(is.na(PSC_Matrix_IL$Rec_GM))) {
  PSC_Matrix_IL <- mutate(PSC_Matrix_IL, Rec_GM = ifelse(!is.na(lag), lag, ifelse(!is.na(lead), lead, Rec_GM)))
  PSC_Matrix_IL <- mutate(PSC_Matrix_IL, lag = lag(Rec_GM), lead = lead(Rec_GM))
}

PSC_Matrix_IL$lag <- NULL
PSC_Matrix_IL$lead <- NULL

toc()

# EXPORT

export(PSC_Sales1, "C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/Pricing_Outputs.csv")
export(PSC_Sales1_MO, "C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/Pricing_Outputs_MO.csv")
export(PSC_Sales1_IL, "C:/Users/danco/Documents/FirstDiscovery/Customers/PSC/Pricing_Outputs_IL.csv")

# What-if Analysis
WhatIf <- PSC_Sales %>% filter(year(DATE) == 2018)

setDT(PSC_Sales)
setDT(PSC_Sales1)
WhatIf <- merge(PSC_Sales, PSC_Sales1[, c("CAT_NO", "CUST_DECILE", "Rec_GM")], by = c("CAT_NO", "CUST_DECILE"), all.x = T)
WhatIf$Target_Rev <- WhatIf$EXT_COST_REB / (1 - WhatIf$Rec_GM)
WhatIf$Rev_Impact <- WhatIf$Target_Rev - WhatIf$EXT_SALES


# Create a best case scenario (Rec_GM), a likely scenario (RAND with the mean around 8%), and worse case (using highest discount for that customer decile)
# This will involve using the Rec_GM and Costs to calculate Revenue, then divide by QTY to get Price, then apply discounting to the price, then working back to the new revenue and comparing the impact



############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################
############################################################################################

# OLD CODE - GRAVEYARD

# Filter dataset again to remove extreme GM outliers
PSC_GM_Outliers <- PSC_Sales_Grouped %>% filter(GM <= 0 | GM_Perc >= 100) # 4,637 extreme Gross Margin outliers. These may want to be examined at a later time
PSC_Sales_Grouped <- PSC_Sales_Grouped %>% filter(GM > 0, GM_Perc < 100) # removes extreme Gross Margin outliers prior to regression

# OPTIONAL: May not want to do this for all Price Structuring approaches
# Remove GM % outliers, i.e., those above or below 3 Standard Deviations
PSC_Sales_Grouped <- data.table(PSC_Sales_Grouped)
PSC_Sales_Grouped <- PSC_Sales_Grouped[ , ToKeep := abs(GM_Perc - mean(GM_Perc)) <= 3*sd(GM_Perc), by = PRODUCT_NO][ToKeep == T]

## Regression at Product CAT (category) level
PSC_CAT_Reg <- PSC_CAT_Grouped %>% 
  group_by(CAT_NO, CUST_DECILE) %>% 
  do(regression = lm(GM_Perc ~ Revenue, data = .)) %>% 
  rowwise() %>% 
  tidy(regression)

## Spread modeled data to isolate two primary coeffiences (y-intercept and trend slope)
PSC_CAT_Reg.wide <- PSC_CAT_Reg %>% select(CAT_NO, term, estimate) %>% spread(term, estimate)

## Rename cols for clear interpretation and to avoid duplicate column names after merge
names(PSC_CAT_Reg.wide)[2] <- "Intercept"
names(PSC_CAT_Reg.wide)[3] <- "Slope"

## Merge regression coefficients with grouped Product CAT data
PSC_CAT_Grouped <- merge(x = PSC_CAT_Grouped, y = PSC_CAT_Reg.wide, by = "CAT_NO", all.x = T)

## Calculate the Regression GM: $GM_Reg
PSC_CAT_Grouped$GM_Reg <- (PSC_CAT_Grouped$Revenue * PSC_CAT_Grouped$Slope) + PSC_CAT_Grouped$Intercept


## Calculate GM Delta of Regression - Actual
PSC_CAT_Grouped$GM_Delta <- PSC_CAT_Grouped$GM_Reg - PSC_CAT_Grouped$GM_Perc

## Calculate Revenue Uplift
PSC_CAT_Grouped$Rev_Uplift <- if_else(((PSC_CAT_Grouped$Costs / (1 - PSC_CAT_Grouped$GM_Reg / 100)) - PSC_CAT_Grouped$Revenue) > 0, ((PSC_CAT_Grouped$Costs / (1 - PSC_CAT_Grouped$GM_Reg / 100)) - PSC_CAT_Grouped$Revenue), 0)

# Remove 'ToKeep' column
PSC_CAT_Grouped <- PSC_CAT_Grouped %>% select(-ToKeep)


######################################################################################################

# Remove unneeded columns for time being. Note that these can always be brough back in if needed for future analyses
# Filter to only include data points necessary for analysis
PSC_Sales_Small <- PSC_Sales_Full %>% 
  select(CUST_NO, PRODUCT_NO, QTY, EXT_COST, EXT_SALES, TYPE, DATE) %>% 
  filter(year(DATE) >= 2017, !grepl("LABOR|labor|Labor", PRODUCT_NO), !grepl("9999", CUST_NO), EXT_SALES > 0, TYPE == "SLS")

# Group Data for Regression Analysis
PSC_Sales_Grouped <- PSC_Sales_Small %>% 
  group_by(PRODUCT_NO, CUST_NO) %>% 
  summarise(Revenue = sum(EXT_SALES, na.rm = T),
            Costs = sum(EXT_COST, na.rm = T),
            QTY = sum(QTY, na.rm = T),
            GM = Revenue - Costs, # Creates the Gross Margin calculation
            GM_Perc = (GM / Revenue) * 100) # Creates the Gross Margin % calculation

# Filter dataset again to remove extreme GM outliers
PSC_GM_Outliers <- PSC_Sales_Grouped %>% filter(GM <= 0 | GM_Perc >= 100) # 4,637 extreme Gross Margin outliers. These may want to be examined at a later time
PSC_Sales_Grouped <- PSC_Sales_Grouped %>% filter(GM > 0, GM_Perc < 100) # removes extreme Gross Margin outliers prior to regression

# Remove GM % outliers, i.e., those above or below 3 Standard Deviations
PSC_Sales_Grouped <- data.table(PSC_Sales_Grouped)
PSC_Sales_Grouped <- PSC_Sales_Grouped[ , ToKeep := abs(GM_Perc - mean(GM_Perc)) <= 3*sd(GM_Perc), by = PRODUCT_NO][ToKeep == T]


## Regression at Product-Customer level
PSC_Sales_Reg <- PSC_Sales_Grouped %>% 
  group_by(PRODUCT_NO) %>% 
  do(regression = lm(GM_Perc ~ Revenue, data = .)) %>% 
  rowwise() %>% 
  tidy(regression)

## Spread modeled data to isolate two primary coeffiences (y-intercept and trend slope)
PSC_Sales_Reg.wide <- PSC_Sales_Reg %>% select(PRODUCT_NO, term, estimate) %>% spread(term, estimate)

## Rename cols for clear interpretation and to avoid duplicate column names after merge
names(PSC_Sales_Reg.wide)[2] <- "Intercept"
names(PSC_Sales_Reg.wide)[3] <- "Slope"

## Merge regression coefficients with grouped sales data
PSC_Sales_Grouped <- merge(x = PSC_Sales_Grouped, y = PSC_Sales_Reg.wide, by = "PRODUCT_NO", all.x = T)

## Calculate the Regression GM: $GM_Reg
PSC_Sales_Grouped$GM_Reg <- (PSC_Sales_Grouped$Revenue * PSC_Sales_Grouped$Slope) + PSC_Sales_Grouped$Intercept


## Calculate GM Delta of Regression - Actual
PSC_Sales_Grouped$GM_Delta <- PSC_Sales_Grouped$GM_Reg - PSC_Sales_Grouped$GM_Perc

## Calculate Revenue Uplift
PSC_Sales_Grouped$Rev_Uplift <- if_else(((PSC_Sales_Grouped$Costs / (1 - PSC_Sales_Grouped$GM_Reg / 100)) - PSC_Sales_Grouped$Revenue) > 0, ((PSC_Sales_Grouped$Costs / (1 - PSC_Sales_Grouped$GM_Reg / 100)) - PSC_Sales_Grouped$Revenue), 0)

# Remove 'ToKeep' column
PSC_Sales_Grouped <- PSC_Sales_Grouped %>% select(-ToKeep)

# Export data 

## Make sure you pre-save a blank txt file prior to running this next command
## as the file.choose() command will prompt you to choose a file to save to
write.table(PSC_Sales_Grouped, file.choose(), sep = "|", row.names = F)


##########################################################################################################################################

# Product Category Regression:

# Load Data
PSC_Product_Details <- read.csv(file.choose(), sep = "|")

names(PSC_Product_Details)[1] <- "PRODUCT_NO"

PSC_Product_Details$PRODUCT_NO <- as.character(PSC_Product_Details$PRODUCT_NO)
PSC_Product_Details$CAT_NO <- as.character(PSC_Product_Details$CAT_NO)

PSC_Sales_Small <- merge(x = PSC_Sales_Small, y = PSC_Product_Details[ , c("PRODUCT_NO", "CAT_NO")], by = "PRODUCT_NO", all.x = T)

# Group Data for Regression Analysis
PSC_CAT_Grouped <- PSC_Sales_Small %>% 
  group_by(CAT_NO, CUST_NO) %>% 
  summarise(Revenue = sum(EXT_SALES, na.rm = T),
            Costs = sum(EXT_COST, na.rm = T),
            QTY = sum(QTY, na.rm = T),
            GM = Revenue - Costs, # Creates the Gross Margin calculation
            GM_Perc = (GM / Revenue) * 100) # Creates the Gross Margin % calculation

# Filter dataset again to remove extreme GM outliers
PSC_CAT_Outliers <- PSC_CAT_Grouped %>% filter(GM <= 0 | GM_Perc >= 100)
PSC_CAT_Grouped <- PSC_CAT_Grouped %>% filter(GM > 0, GM_Perc < 100) # removes extreme Gross Margin outliers prior to regression

# Remove GM % outliers, i.e., those above or below 3 Standard Deviations
PSC_CAT_Grouped <- data.table(PSC_CAT_Grouped)
PSC_CAT_Grouped <- PSC_CAT_Grouped[ , ToKeep := abs(GM_Perc - mean(GM_Perc)) <= 3*sd(GM_Perc), by = CAT_NO][ToKeep == T]


## Regression at Product CAT (category) level
PSC_CAT_Reg <- PSC_CAT_Grouped %>% 
  group_by(CAT_NO) %>% 
  do(regression = lm(GM_Perc ~ Revenue, data = .)) %>% 
  rowwise() %>% 
  tidy(regression)

## Spread modeled data to isolate two primary coeffiences (y-intercept and trend slope)
PSC_CAT_Reg.wide <- PSC_CAT_Reg %>% select(CAT_NO, term, estimate) %>% spread(term, estimate)

## Rename cols for clear interpretation and to avoid duplicate column names after merge
names(PSC_CAT_Reg.wide)[2] <- "Intercept"
names(PSC_CAT_Reg.wide)[3] <- "Slope"

## Merge regression coefficients with grouped Product CAT data
PSC_CAT_Grouped <- merge(x = PSC_CAT_Grouped, y = PSC_CAT_Reg.wide, by = "CAT_NO", all.x = T)

## Calculate the Regression GM: $GM_Reg
PSC_CAT_Grouped$GM_Reg <- (PSC_CAT_Grouped$Revenue * PSC_CAT_Grouped$Slope) + PSC_CAT_Grouped$Intercept


## Calculate GM Delta of Regression - Actual
PSC_CAT_Grouped$GM_Delta <- PSC_CAT_Grouped$GM_Reg - PSC_CAT_Grouped$GM_Perc

## Calculate Revenue Uplift
PSC_CAT_Grouped$Rev_Uplift <- if_else(((PSC_CAT_Grouped$Costs / (1 - PSC_CAT_Grouped$GM_Reg / 100)) - PSC_CAT_Grouped$Revenue) > 0, ((PSC_CAT_Grouped$Costs / (1 - PSC_CAT_Grouped$GM_Reg / 100)) - PSC_CAT_Grouped$Revenue), 0)

# Remove 'ToKeep' column
PSC_CAT_Grouped <- PSC_CAT_Grouped %>% select(-ToKeep)

# Export data 

## Make sure you pre-save a blank txt file prior to running this next command
## as the file.choose() command will prompt you to choose a file to save to
write.table(PSC_CAT_Grouped, file.choose(), sep = "|", row.names = F)

#################################################################################################################################
# Sample Price Structuring - BCA

write.csv(CUST_DECILE_Revs, "C:/Users/combsd/Desktop/Personal/Upwork/Plumbing Pricing/PSC_Customer_Deciles.csv", row.names = F)

# Create GM_Perc
PSC_Sales$GM_Perc <- (PSC_Sales$EXT_SALES - PSC_Sales$EXT_COST_REB) / PSC_Sales$EXT_SALES

# Create demow with TOTC454CUFGW
PSC_Demo <- PSC_Sales %>% 
  filter(PRODUCT_NO == "TOTC454CUFGW", year(DATE) == 2018, EXT_SALES > 0, GM_Perc > 0) %>% 
  select(-SLS_NO, -EXT_AVG_COST, -ORDER., -TYPE, -ORD_TYP, -SV_CODE, -REQ_DATE, -FRTCOST, -ZIP, - WRT_BY, -SBR, -MM, -YY)

# Quickly plot conceptual view of what will be done
ggplot(PSC_Demo, aes(x = QTY, y = GM_Perc)) +
  geom_point(aes(size = EXT_SALES, col = CUST_DECILE)) +
  facet_wrap(~ CUST_DECILE, nrow = 4) +
  theme_classic() +
  labs(x = "Quantity (Units Sold)",
       y = "Gross Margin % of Sale",
       title = "Gross Margin Dispersion by Customer Deciles",
       subtitle = "Product No. TOTC454CUFGW")


# anyone with a CUST_RANK < 0.9225118 contributed less than $20K of business from Jan 2017 - May 2018

# Floor GMs
PSC_orig <- PSC_Demo %>% select(CUST_DECILE, QTY, GM_Perc)
PSC_agg <- aggregate(QTY ~ CUST_DECILE, PSC_orig, max)
PSC_floor <- merge(PSC_agg, PSC_orig)
PSC_floor <- PSC_floor %>% group_by(CUST_DECILE) %>% summarise(floorGM = max(GM_Perc))
temp_floor <- PSC_floor # store replica of PSC_floor to compare to after the following changes

if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 1", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 2", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 2", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 1", "floorGM"]
}
if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 2", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 3", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 3", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 2", "floorGM"]
}
if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 3", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 4", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 4", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 3", "floorGM"]
}
if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 4", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 5", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 5", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 4", "floorGM"]
}
if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 5", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 6", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 6", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 5", "floorGM"]
}
if(PSC_floor[PSC_floor$CUST_DECILE == "Decile 6", "floorGM"] > PSC_floor[PSC_floor$CUST_DECILE == "Decile 7", "floorGM"]) {
  PSC_floor[PSC_floor$CUST_DECILE == "Decile 7", "floorGM"] <- PSC_floor[PSC_floor$CUST_DECILE == "Decile 6", "floorGM"]
}

# Compare changes in price structuring of Floor GMs
temp_floor
PSC_floor

# Ceiling GMs
PSC_ceiling <- data.table(PSC_Demo)
PSC_ceiling <- PSC_ceiling[ , ToKeep := abs(GM_Perc - mean(GM_Perc)) <= 3*sd(GM_Perc), by = CUST_DECILE][ToKeep == T]
PSC_ceiling <- PSC_ceiling[ , ToKeep := GM_Perc == max(GM_Perc, na.rm = T), by = CUST_DECILE][ToKeep == T]
PSC_ceiling <- PSC_ceiling %>% select(CUST_DECILE, GM_Perc) %>% arrange(CUST_DECILE)
PSC_ceiling <- PSC_ceiling %>% group_by(CUST_DECILE) %>% summarise(targetGM = max(GM_Perc, na.rm = T))
temp_ceiling <- PSC_ceiling

if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 1", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 2", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 2", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 1", "targetGM"]
}
if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 2", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 3", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 3", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 2", "targetGM"]
}
if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 3", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 4", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 4", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 3", "targetGM"]
}
if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 4", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 5", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 5", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 4", "targetGM"]
}
if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 5", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 6", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 6", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 5", "targetGM"]
}
if(PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 6", "targetGM"] > PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 7", "targetGM"]) {
  PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 7", "targetGM"] <- PSC_ceiling[PSC_ceiling$CUST_DECILE == "Decile 6", "targetGM"]
}

temp_ceiling
PSC_ceiling

# Merge back Floor and Ceiling GMs
PSC_Demo <- merge(PSC_Demo, PSC_floor, all.x = T)
PSC_Demo <- merge(PSC_Demo, PSC_ceiling, all.x = T)

# Calculate what-if scenarios using the new Floor and Ceiling GMs
PSC_Demo$floorGM <- if_else(PSC_Demo$floorGM > PSC_Demo$GM_Perc, PSC_Demo$floorGM, PSC_Demo$GM_Perc)
PSC_Demo$floorRev <- PSC_Demo$EXT_COST_REB / (1 - PSC_Demo$floorGM)

PSC_Demo$targetRev <- PSC_Demo$EXT_COST_REB / (1 - PSC_Demo$targetGM)
## Revenue Uplift range:
(sum(PSC_Demo$floorRev) - sum(PSC_Demo$EXT_SALES)) / sum(PSC_Demo$EXT_SALES)
(sum(PSC_Demo$targetRev) - sum(PSC_Demo$EXT_SALES)) / sum(PSC_Demo$EXT_SALES)

# Price Execution: translation into Price Matrix
GM_Matrix <- PSC_Demo %>% group_by(CUST_DECILE) %>% summarise(floorGM = min(floorGM, na.rm = T),
                                                              targetGM = mean(targetGM, na.rm = T)) 
## GM Translation to Prices
PSC_Demo$Unit_Cost <- PSC_Demo$EXT_COST_REB / PSC_Demo$QTY
summary(PSC_Demo$Unit_Cost)

# this could be an interesting analysis to see how much time the lower decile customers consume
PSC_Demo %>% group_by(CUST_DECILE) %>% summarise(n = n()) 

write.csv(PSC_Demo, "C:/Users/combsd/Desktop/Personal/Upwork/Plumbing Pricing/PSC_Demo.csv", row.names = F)



PSC_Demo$Price <- PSC_Demo$EXT_SALES / PSC_Demo$QTY

PSC_pricePlot <- ggplot(PSC_Demo, aes(x = DATE)) +
  geom_point(aes(y = Price, col = CUST_DECILE, size = QTY)) +
  theme_classic() +
  labs(title = "2018 Prices over Time",
       subtitle = "Product No. TOTC454CUFGW",
       x = "Date",
       y = "Unit Price ($)")

PSC_pricePlot + facet_wrap(~ CUST_DECILE)

PSC_Demo$floorPrice <- (PSC_Demo$EXT_COST_REB / (1 - PSC_Demo$floorGM)) / PSC_Demo$QTY
PSC_Demo$targetPrice <- (PSC_Demo$EXT_COST_REB / (1 - PSC_Demo$targetGM)) / PSC_Demo$QTY

targetPrices <- PSC_Demo %>% group_by(CUST_DECILE) %>% summarise(floorPrice = mean(floorPrice, na.rm = T),
                                                                 targetPrice = max(targetPrice, na.rm = T))

# Regression Comparison

# Group Data for Regression Analysis
PSC_Demo_Grouped <- PSC_Demo %>% 
  group_by(CUST_NO) %>% 
  summarise(Revenue = sum(EXT_SALES, na.rm = T),
            Costs = sum(EXT_COST_REB, na.rm = T),
            QTY = sum(QTY, na.rm = T),
            GM_Perc = (Revenue - Costs) / Revenue)

PSC_Demo_Grouped <- data.table(PSC_Demo_Grouped)
PSC_Demo_Grouped <- PSC_Demo_Grouped[ , ToKeep := abs(GM_Perc - mean(GM_Perc)) <= 3*sd(GM_Perc)][ToKeep == T]

PSC_regPlot <- ggplot(PSC_Demo_Grouped, aes(x = Revenue, y = GM_Perc)) +
  geom_point() +
  theme_classic() +
  stat_smooth(method = "lm") +
  labs(title = "GM % vs. Revenue (Sales)",
       subtitle = "Product No. TOTC454CUFGW",
       x = "Revenue ($)",
       y = "Gross Margin %")

## Regression at Product-Customer level
PSC_Demo_Reg <- PSC_Demo_Grouped %>% 
  do(regression = lm(GM_Perc ~ Revenue, data = .)) %>% 
  rowwise() %>% 
  tidy(regression)

## Spread modeled data to isolate two primary coeffiences (y-intercept and trend slope)
PSC_Demo_Reg <- PSC_Demo_Reg %>% select(term, estimate) %>% spread(term, estimate)

## Rename cols for clear interpretation
names(PSC_Demo_Reg)[1] <- "Intercept"
names(PSC_Demo_Reg)[2] <- "Slope"

## Calculate the Regression GM: $GM_Reg
PSC_Demo_Grouped$GM_Reg <- PSC_Demo_Reg[1,1] + PSC_Demo_Reg[1,2]*PSC_Demo_Grouped$Revenue

## Calculate Regression Revenue
PSC_Demo_Grouped$Rev_Reg <- PSC_Demo_Grouped$Costs / (1 - PSC_Demo_Grouped$GM_Reg)
PSC_Demo_Grouped$Rev_Reg <- if_else(PSC_Demo_Grouped$Rev_Reg > PSC_Demo_Grouped$Revenue, PSC_Demo_Grouped$Rev_Reg, PSC_Demo_Grouped$Revenue)
PSC_Demo_Grouped$GM_Reg_New <- (PSC_Demo_Grouped$Rev_Reg - PSC_Demo_Grouped$Costs) / PSC_Demo_Grouped$Rev_Reg

## Compare what-if analysis
(sum(PSC_Demo_Grouped$Rev_Reg) - sum(PSC_Demo_Grouped$Revenue)) / sum(PSC_Demo_Grouped$Revenue)

# Revenue contributions under $1,000 in 2018:
PSC_Sales %>% group_by(CUST_NO, CUST_RANK) %>% summarise(Revenue = sum(EXT_SALES, na.rm = T)) %>% filter(Revenue <= 1000) %>% arrange(desc(CUST_RANK))

# Next Year Regression View:
PSC_Demo_Grouped %>% filter(GM_Perc > 0.38) %>% 
  ggplot(aes(x = Rev_Reg, y = GM_Reg_New)) +
  geom_point() +
  theme_classic() +
  stat_smooth(method = "lm") +
  labs(title = "GM % vs. Revenue (Sales)",
       subtitle = "Product No. TOTC454CUFGW",
       x = "Revenue ($)",
       y = "Gross Margin %") +
  ylim(0, 0.6)


# Product_Categories <- as.character(unique(PSC_floor$CAT_NO))

# for(k in seq_along(Product_Categories)) {
# subdata <- subset(PSC_floor, CAT_NO == Product_Categories[k])
# deciles <- sort(unique(subdata$CUST_DECILE))

# for(k in 2:length(deciles)) {
#   if(subdata[subdata$CUST_DECILE == subdata$CUST_DECILE[k], "floorGM"] < subdata[subdata$CUST_DECILE == subdata$CUST_DECILE[k-1], "floorGM"]) {
#     subdata[subdata$CUST_DECILE == subdata$CUST_DECILE[k], "floorGM"] <- subdata[subdata$CUST_DECILE == subdata$CUST_DECILE[k-1], "floorGM"]
#   }
# }
# if (!exists("temp")) {
#   temp <- subdata
# } else {
#   temp <- rbind(temp, subdata) 
# }
# }