#Import libraries
library(DBI)
library(dplyr)
library(RSQLite) 

#Create database
if (file.exists("airline3.db")) 
  file.remove("airline3.db")

conn <- dbConnect(RSQLite::SQLite(), "airline3.db")

#Add csv files data
airports <- read.csv("airports.csv", header = TRUE)

summary(airports)
length(unique(airports$airport) )
length(unique(airports$iata) )


carriers <- read.csv("carriers.csv", header = TRUE)
planes <- read.csv("plane-data.csv", header = TRUE)

dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)


n = 50000
for(i in c(2004,2006)) {
  ontime <- read.csv(paste0(i, ".csv"), header = TRUE)
  ontime = sample_n(ontime, n)
  
  if(i == 2004) {
    dbWriteTable(conn, "ontime", ontime)
  } else {
    dbWriteTable(conn, "ontime", ontime, append = TRUE)
  }
}


head(ontime)
head(airports)
colnames(airports)
unique(ontime$Cancelled) 
table(ontime$Cancelled)

#Queries (DBI)

#Q1, Which of the following airplanes has the lowest associated average departure delay
#(excluding cancelled and diverted flights)?

q1 <- dbGetQuery(conn, 
                 "SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
FROM planes JOIN ontime USING(tailnum)
WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
GROUP BY model
ORDER BY avg_delay")

print(q1)

print(paste(q1[1, "model"], "has the lowest associated average departure delay."))

#Q2, Which of the following cities has the highest number of inbound flights 
#(excluding cancelled flights)?

q2 <- dbGetQuery(conn,
                 "SELECT city AS city, COUNT(*) AS count_flights
                      FROM ontime JOIN airports ON ontime.dest = airports.iata
                      WHERE ontime.cancelled = 0 AND city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
                      GROUP BY city
                      ORDER BY count_flights DESC")


print(paste(q2[1, "city"], "has the highest number of inbound flights (excluding cancelled flights)"))

#Q3, Which of the following companies has the highest number of cancelled flights?

q3 <- dbGetQuery(conn,
                 "SELECT carriers.Description AS carrier, sum(cancelled) as number_cancelled_flights
                      FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                      WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                      GROUP BY carriers.Description
                      ORDER BY number_cancelled_flights DESC")

print(paste(q3[1, "carrier"], "has the highest number of cancelled flights"))

#Q4, Which of the following companies has the highest number of cancelled
#flights, relative to their number of total flights?

q4 <- dbGetQuery(conn, 
                 "SELECT 
q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
FROM
(
  SELECT carriers.Description AS carrier, COUNT(*) AS numerator
  FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
  WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
  GROUP BY carriers.Description
) AS q1 JOIN 
(
  SELECT carriers.Description AS carrier, COUNT(*) AS denominator
  FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
  WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
  GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC")
  
  print(paste(q4[1, "carrier"], "highest number of cancelled flights, relative to their number of total flights"))
  
  
  
  
  #Queries (dplyr)
  
  
  q1 <- ontime %>% 
    inner_join(planes, by = "tailnum", suffix = c(".ontime", ".planes")) %>%
    filter(Cancelled == 0 & Diverted == 0 & DepDelay > 0) %>%
    group_by(model) %>%
    summarize(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
    arrange(avg_delay) 
  
  print(head(q1, 1))
  
  q2 <- ontime %>% 
    inner_join(airports, by = c("Dest" = "iata")) %>%
    filter(Cancelled == 0) %>%
    group_by(city) %>%
    summarize(total = n()) %>%
    arrange(desc(total)) 
  
  print(head(q2, 1))
  
  q3 <- ontime %>% 
    inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
    filter(Cancelled == 1 & Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
    group_by(Description) %>%
    summarize(total = n()) %>%
    arrange(desc(total))
  
  print(head(q3, 1))
  
  q4a <- inner_join(ontime, carriers, by = c("UniqueCarrier" = "Code")) %>%
    filter(Cancelled == 1 & Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
    group_by(Description) %>%
    summarize(numerator = n()) %>%
    rename(carrier = Description)
  
  q4b <- inner_join(ontime, carriers, by = c("UniqueCarrier" = "Code")) %>%
    filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
    group_by(Description) %>%
    summarize(denominator = n()) %>%
    rename(carrier = Description)
  
  q4 <- inner_join(q4a, q4b, by = "carrier") %>%
    mutate_if(is.integer, as.double) %>%
    mutate(ratio = numerator/denominator) %>%
    select(carrier, ratio) %>%
    arrange(desc(ratio)) 
  
  print(head(q4, 1))
  
  
  