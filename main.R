library(redcapAPI)
library(dplyr)     # group_by and filter
library(lubridate) # now
source("tokens.R")

# PART1: Define data access parameters
# Both kRedcapAPIURL and kRedcapTokens are stored in the tokens.R file

# PART2: Export and bind data from all health facilities
my.fields <- c(
  'record_id',
  'study_number',
  'int_date',     # Date of EPI visits
  'comp_date',    # Date of the non-compliant contacts
  'wdrawal_date', # Withdrawal date
  'death_date'    # Death date
)

my.epi.events <- c(
  'epipenta1_v0_recru_arm_1', # Recruitment - AZi 1st dose
  'epipenta2_v1_iptis_arm_1', # Penta2
  'epipenta3_v2_iptis_arm_1', # Penta3
  'epivita_v3_iptisp3_arm_1', # VitA1
  'epimvr1_v4_iptisp4_arm_1', # AZi 2nd dose
  'epivita_v5_iptisp5_arm_1', # VitA2
  'epimvr2_v6_iptisp6_arm_1' # AZi 3rd dose
)

my.other.events <- c(
  'out_of_schedule_arm_1',    # Non-compliant
  'end_of_fu_arm_1'           # End of F/U: Withdrawals & Deaths
)

data <- data.frame()
for (hf in names(kRedcapTokens)) {
  print(paste("Extracting data from", hf))
  
  rcon <- redcapConnection(kRedcapAPIURL, kRedcapTokens[[hf]])
  hf.data <- exportRecords(
    rcon,
    factors            = F,
    labels             = F,
    fields             = my.fields,
    events             = c(my.epi.events, my.other.events)
  )
  # cbind is attaching by columns so we use it to attach a new column with the 
  # HF code
  hf.data <- cbind(hf = hf, hf.data)
  
  # rbind is attaching by rows so we use it to attach the individual HF data 
  # frames together
  data <- rbind(data, hf.data)
}

# PART3: Create PK column
data$pk <- paste(data$hf, data$record_id, sep = "_")

# PART4: Remove withdrawals & deaths
filter <- !is.na(data$wdrawal_date)
withdrawals <- data[filter, 'pk']

filter <- !(data$pk %in% withdrawals)
data <- data[filter, ]

filter <- !is.na(data$death_date)
deaths <- data[filter, 'pk']

filter <- !(data$pk %in% deaths)
data <- data[filter, ]

# PART5: Filter participants contacted because they were non-compliant
filter <- data$redcap_repeat_instrument == 'noncompliant'
columns <- c('hf', 'record_id', 'redcap_repeat_instance', 'comp_date', 'pk')
non.compliants <- data[which(filter), columns]

# PART6: A participant may have more than one non-compliant form. Keep only last
#        non-compliant contact
non.compliants <- group_by(non.compliants, pk)
# In some cases, we can find an empty non-compliant form. This is a 
# comp_date == NA. If we don't remove NAs in the max function those 
# participants won't be considered and will be removed
non.compliants <- filter(non.compliants, comp_date == max(comp_date, na.rm = T))

# PART7: Attach the last EPI visit to the non-compliant participants
filter <- data$redcap_event_name %in% my.epi.events
columns <- c('pk', 'redcap_event_name', 'int_date')
epi.visits <- data[filter, columns]

# Last EPI visit by participant
epi.visits <- group_by(epi.visits, pk)
last.epi.visits <- filter(epi.visits, int_date == max(int_date, na.rm = T))

# Merge last EPI visit to the non-compliant participants
non.compliants <- merge(non.compliants, last.epi.visits, sort = F)

# PART8: Filter non-compliant participants that never came to the HF after the
#        non-compliant contact
filter <- non.compliants$comp_date > non.compliants$int_date
non.returning.non.compliants <- non.compliants[filter, ]

non.returning.non.compliants$since <- 
  difftime(now(), non.returning.non.compliants$comp_date, units = "weeks")

# PART9: Attach study numbers
filter <- !is.na(data$study_number)
columns <- c('pk', 'study_number')
study.numbers <- data[filter, columns]

non.returning.non.compliants <- merge(non.returning.non.compliants, 
                                      study.numbers, sort = F)

# PART10: Arrange data frame and save it in a CSV file
colum.names <- c('pk', 'hf', 'record_id', 'instance', 'last_non_comp_date', 
                 'last_epi_visit', 'last_epi_visit_date', 
                 'weeks_since_last_contact', 'study_number')
colnames(non.returning.non.compliants) <- colum.names

report.columns <- c('hf', 'record_id', 'study_number', 'last_epi_visit', 
                    'last_epi_visit_date', 'last_non_comp_date', 
                    'weeks_since_last_contact')
report <- non.returning.non.compliants[, report.columns]