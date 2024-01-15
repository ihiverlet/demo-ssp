library("aws.s3")
library("arrow")
library("dplyr")

# Read input file from s3
df <- 
  aws.s3::s3read_using(
    FUN = read_parquet,
    object = "/demo/fd-indcvi-2020.parquet",
    bucket = "inesh",
    opts = list("region" = "")
  )

# 
result <- df %>% group_by(DEPT) %>% 
  summarise(mean_anai=2023-mean(ANAI),
            .groups = 'drop') 

## Write the result to S3

aws.s3::s3write_using(
  result,
  FUN = write_parquet,
  object = "/demo/output.parquet",
  bucket = "inesh",
  opts = list("region" = ""))

