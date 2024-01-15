#install.packages("aws.s3", repos = "https://cloud.R-project.org")

library("aws.s3")
library("arrow")
library("dplyr")

#bucketlist(region="")
aws.s3::get_bucket("inesh", region = "")


df <- 
  aws.s3::s3read_using(
    FUN = read_parquet,
    object = "/demo/fd-indcvi-2020.parquet",
    bucket = "inesh",
    opts = list("region" = "")
  )


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



