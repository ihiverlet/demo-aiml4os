#install.packages("aws.s3", repos = "https://cloud.R-project.org")

Sys.setenv("AWS_ACCESS_KEY_ID" = "M4RX9X34NB8J1YZQQ3FM",
           "AWS_SECRET_ACCESS_KEY" = "UjPNXzuO31jfPcUV0gVn+gQlIlvV3F0vvfJ+yTdZ",
           "AWS_DEFAULT_REGION" = "us-east-1",
           "AWS_SESSION_TOKEN" = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NLZXkiOiJNNFJYOVgzNE5COEoxWVpRUTNGTSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sImF1ZCI6WyJtaW5pby1kYXRhbm9kZSIsIm9ueXhpYSIsImdyYWZhbmEiLCJhY2NvdW50Il0sImF1dGhfdGltZSI6MTcwNDIyMDMwMCwiYXpwIjoib255eGlhIiwiZW1haWwiOiJpbmVzLmhpdmVybGV0QGluc2VlLmZyIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImV4cCI6MTcwNTM0OTUyNSwiZmFtaWx5X25hbWUiOiJIaXZlcmxldCIsImdpdmVuX25hbWUiOiJJbmVzIiwiZ3JvdXBzIjpbImNvZGlmaWNhdGlvbi1wY3MiLCJwYXJsaWFtZW50Iiwic3NwY2xvdWQtYWRtaW4iLCJzc3BsYWIiLCJ0ZXN0Il0sImlhdCI6MTcwNTI2MzEyNSwiaXNzIjoiaHR0cHM6Ly9hdXRoLmxhYi5zc3BjbG91ZC5mci9hdXRoL3JlYWxtcy9zc3BjbG91ZCIsImp0aSI6IjRmMGVhY2M5LTM0OWMtNDUyYy1hYjZkLWM2MmJlY2U1ZDczMCIsImxvY2FsZSI6ImVuIiwibmFtZSI6IkluZXMgSGl2ZXJsZXQiLCJwb2xpY3kiOiJzdHNvbmx5IiwicHJlZmVycmVkX3VzZXJuYW1lIjoiaW5lc2giLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiIsImFkbWluLWtleWNsb2FrIiwiZGVmYXVsdC1yb2xlcy1zc3BjbG91ZCJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfSwiZ3JhZmFuYSI6eyJyb2xlcyI6WyJBZG1pbiJdfX0sInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZ3JvdXBzIGVtYWlsIiwic2Vzc2lvbl9zdGF0ZSI6IjY1MTlmYTM1LTJhMjMtNGFiYi05OTRjLTYzNWMwNzFhZDFjYiIsInNpZCI6IjY1MTlmYTM1LTJhMjMtNGFiYi05OTRjLTYzNWMwNzFhZDFjYiIsInN1YiI6IjMwZmMzNzcxLWQ4ZGItNGIwYi1iOGNmLWRiMjI1ODgxMWNiYSIsInR5cCI6IkJlYXJlciJ9.nLPNK7Mh3M914tnOW5DqVlaHDpQ8usfgJzIf4cgsAGdZm4lgIMA-o4Jr0dn1117FQ9RyGrO0B3C7zXrtONCZtA",
           "AWS_S3_ENDPOINT"= "minio.lab.sspcloud.fr")

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


#df_occitanie <- df[df$DEPT == "31" ,]
# result <- df_occitanie %>% count(STOCD, sort = TRUE)


result <- df %>%  group_by(DEPT, STOCD) %>% 
  summarise(n = n()) 

result2 <- df %>% group_by(DEPT) %>% 
  summarise(mean_anai=2023-mean(ANAI),
            .groups = 'drop') 

## Write the result to S3

aws.s3::s3write_using(
  result,
  FUN = write_parquet,
  object = "/demo/output.parquet",
  bucket = "inesh",
  opts = list("region" = ""))



