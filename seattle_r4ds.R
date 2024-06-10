#usethis::create_from_github(
#  "https://github.com/cargingarsan/seattle_dat.git",
#  destdir = ("E:/proyectos/rstudio_git/bigdata")
#)



dir.create("data", showWarnings = FALSE)

curl::multi_download(
  "https://r4ds.s3.us-west-2.amazonaws.com/seattle-library-checkouts.csv",
  "data/seattle-library-checkouts.csv",
  resume = TRUE
)


