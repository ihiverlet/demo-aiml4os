# Load the parallel package for parallel processing
library(parallel)

# Define the number of samples
NUM_SAMPLES <- 10000000

# Define the function to check if a point is inside the unit circle
inside <- function(dummy) {
  x <- runif(1) # random number between 0 and 1
  y <- runif(1) # random number between 0 and 1
  return((x^2 + y^2) < 1)
}

# Use mclapply to perform parallel computation
# This function splits the work across available cores
count <- sum(unlist(mclapply(1:NUM_SAMPLES, inside, mc.cores = detectCores())))

# Calculate Pi approximation
pi_estimate <- 4 * count / NUM_SAMPLES
cat("Pi is roughly", pi_estimate, "\n")

